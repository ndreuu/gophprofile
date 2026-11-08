//go:build integration

package postgres

import (
	"context"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"gophprofile/internal/domain"
)

func TestRealPostgresTransactionsInboxOutboxAndOwnership(t *testing.T) {
	dsn := os.Getenv("POSTGRES_INTEGRATION_URL")
	if dsn == "" {
		t.Skip("POSTGRES_INTEGRATION_URL is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "gophprofile_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		_ = admin.Close(ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		if err != nil {
			t.Error(err)
		}
		_ = admin.Close(cleanup)
	})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	repo, err := New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(repo.Close)
	if err = repo.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err = repo.Migrate(ctx); err != nil {
		t.Fatal("repeated migration", err)
	}
	id := uuid.NewString()
	now := time.Now().UTC()
	avatar := domain.Avatar{ID: id, UserID: "owner", FileName: "test.png", MIMEType: "image/png", Size: 32, S3Key: "originals/" + id, Dimensions: domain.Dimensions{Width: 10, Height: 10}, Status: domain.StatusProcessing, CreatedAt: now, UpdatedAt: now}
	event := domain.Event{ID: uuid.NewString(), Type: domain.EventUploaded, AvatarID: id, UserID: "owner", S3Key: avatar.S3Key, CreatedAt: now}
	if err = repo.Create(ctx, avatar, event); err != nil {
		t.Fatal(err)
	}
	if _, err = repo.Get(ctx, id); err != nil {
		t.Fatal(err)
	}
	sent, err := repo.DispatchPending(ctx, func(context.Context, domain.Event) error { return errors.New("broker temporarily unavailable") })
	if sent || err == nil {
		t.Fatal("failed event was acknowledged")
	}
	sent, err = repo.DispatchPending(ctx, func(_ context.Context, received domain.Event) error {
		if received.ID != event.ID {
			t.Error("wrong outbox payload")
		}
		return nil
	})
	if !sent || err != nil {
		t.Fatal("outbox retry", sent, err)
	}
	if err = repo.ProcessEvent(ctx, event, func(context.Context, domain.Avatar) (domain.JobResult, error) {
		return domain.JobResult{}, errors.New("S3 temporarily unavailable")
	}); err == nil {
		t.Fatal("failed job was accepted")
	}
	var calls atomic.Int32
	callback := func(context.Context, domain.Avatar) (domain.JobResult, error) {
		calls.Add(1)
		return domain.JobResult{Status: domain.StatusCompleted, Thumbnails: []domain.Thumbnail{{Size: "100x100", S3Key: "thumbnails/" + id + "/100x100.jpg", URL: "/thumbnail"}}}, nil
	}
	if err = repo.ProcessEvent(ctx, event, callback); err != nil {
		t.Fatal(err)
	}
	if err = repo.ProcessEvent(ctx, event, callback); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("duplicate work executed")
	}
	stored, err := repo.Get(ctx, id)
	if err != nil || stored.Status != domain.StatusCompleted || stored.Thumbnails[0].S3Key == "" {
		t.Fatal("lost metadata", stored, err)
	}
	deleted := domain.Event{ID: uuid.NewString(), Type: domain.EventDeleted, AvatarID: id, UserID: "owner", CreatedAt: time.Now().UTC()}
	if err = repo.SoftDelete(ctx, id, "other", deleted); !errors.Is(err, domain.ErrForbidden) {
		t.Fatal("ownership", err)
	}
	if err = repo.SoftDelete(ctx, id, "owner", deleted); err != nil {
		t.Fatal(err)
	}
	if err = repo.SoftDelete(ctx, id, "owner", deleted); err != nil {
		t.Fatal("repeated delete", err)
	}
	if _, err = repo.Get(ctx, id); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("deleted avatar visible", err)
	}
	event.ID = uuid.NewString()
	if err = repo.ProcessEvent(ctx, event, callback); err != nil {
		t.Fatal("late upload", err)
	}
	if calls.Load() != 1 {
		t.Fatal("deleted avatar processed again")
	}
	sent, err = repo.DispatchPending(ctx, func(_ context.Context, event domain.Event) error {
		if event.Type != domain.EventDeleted || len(event.S3Keys) != 2 {
			t.Error("missing deletion keys")
		}
		return nil
	})
	if !sent || err != nil {
		t.Fatal("delete outbox", sent, err)
	}
}
