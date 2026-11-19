//go:build integration

package postgres

import (
	"context"
	"testing"

	"gophprofile/internal/domain"
)

func TestPersistentObservabilityStatsIncludeThumbnailsAndPendingOutbox(t *testing.T) {
	fixture := newMigrationFixture(t)
	if err := fixture.repo.Migrate(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	avatar, event := sampleAvatar(), sampleEvent()
	avatar.Size = 100
	avatar.Thumbnails = []domain.Thumbnail{
		{Size: "100x100", S3Key: "small/key", SizeBytes: 7},
		{Size: "300x300", S3Key: "large/key", SizeBytes: 13},
	}
	if err := fixture.repo.Create(fixture.ctx, avatar, event); err != nil {
		t.Fatal(err)
	}
	fixture.repo.Close()
	restarted, err := New(fixture.ctx, fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	bytes, depth, acquired, idle, total, err := restarted.ObservabilityStats(fixture.ctx)
	if err != nil || bytes != 120 || depth != 1 || total < 1 || acquired+idle > total {
		t.Fatalf("bytes=%d depth=%d pool=%d/%d/%d err=%v", bytes, depth, acquired, idle, total, err)
	}
	if sent, err := restarted.DispatchPending(fixture.ctx, func(context.Context, domain.Event) error { return nil }); !sent || err != nil {
		t.Fatalf("sent=%v err=%v", sent, err)
	}
	if _, depth, _, _, _, err := restarted.ObservabilityStats(fixture.ctx); err != nil || depth != 0 {
		t.Fatalf("sent event remained pending: depth=%d err=%v", depth, err)
	}
	event.ID = "ef98772d-d8ce-4982-bf10-56c59c091c81"
	if err := restarted.SoftDelete(fixture.ctx, avatar.ID, avatar.UserID, event); err != nil {
		t.Fatal(err)
	}
	if bytes, depth, _, _, _, err := restarted.ObservabilityStats(fixture.ctx); err != nil || bytes != 0 || depth != 1 {
		t.Fatalf("deleted metadata remained active: bytes=%d depth=%d err=%v", bytes, depth, err)
	}
}
