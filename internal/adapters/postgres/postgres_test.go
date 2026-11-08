package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v4"

	"gophprofile/internal/domain"
	"gophprofile/migrations"
)

const testAvatarID = "cfa7d924-00c1-4d09-bde9-9c625b31f1cd"
const testMessageID = "fc554472-1377-4b21-81c3-a94cd7821051"

func repositoryMock(t *testing.T) (*Repository, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
		mock.Close()
	})
	return &Repository{pool: mock}, mock
}

func sampleAvatar() domain.Avatar {
	now := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	return domain.Avatar{
		ID: testAvatarID, UserID: "andrew", FileName: "photo.png", MIMEType: "image/png",
		Size: 512, S3Key: "original/" + testAvatarID, Status: domain.StatusProcessing,
		Dimensions: domain.Dimensions{Width: 640, Height: 480}, CreatedAt: now, UpdatedAt: now,
		Thumbnails: []domain.Thumbnail{{Size: "100x100", URL: "/small", S3Key: "small/key"}},
	}
}

func sampleEvent() domain.Event {
	return domain.Event{ID: testMessageID, Type: domain.EventUploaded, AvatarID: testAvatarID,
		UserID: "andrew", S3Key: "original/" + testAvatarID, CreatedAt: sampleAvatar().CreatedAt}
}

func avatarRows(avatar domain.Avatar) *pgxmock.Rows {
	raw, _ := marshalThumbnails(avatar.Thumbnails)
	var deleted any
	if avatar.DeletedAt != nil {
		deleted = avatar.DeletedAt
	}
	return pgxmock.NewRows(strings.Split("id,user_id,file_name,mime_type,size_bytes,s3_key,width,height,thumbnails,processing_status,created_at,updated_at,deleted_at", ",")).
		AddRow(avatar.ID, avatar.UserID, avatar.FileName, avatar.MIMEType, avatar.Size, avatar.S3Key,
			avatar.Dimensions.Width, avatar.Dimensions.Height, raw, avatar.Status,
			avatar.CreatedAt, avatar.UpdatedAt, deleted)
}

type matchArgument func(any) bool

func (match matchArgument) Match(value any) bool { return match(value) }

func anyArgs(count int) []any {
	args := make([]any, count)
	for index := range args {
		args[index] = pgxmock.AnyArg()
	}
	return args
}

func TestCreateCommitsAvatarAndOutbox(t *testing.T) {
	repo, mock := repositoryMock(t)
	avatar, event := sampleAvatar(), sampleEvent()
	thumbnails, _ := marshalThumbnails(avatar.Thumbnails)
	payload, _ := json.Marshal(event)
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO avatars").WithArgs(avatar.ID, avatar.UserID, avatar.FileName,
		avatar.MIMEType, avatar.Size, avatar.S3Key, 640, 480, thumbnails, avatar.Status,
		avatar.CreatedAt, avatar.UpdatedAt).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("INSERT INTO outbox").WithArgs(event.ID, payload, event.CreatedAt).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	if err := repo.Create(context.Background(), avatar, event); err != nil {
		t.Fatal(err)
	}
}

func TestCreateOutboxFailureRollsBackMetadata(t *testing.T) {
	repo, mock := repositoryMock(t)
	failure := errors.New("outbox unavailable")
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO avatars").WithArgs(anyArgs(12)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("INSERT INTO outbox").WithArgs(anyArgs(3)...).WillReturnError(failure)
	mock.ExpectRollback()
	if err := repo.Create(context.Background(), sampleAvatar(), sampleEvent()); !errors.Is(err, failure) {
		t.Fatalf("got %v, expected wrapped failure", err)
	}
}

func TestGetPreservesPrivateStorageKeys(t *testing.T) {
	repo, mock := repositoryMock(t)
	want := sampleAvatar()
	mock.ExpectQuery("FROM avatars WHERE id = \\$1 AND deleted_at IS NULL").WithArgs(want.ID).
		WillReturnRows(avatarRows(want))
	got, err := repo.Get(context.Background(), want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.URL != "/api/v1/avatars/"+want.ID || !reflect.DeepEqual(got.Thumbnails, want.Thumbnails) || got.S3Key != want.S3Key {
		t.Fatalf("storage metadata was not restored: %+v", got)
	}
	public, _ := json.Marshal(got)
	if strings.Contains(string(public), "small/key") || strings.Contains(string(public), want.S3Key) {
		t.Fatalf("public JSON exposed storage keys: %s", public)
	}
}

func TestGetMissingReturnsDomainError(t *testing.T) {
	repo, mock := repositoryMock(t)
	mock.ExpectQuery("FROM avatars WHERE id").WithArgs(testAvatarID).WillReturnError(pgx.ErrNoRows)
	if _, err := repo.Get(context.Background(), testAvatarID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCurrentUsesStableNewestOrdering(t *testing.T) {
	repo, mock := repositoryMock(t)
	mock.ExpectQuery("ORDER BY created_at DESC, id DESC LIMIT 1").WithArgs("andrew").
		WillReturnRows(avatarRows(sampleAvatar()))
	if _, err := repo.Current(context.Background(), "andrew"); err != nil {
		t.Fatal(err)
	}
}

func TestListOmitsDeletedAndReturnsEmptyArray(t *testing.T) {
	repo, mock := repositoryMock(t)
	mock.ExpectQuery("WHERE user_id = \\$1 AND deleted_at IS NULL ORDER BY created_at DESC, id DESC").
		WithArgs("andrew").WillReturnRows(avatarRows(sampleAvatar())).RowsWillBeClosed()
	avatars, err := repo.List(context.Background(), "andrew")
	if err != nil || len(avatars) != 1 {
		t.Fatalf("avatars=%+v err=%v", avatars, err)
	}
	mock.ExpectQuery("FROM avatars").WithArgs("nobody").WillReturnRows(pgxmock.NewRows([]string{"id"})).RowsWillBeClosed()
	avatars, err = repo.List(context.Background(), "nobody")
	if err != nil || avatars == nil || len(avatars) != 0 {
		t.Fatalf("empty collection must encode as []: avatars=%v err=%v", avatars, err)
	}
}

func TestSoftDeleteLocksAndCapturesAllStorageKeys(t *testing.T) {
	repo, mock := repositoryMock(t)
	avatar := sampleAvatar()
	mock.ExpectBegin()
	mock.ExpectQuery("FROM avatars WHERE id = \\$1 FOR UPDATE").WithArgs(avatar.ID).
		WillReturnRows(avatarRows(avatar))
	mock.ExpectExec("UPDATE avatars SET deleted_at = now").WithArgs(avatar.ID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectExec("INSERT INTO outbox").WithArgs(testMessageID, matchArgument(func(value any) bool {
		raw, ok := value.([]byte)
		if !ok {
			return false
		}
		var event domain.Event
		return json.Unmarshal(raw, &event) == nil && event.Type == domain.EventDeleted &&
			event.AvatarID == avatar.ID && event.UserID == avatar.UserID &&
			reflect.DeepEqual(event.S3Keys, []string{avatar.S3Key, "small/key"})
	}), avatar.CreatedAt).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	if err := repo.SoftDelete(context.Background(), avatar.ID, avatar.UserID, sampleEvent()); err != nil {
		t.Fatal(err)
	}
}

func TestSoftDeleteRejectsDifferentOwner(t *testing.T) {
	repo, mock := repositoryMock(t)
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE").WithArgs(testAvatarID).WillReturnRows(avatarRows(sampleAvatar()))
	mock.ExpectRollback()
	if err := repo.SoftDelete(context.Background(), testAvatarID, "another", sampleEvent()); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("expected forbidden, got %v", err)
	}
}

func TestSoftDeleteIsIdempotent(t *testing.T) {
	repo, mock := repositoryMock(t)
	avatar := sampleAvatar()
	avatar.DeletedAt = &avatar.UpdatedAt
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE").WithArgs(avatar.ID).WillReturnRows(avatarRows(avatar))
	mock.ExpectCommit()
	if err := repo.SoftDelete(context.Background(), avatar.ID, avatar.UserID, sampleEvent()); err != nil {
		t.Fatal(err)
	}
}

func expectProcessingStart(mock pgxmock.PgxPoolIface, avatar domain.Avatar, duplicate bool) {
	mock.ExpectBegin()
	mock.ExpectQuery("FROM avatars WHERE id = \\$1 FOR UPDATE").WithArgs(avatar.ID).
		WillReturnRows(avatarRows(avatar))
	query := mock.ExpectQuery("INSERT INTO processed_messages").WithArgs(testMessageID)
	if duplicate {
		query.WillReturnError(pgx.ErrNoRows)
	} else {
		query.WillReturnRows(pgxmock.NewRows([]string{"message_id"}).AddRow(testMessageID))
	}
}

func TestProcessEventCommitsResultWithInbox(t *testing.T) {
	repo, mock := repositoryMock(t)
	expectProcessingStart(mock, sampleAvatar(), false)
	thumbnails := []domain.Thumbnail{{Size: "300x300", S3Key: "new/thumbnail", URL: "/new"}}
	stored, _ := marshalThumbnails(thumbnails)
	mock.ExpectExec("UPDATE avatars SET thumbnails").WithArgs(testAvatarID, stored, domain.StatusCompleted).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()
	called := false
	err := repo.ProcessEvent(context.Background(), sampleEvent(), func(_ context.Context, avatar domain.Avatar) (domain.JobResult, error) {
		called = true
		if avatar.ID != testAvatarID {
			t.Fatalf("callback got wrong avatar %s", avatar.ID)
		}
		return domain.JobResult{Thumbnails: thumbnails, Status: domain.StatusCompleted}, nil
	})
	if err != nil || !called {
		t.Fatalf("called=%v err=%v", called, err)
	}
}

func TestProcessEventErrorDoesNotCommitInbox(t *testing.T) {
	repo, mock := repositoryMock(t)
	expectProcessingStart(mock, sampleAvatar(), false)
	mock.ExpectRollback()
	failure := errors.New("S3 temporary failure")
	err := repo.ProcessEvent(context.Background(), sampleEvent(), func(context.Context, domain.Avatar) (domain.JobResult, error) {
		return domain.JobResult{}, failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("callback failure must remain retryable: %v", err)
	}
}

func TestProcessEventDatabaseFailureRollsBackInbox(t *testing.T) {
	repo, mock := repositoryMock(t)
	expectProcessingStart(mock, sampleAvatar(), false)
	failure := errors.New("metadata write failed")
	mock.ExpectExec("UPDATE avatars SET thumbnails").WithArgs(anyArgs(3)...).WillReturnError(failure)
	mock.ExpectRollback()
	err := repo.ProcessEvent(context.Background(), sampleEvent(), func(context.Context, domain.Avatar) (domain.JobResult, error) {
		return domain.JobResult{Status: domain.StatusCompleted}, nil
	})
	if !errors.Is(err, failure) {
		t.Fatalf("failed DB update must remain retryable: %v", err)
	}
}

func TestProcessEventRejectsMismatchedOwnerAndActiveDeletion(t *testing.T) {
	for _, scenario := range []string{"wrong-owner", "active-deletion"} {
		t.Run(scenario, func(t *testing.T) {
			repo, mock := repositoryMock(t)
			event := sampleEvent()
			if scenario == "wrong-owner" {
				event.UserID = "someone-else"
			} else {
				event.Type = domain.EventDeleted
			}
			mock.ExpectBegin()
			mock.ExpectQuery("FOR UPDATE").WithArgs(testAvatarID).WillReturnRows(avatarRows(sampleAvatar()))
			mock.ExpectRollback()
			err := repo.ProcessEvent(context.Background(), event, func(context.Context, domain.Avatar) (domain.JobResult, error) {
				t.Fatal("invalid event must never execute storage operations")
				return domain.JobResult{}, nil
			})
			if !errors.Is(err, domain.ErrInvalid) {
				t.Fatalf("expected invalid event, got %v", err)
			}
		})
	}
}

func TestStorageKeysDeduplicatesAndOmitsEmpty(t *testing.T) {
	avatar := sampleAvatar()
	avatar.Thumbnails = append(avatar.Thumbnails,
		domain.Thumbnail{S3Key: avatar.S3Key}, domain.Thumbnail{S3Key: ""},
		domain.Thumbnail{S3Key: "small/key"})
	if got := storageKeys(avatar); !reflect.DeepEqual(got, []string{avatar.S3Key, "small/key"}) {
		t.Fatalf("unexpected deletion keys: %v", got)
	}
}

func TestProcessEventSkipsDuplicateOrObsoleteUpload(t *testing.T) {
	for _, scenario := range []string{"duplicate", "deleted", "completed"} {
		t.Run(scenario, func(t *testing.T) {
			repo, mock := repositoryMock(t)
			avatar := sampleAvatar()
			if scenario == "deleted" {
				avatar.DeletedAt = &avatar.UpdatedAt
			}
			if scenario == "completed" {
				avatar.Status = domain.StatusCompleted
			}
			expectProcessingStart(mock, avatar, scenario == "duplicate")
			mock.ExpectCommit()
			if err := repo.ProcessEvent(context.Background(), sampleEvent(), func(context.Context, domain.Avatar) (domain.JobResult, error) {
				t.Fatal("obsolete message must not write to S3")
				return domain.JobResult{}, nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestProcessDeletionRunsForSoftDeletedAvatar(t *testing.T) {
	repo, mock := repositoryMock(t)
	avatar, event := sampleAvatar(), sampleEvent()
	avatar.DeletedAt = &avatar.UpdatedAt
	event.Type = domain.EventDeleted
	expectProcessingStart(mock, avatar, false)
	stored, _ := marshalThumbnails(avatar.Thumbnails)
	mock.ExpectExec("UPDATE avatars SET thumbnails").WithArgs(avatar.ID, stored, avatar.Status).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()
	called := false
	if err := repo.ProcessEvent(context.Background(), event, func(context.Context, domain.Avatar) (domain.JobResult, error) {
		called = true
		return domain.JobResult{}, nil
	}); err != nil || !called {
		t.Fatalf("delete handler called=%v err=%v", called, err)
	}
}

func TestDispatchPendingPublishesBeforeMarkingSent(t *testing.T) {
	repo, mock := repositoryMock(t)
	event := sampleEvent()
	payload, _ := json.Marshal(event)
	mock.ExpectBegin()
	mock.ExpectQuery("LIMIT 1 FOR UPDATE SKIP LOCKED").WillReturnRows(
		pgxmock.NewRows([]string{"id", "payload"}).AddRow(event.ID, payload))
	mock.ExpectExec("UPDATE outbox SET sent_at = now").WithArgs(event.ID).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()
	called := false
	dispatched, err := repo.DispatchPending(context.Background(), func(_ context.Context, got domain.Event) error {
		called = true
		if !reflect.DeepEqual(event, got) {
			t.Fatalf("published event changed: %+v", got)
		}
		return nil
	})
	if err != nil || !dispatched || !called {
		t.Fatalf("dispatched=%v called=%v err=%v", dispatched, called, err)
	}
}

func TestDispatchFailureLeavesEventPending(t *testing.T) {
	repo, mock := repositoryMock(t)
	payload, _ := json.Marshal(sampleEvent())
	failure := errors.New("broker unavailable")
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE SKIP LOCKED").WillReturnRows(
		pgxmock.NewRows([]string{"id", "payload"}).AddRow(testMessageID, payload))
	mock.ExpectRollback()
	dispatched, err := repo.DispatchPending(context.Background(), func(context.Context, domain.Event) error { return failure })
	if dispatched || !errors.Is(err, failure) {
		t.Fatalf("failed event must remain pending: dispatched=%v err=%v", dispatched, err)
	}
}

func TestDispatchEmptyOutboxDoesNotPublish(t *testing.T) {
	repo, mock := repositoryMock(t)
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE SKIP LOCKED").WillReturnError(pgx.ErrNoRows)
	mock.ExpectRollback()
	dispatched, err := repo.DispatchPending(context.Background(), func(context.Context, domain.Event) error {
		t.Fatal("empty outbox must not publish")
		return nil
	})
	if dispatched || err != nil {
		t.Fatalf("dispatched=%v err=%v", dispatched, err)
	}
}

func TestMigrateLocksSchemaAndAppliesOnlyOnce(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(map[bool]string{false: "first run", true: "already applied"}[applied], func(t *testing.T) {
			repo, mock := repositoryMock(t)
			mock.ExpectBegin()
			mock.ExpectExec("SELECT pg_advisory_xact_lock").WithArgs(int64(4815162342)).
				WillReturnResult(pgxmock.NewResult("SELECT", 1))
			mock.ExpectExec("CREATE TABLE IF NOT EXISTS schema_migrations").
				WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))
			mock.ExpectQuery("SELECT EXISTS").WithArgs("001_initial.sql").WillReturnRows(
				pgxmock.NewRows([]string{"exists"}).AddRow(applied))
			if !applied {
				source, _ := migrations.Files.ReadFile("001_initial.sql")
				mock.ExpectExec("CREATE TABLE avatars").WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))
				if !strings.Contains(string(source), "processed_messages") {
					t.Fatal("migration must include durable inbox")
				}
				mock.ExpectExec("INSERT INTO schema_migrations").WithArgs("001_initial.sql").
					WillReturnResult(pgxmock.NewResult("INSERT", 1))
			}
			mock.ExpectCommit()
			if err := repo.Migrate(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPingAndClose(t *testing.T) {
	repo, mock := repositoryMock(t)
	mock.ExpectPing()
	if err := repo.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	mock.ExpectClose()
	repo.Close()
}
