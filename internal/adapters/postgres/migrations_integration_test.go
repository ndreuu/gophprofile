//go:build integration

package postgres

import (
	"context"
	"errors"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"gophprofile/internal/domain"
	"gophprofile/migrations"
)

type migrationFixture struct {
	ctx  context.Context
	repo *Repository
	pool *pgxpool.Pool
	dsn  string
}

func newMigrationFixture(t *testing.T) migrationFixture {
	t.Helper()
	dsn := os.Getenv("POSTGRES_INTEGRATION_URL")
	if dsn == "" {
		t.Skip("POSTGRES_INTEGRATION_URL is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	var admin *pgx.Conn
	var repo *Repository
	var created bool
	schema := "gophprofile_migrations_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Cleanup(func() {
		cancel()
		if repo != nil {
			repo.Close()
		}
		if admin != nil {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if created {
				if _, err := admin.Exec(cleanup, "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
					t.Error(err)
				}
			}
			_ = admin.Close(cleanup)
		}
	})
	var err error
	admin, err = pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	created = true
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	repo, err = New(ctx, parsed.String())
	if err != nil {
		t.Fatal(err)
	}
	return migrationFixture{ctx: ctx, repo: repo, pool: repo.pool.(*pgxpool.Pool), dsn: parsed.String()}
}

func installLegacySchema(t *testing.T, fixture migrationFixture) {
	t.Helper()
	source, err := os.ReadFile("testdata/legacy_001_initial.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, string(source)); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `CREATE TABLE schema_migrations (
        version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    ); INSERT INTO schema_migrations (version) VALUES ('001_initial.sql')`); err != nil {
		t.Fatal(err)
	}
}

func migrationProvider(t *testing.T, fixture migrationFixture) *goose.Provider {
	t.Helper()
	db := stdlib.OpenDBFromPool(fixture.pool)
	t.Cleanup(func() { _ = db.Close() })
	provider, err := migrations.NewProvider(db)
	if err != nil {
		t.Fatal(err)
	}
	return provider
}

func requireSchemaVersion(t *testing.T, fixture migrationFixture, version int64) {
	t.Helper()
	got, err := migrationProvider(t, fixture).GetDBVersion(fixture.ctx)
	if err != nil || got != version {
		t.Fatalf("migration version=%d want=%d err=%v", got, version, err)
	}
}

func requireConstrainedSchema(t *testing.T, fixture migrationFixture) {
	t.Helper()
	for column, limit := range map[string]int{"user_id": 255, "file_name": 255, "mime_type": 100, "s3_key": 500} {
		var dataType string
		var maxLength int
		err := fixture.pool.QueryRow(fixture.ctx, `SELECT data_type, character_maximum_length
            FROM information_schema.columns WHERE table_schema = current_schema()
            AND table_name = 'avatars' AND column_name = $1`, column).Scan(&dataType, &maxLength)
		if err != nil || dataType != "character varying" || maxLength != limit {
			t.Fatalf("column=%s type=%s length=%d err=%v", column, dataType, maxLength, err)
		}
	}
	var dataType, typeName string
	err := fixture.pool.QueryRow(fixture.ctx, `SELECT data_type, udt_name FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'avatars' AND column_name = 'processing_status'`).Scan(&dataType, &typeName)
	if err != nil || dataType != "USER-DEFINED" || typeName != "avatar_processing_status" {
		t.Fatalf("status type=%s udt=%s err=%v", dataType, typeName, err)
	}
	var labels []string
	err = fixture.pool.QueryRow(fixture.ctx, `SELECT array_agg(e.enumlabel ORDER BY e.enumsortorder)::text[]
        FROM pg_enum e JOIN pg_type t ON t.oid = e.enumtypid JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = current_schema() AND t.typname = 'avatar_processing_status'`).Scan(&labels)
	if err != nil || !reflect.DeepEqual(labels, []string{"processing", "completed", "failed"}) {
		t.Fatalf("enum labels=%v err=%v", labels, err)
	}
}

func TestPostgresMigrationsFreshSchemaRejectsInvalidMetadata(t *testing.T) {
	fixture := newMigrationFixture(t)
	if err := fixture.repo.Migrate(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.Migrate(fixture.ctx); err != nil {
		t.Fatal("repeated migration", err)
	}
	requireConstrainedSchema(t, fixture)
	requireSchemaVersion(t, fixture, 2)
	var legacyMissing bool
	if err := fixture.pool.QueryRow(fixture.ctx, "SELECT to_regclass('schema_migrations') IS NULL").Scan(&legacyMissing); err != nil || !legacyMissing {
		t.Fatalf("fresh database must use goose versioning: legacyMissing=%v err=%v", legacyMissing, err)
	}
	avatar, event := sampleAvatar(), sampleEvent()
	if err := fixture.repo.Create(fixture.ctx, avatar, event); err != nil {
		t.Fatal(err)
	}
	for column, limit := range map[string]int{"user_id": 255, "file_name": 255, "mime_type": 100, "s3_key": 500} {
		_, err := fixture.pool.Exec(fixture.ctx, "UPDATE avatars SET "+pgx.Identifier{column}.Sanitize()+" = $1 WHERE id = $2", strings.Repeat("x", limit+1), avatar.ID)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "22001" {
			t.Fatalf("column=%s accepted overlong value: %v", column, err)
		}
	}
	if _, err := fixture.pool.Exec(fixture.ctx, "UPDATE avatars SET processing_status = 'unknown' WHERE id = $1", avatar.ID); err == nil {
		t.Fatal("enum accepted an unknown processing status")
	}
	for _, status := range []string{domain.StatusProcessing, domain.StatusCompleted, domain.StatusFailed} {
		if _, err := fixture.pool.Exec(fixture.ctx, "UPDATE avatars SET processing_status = $1 WHERE id = $2", status, avatar.ID); err != nil {
			t.Fatalf("valid enum status %s rejected: %v", status, err)
		}
		got, err := fixture.repo.Get(fixture.ctx, avatar.ID)
		if err != nil || got.Status != status {
			t.Fatalf("enum did not round-trip through repository: got=%s want=%s err=%v", got.Status, status, err)
		}
	}
}

func TestPostgresMigrationsAdoptLegacyRegistryWithoutDataLoss(t *testing.T) {
	fixture := newMigrationFixture(t)
	installLegacySchema(t, fixture)
	avatar, event := sampleAvatar(), sampleEvent()
	if err := fixture.repo.Create(fixture.ctx, avatar, event); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.ProcessEvent(fixture.ctx, event, func(context.Context, domain.Avatar) (domain.JobResult, error) {
		return domain.JobResult{Status: domain.StatusCompleted, Thumbnails: avatar.Thumbnails}, nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := fixture.repo.Get(fixture.ctx, avatar.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.Migrate(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	after, err := fixture.repo.Get(fixture.ctx, avatar.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("legacy avatar changed: before=%+v after=%+v err=%v", before, after, err)
	}
	requireConstrainedSchema(t, fixture)
	requireSchemaVersion(t, fixture, 2)
	for _, table := range []string{"avatars", "outbox", "processed_messages", "schema_migrations"} {
		var count int
		if err := fixture.pool.QueryRow(fixture.ctx, "SELECT count(*) FROM "+pgx.Identifier{table}.Sanitize()).Scan(&count); err != nil || count != 1 {
			t.Fatalf("legacy table %s lost rows: count=%d err=%v", table, count, err)
		}
	}
	calls := 0
	if err := fixture.repo.ProcessEvent(fixture.ctx, event, func(context.Context, domain.Avatar) (domain.JobResult, error) {
		calls++
		return domain.JobResult{}, nil
	}); err != nil || calls != 0 {
		t.Fatalf("legacy inbox deduplication lost: calls=%d err=%v", calls, err)
	}
	sent, err := fixture.repo.DispatchPending(fixture.ctx, func(_ context.Context, got domain.Event) error {
		if !reflect.DeepEqual(got, event) {
			t.Errorf("legacy outbox payload changed: got=%+v want=%+v", got, event)
		}
		return nil
	})
	if !sent || err != nil {
		t.Fatalf("legacy outbox cannot dispatch: sent=%v err=%v", sent, err)
	}
	nextAvatar, nextEvent := sampleAvatar(), sampleEvent()
	nextAvatar.ID = uuid.NewString()
	nextEvent.ID = uuid.NewString()
	nextEvent.AvatarID = nextAvatar.ID
	if err := fixture.repo.Create(fixture.ctx, nextAvatar, nextEvent); err != nil {
		t.Fatal("new writes after legacy upgrade", err)
	}
	if err := fixture.repo.ProcessEvent(fixture.ctx, nextEvent, func(context.Context, domain.Avatar) (domain.JobResult, error) {
		return domain.JobResult{Status: domain.StatusCompleted}, nil
	}); err != nil {
		t.Fatal("processing after legacy upgrade", err)
	}
}

func TestPostgresMigrationsRejectInvalidLegacyRowsAtomically(t *testing.T) {
	for _, field := range []string{"user_id", "file_name", "mime_type", "s3_key", "processing_status"} {
		t.Run(field, func(t *testing.T) {
			fixture := newMigrationFixture(t)
			installLegacySchema(t, fixture)
			avatar := sampleAvatar()
			switch field {
			case "user_id":
				avatar.UserID = strings.Repeat("u", 256)
			case "file_name":
				avatar.FileName = strings.Repeat("f", 256)
			case "mime_type":
				avatar.MIMEType = strings.Repeat("m", 101)
			case "s3_key":
				avatar.S3Key = strings.Repeat("k", 501)
			case "processing_status":
				avatar.Status = "invalid-imported-status"
			}
			if err := fixture.repo.Create(fixture.ctx, avatar, sampleEvent()); err != nil {
				t.Fatal(err)
			}
			before, err := fixture.repo.Get(fixture.ctx, avatar.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.repo.Migrate(fixture.ctx); err == nil {
				t.Fatal("migration accepted invalid legacy metadata")
			}
			after, err := fixture.repo.Get(fixture.ctx, avatar.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected migration altered data: before=%+v after=%+v err=%v", before, after, err)
			}
			requireSchemaVersion(t, fixture, 1)
			var statusType string
			if err := fixture.pool.QueryRow(fixture.ctx, `SELECT data_type FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = 'avatars' AND column_name = 'processing_status'`).Scan(&statusType); err != nil || statusType != "text" {
				t.Fatalf("enum migration was partially committed: type=%s err=%v", statusType, err)
			}
			clean := sampleAvatar()
			if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE avatars SET user_id=$1, file_name=$2,
                mime_type=$3, s3_key=$4, processing_status=$5 WHERE id=$6`,
				clean.UserID, clean.FileName, clean.MIMEType, clean.S3Key, clean.Status, avatar.ID); err != nil {
				t.Fatal(err)
			}
			if err := fixture.repo.Migrate(fixture.ctx); err != nil {
				t.Fatal("retry after correcting invalid data", err)
			}
			requireConstrainedSchema(t, fixture)
			requireSchemaVersion(t, fixture, 2)
		})
	}
}

func TestPostgresMigrationsConcurrentStartup(t *testing.T) {
	fixture := newMigrationFixture(t)
	other, err := New(fixture.ctx, fixture.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(other.Close)
	start := make(chan struct{})
	errors := make(chan error, 2)
	for _, repo := range []*Repository{fixture.repo, other} {
		go func() {
			<-start
			errors <- repo.Migrate(fixture.ctx)
		}()
	}
	close(start)
	for range 2 {
		select {
		case err := <-errors:
			if err != nil {
				t.Fatal("concurrent migration failed", err)
			}
		case <-fixture.ctx.Done():
			t.Fatal("concurrent migration timed out", fixture.ctx.Err())
		}
	}
	requireConstrainedSchema(t, fixture)
	requireSchemaVersion(t, fixture, 2)
	for _, version := range []int{1, 2} {
		var count int
		if err := fixture.pool.QueryRow(fixture.ctx, "SELECT count(*) FROM goose_db_version WHERE version_id=$1 AND is_applied", version).Scan(&count); err != nil || count != 1 {
			t.Fatalf("version %d was applied %d times: %v", version, count, err)
		}
	}
}

func TestPostgresMigrationsRollbackAndReapply(t *testing.T) {
	fixture := newMigrationFixture(t)
	if err := fixture.repo.Migrate(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.Create(fixture.ctx, sampleAvatar(), sampleEvent()); err != nil {
		t.Fatal(err)
	}
	before, err := fixture.repo.Get(fixture.ctx, sampleAvatar().ID)
	if err != nil {
		t.Fatal(err)
	}
	provider := migrationProvider(t, fixture)
	result, err := provider.Down(fixture.ctx)
	if err != nil || result.Source.Version != 2 {
		t.Fatalf("rollback result=%+v err=%v", result, err)
	}
	requireSchemaVersion(t, fixture, 1)
	after, err := fixture.repo.Get(fixture.ctx, before.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("rollback changed avatar: before=%+v after=%+v err=%v", before, after, err)
	}
	if err := fixture.repo.Migrate(fixture.ctx); err != nil {
		t.Fatal("reapply constraints", err)
	}
	requireConstrainedSchema(t, fixture)
	requireSchemaVersion(t, fixture, 2)
	if _, err := provider.DownTo(fixture.ctx, 0); err != nil {
		t.Fatal("rollback initial schema", err)
	}
	requireSchemaVersion(t, fixture, 0)
	var absent bool
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT to_regclass('avatars') IS NULL AND
        to_regclass('outbox') IS NULL AND to_regclass('processed_messages') IS NULL`).Scan(&absent); err != nil || !absent {
		t.Fatalf("initial rollback did not remove test tables: absent=%v err=%v", absent, err)
	}
	if err := fixture.repo.Migrate(fixture.ctx); err != nil {
		t.Fatal("recreate initial schema", err)
	}
	requireConstrainedSchema(t, fixture)
}
