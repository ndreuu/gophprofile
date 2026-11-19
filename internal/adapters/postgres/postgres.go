package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"gophprofile/internal/domain"
	"gophprofile/migrations"
)

type database interface {
	Begin(context.Context) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Ping(context.Context) error
	Close()
}

type Repository struct {
	pool    database
	migrate func(context.Context) error
}

var _ domain.Repository = (*Repository)(nil)
var _ domain.Outbox = (*Repository)(nil)

func New(ctx context.Context, dsn string) (*Repository, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("configure PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect PostgreSQL: %w", err)
	}
	return &Repository{
		pool: pool,
		migrate: func(ctx context.Context) error {
			db := stdlib.OpenDBFromPool(pool)
			defer func() { _ = db.Close() }()
			if err := migrations.Up(ctx, db); err != nil {
				return err
			}
			pool.Reset()
			return nil
		},
	}, nil
}

// Close releases the connection pool.
func (r *Repository) Close() { r.pool.Close() }

// Ping verifies that PostgreSQL is reachable.
func (r *Repository) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

// Migrate applies embedded goose migrations with session advisory locking.
func (r *Repository) Migrate(ctx context.Context) error {
	if r.migrate == nil {
		return errors.New("PostgreSQL migrator is not configured")
	}
	if err := r.migrate(ctx); err != nil {
		return fmt.Errorf("migrate PostgreSQL: %w", err)
	}
	return nil
}

const avatarColumns = `id::text, user_id::text, file_name::text, mime_type::text, size_bytes, s3_key::text,
    width, height, thumbnails, processing_status::text, created_at, updated_at, deleted_at`

type storedThumbnail struct {
	Size      string `json:"size"`
	URL       string `json:"url"`
	S3Key     string `json:"s3_key"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

func marshalThumbnails(thumbnails []domain.Thumbnail) ([]byte, error) {
	stored := make([]storedThumbnail, 0, len(thumbnails))
	for _, thumbnail := range thumbnails {
		stored = append(stored, storedThumbnail(thumbnail))
	}
	return json.Marshal(stored)
}

type scanner interface{ Scan(...any) error }

func scanAvatar(row scanner) (domain.Avatar, error) {
	var avatar domain.Avatar
	var raw []byte
	err := row.Scan(&avatar.ID, &avatar.UserID, &avatar.FileName, &avatar.MIMEType,
		&avatar.Size, &avatar.S3Key, &avatar.Dimensions.Width, &avatar.Dimensions.Height,
		&raw, &avatar.Status, &avatar.CreatedAt, &avatar.UpdatedAt, &avatar.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Avatar{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Avatar{}, fmt.Errorf("read avatar: %w", err)
	}
	var stored []storedThumbnail
	if err := json.Unmarshal(raw, &stored); err != nil {
		return domain.Avatar{}, fmt.Errorf("decode stored thumbnails: %w", err)
	}
	avatar.Thumbnails = make([]domain.Thumbnail, 0, len(stored))
	for _, thumbnail := range stored {
		avatar.Thumbnails = append(avatar.Thumbnails, domain.Thumbnail(thumbnail))
	}
	avatar.URL = "/api/v1/avatars/" + avatar.ID
	return avatar, nil
}

// Create commits metadata and its upload event atomically.
func (r *Repository) Create(ctx context.Context, avatar domain.Avatar, event domain.Event) (err error) {
	ctx, span := startDatabaseSpan(ctx, "create_avatar")
	defer func() { finishDatabaseSpan(span, err) }()
	thumbnails, err := marshalThumbnails(avatar.Thumbnails)
	if err != nil {
		return fmt.Errorf("encode thumbnails: %w", err)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin create: %w", err)
	}
	defer rollback(ctx, tx)
	_, err = tx.Exec(ctx, `INSERT INTO avatars
        (id, user_id, file_name, mime_type, size_bytes, s3_key, width, height,
         thumbnails, processing_status, created_at, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
		avatar.ID, avatar.UserID, avatar.FileName, avatar.MIMEType, avatar.Size, avatar.S3Key,
		avatar.Dimensions.Width, avatar.Dimensions.Height, thumbnails, avatar.Status,
		avatar.CreatedAt, avatar.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert avatar: %w", err)
	}
	if err := insertOutbox(ctx, tx, event); err != nil {
		return err
	}
	return commit(ctx, tx, "create avatar")
}

func (r *Repository) Get(ctx context.Context, id string) (avatar domain.Avatar, err error) {
	ctx, span := startDatabaseSpan(ctx, "get_avatar")
	defer func() { finishDatabaseSpan(span, err) }()
	return scanAvatar(r.pool.QueryRow(ctx, "SELECT "+avatarColumns+" FROM avatars WHERE id = $1 AND deleted_at IS NULL", id))
}

// Current returns the newest non-deleted avatar with a stable UUID tie-breaker.
func (r *Repository) Current(ctx context.Context, userID string) (avatar domain.Avatar, err error) {
	ctx, span := startDatabaseSpan(ctx, "current_avatar")
	defer func() { finishDatabaseSpan(span, err) }()
	return scanAvatar(r.pool.QueryRow(ctx, "SELECT "+avatarColumns+` FROM avatars
        WHERE user_id = $1 AND deleted_at IS NULL ORDER BY created_at DESC, id DESC LIMIT 1`, userID))
}

func (r *Repository) List(ctx context.Context, userID string) (result []domain.Avatar, err error) {
	ctx, span := startDatabaseSpan(ctx, "list_avatars")
	defer func() { finishDatabaseSpan(span, err) }()
	rows, err := r.pool.Query(ctx, "SELECT "+avatarColumns+` FROM avatars
        WHERE user_id = $1 AND deleted_at IS NULL ORDER BY created_at DESC, id DESC`, userID)
	if err != nil {
		return nil, fmt.Errorf("list avatars: %w", err)
	}
	defer rows.Close()
	avatars := make([]domain.Avatar, 0)
	for rows.Next() {
		avatar, err := scanAvatar(rows)
		if err != nil {
			return nil, err
		}
		avatars = append(avatars, avatar)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate avatars: %w", err)
	}
	return avatars, nil
}

// SoftDelete serializes with processing so deletion includes every completed file.
func (r *Repository) SoftDelete(ctx context.Context, id, userID string, event domain.Event) (err error) {
	ctx, span := startDatabaseSpan(ctx, "soft_delete_avatar")
	defer func() { finishDatabaseSpan(span, err) }()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin delete: %w", err)
	}
	defer rollback(ctx, tx)
	avatar, err := scanAvatar(tx.QueryRow(ctx, "SELECT "+avatarColumns+" FROM avatars WHERE id = $1 FOR UPDATE", id))
	if err != nil {
		return err
	}
	if avatar.UserID != userID {
		return domain.ErrForbidden
	}
	if avatar.DeletedAt != nil {
		return commit(ctx, tx, "delete avatar")
	}
	if _, err := tx.Exec(ctx, "UPDATE avatars SET deleted_at = now(), updated_at = now() WHERE id = $1", id); err != nil {
		return fmt.Errorf("soft-delete avatar: %w", err)
	}
	event.Type = domain.EventDeleted
	event.AvatarID, event.UserID = avatar.ID, avatar.UserID
	event.S3Key = ""
	event.S3Keys = storageKeys(avatar)
	if err := insertOutbox(ctx, tx, event); err != nil {
		return err
	}
	return commit(ctx, tx, "delete avatar")
}

func storageKeys(avatar domain.Avatar) []string {
	keys := make([]string, 0, len(avatar.Thumbnails)+1)
	seen := make(map[string]struct{})
	appendKey := func(key string) {
		if key != "" {
			if _, ok := seen[key]; !ok {
				keys = append(keys, key)
				seen[key] = struct{}{}
			}
		}
	}
	appendKey(avatar.S3Key)
	for _, thumbnail := range avatar.Thumbnails {
		appendKey(thumbnail.S3Key)
	}
	return keys
}

// ProcessEvent atomically commits the inbox marker and successful work. Callback
// writes to S3 must use deterministic keys because external writes cannot roll back.
func (r *Repository) ProcessEvent(ctx context.Context, event domain.Event, handler domain.EventHandler) (err error) {
	ctx, span := startDatabaseSpan(ctx, "process_event")
	defer func() { finishDatabaseSpan(span, err) }()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin processing: %w", err)
	}
	defer rollback(ctx, tx)
	avatar, err := scanAvatar(tx.QueryRow(ctx, "SELECT "+avatarColumns+" FROM avatars WHERE id = $1 FOR UPDATE", event.AvatarID))
	if err != nil {
		return err
	}
	if avatar.UserID != event.UserID {
		return fmt.Errorf("%w: event owner does not match avatar", domain.ErrInvalid)
	}
	if event.Type == domain.EventDeleted && avatar.DeletedAt == nil {
		return fmt.Errorf("%w: delete event targets an active avatar", domain.ErrInvalid)
	}
	var inserted string
	err = tx.QueryRow(ctx, `INSERT INTO processed_messages (message_id) VALUES ($1)
        ON CONFLICT (message_id) DO NOTHING RETURNING message_id::text`, event.ID).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		return commit(ctx, tx, "duplicate message")
	}
	if err != nil {
		return fmt.Errorf("record processed message: %w", err)
	}
	if event.Type == domain.EventUploaded && (avatar.DeletedAt != nil || avatar.Status == domain.StatusCompleted) {
		return commit(ctx, tx, "skip obsolete upload")
	}
	result, err := handler(ctx, avatar)
	if err != nil {
		return fmt.Errorf("process %s: %w", event.Type, err)
	}
	if result.Thumbnails != nil {
		avatar.Thumbnails = result.Thumbnails
	}
	if result.Status != "" {
		avatar.Status = result.Status
	}
	thumbnails, err := marshalThumbnails(avatar.Thumbnails)
	if err != nil {
		return fmt.Errorf("encode processed thumbnails: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE avatars SET thumbnails = $2, processing_status = $3,
        updated_at = now() WHERE id = $1`, avatar.ID, thumbnails, avatar.Status); err != nil {
		return fmt.Errorf("save processing result: %w", err)
	}
	return commit(ctx, tx, "process message")
}

// DispatchPending publishes one event while holding its outbox row lock. A lost
// broker acknowledgement may cause redelivery; the worker's inbox handles it.
func (r *Repository) DispatchPending(ctx context.Context, publish func(context.Context, domain.Event) error) (dispatched bool, err error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin outbox dispatch: %w", err)
	}
	defer rollback(ctx, tx)
	var id string
	var payload []byte
	err = tx.QueryRow(ctx, `SELECT id::text, payload FROM outbox WHERE sent_at IS NULL
        ORDER BY created_at, id LIMIT 1 FOR UPDATE SKIP LOCKED`).Scan(&id, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read outbox: %w", err)
	}
	var event domain.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return false, fmt.Errorf("decode outbox event: %w", err)
	}
	ctx = propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier(event.TraceContext))
	ctx, span := startDatabaseSpan(ctx, "dispatch_outbox")
	defer func() { finishDatabaseSpan(span, err) }()
	if err := publish(ctx, event); err != nil {
		return false, fmt.Errorf("publish outbox event: %w", err)
	}
	if _, err := tx.Exec(ctx, "UPDATE outbox SET sent_at = now() WHERE id = $1", id); err != nil {
		return false, fmt.Errorf("mark outbox event sent: %w", err)
	}
	if err := commit(ctx, tx, "dispatch outbox"); err != nil {
		return false, err
	}
	return true, nil
}

type executor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func insertOutbox(ctx context.Context, tx executor, event domain.Event) (err error) {
	ctx, span := startDatabaseSpan(ctx, "insert_outbox")
	defer func() { finishDatabaseSpan(span, err) }()
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	event.TraceContext = carrier
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode outbox event: %w", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO outbox (id, payload, created_at) VALUES ($1, $2, $3)", event.ID, payload, event.CreatedAt); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func commit(ctx context.Context, tx pgx.Tx, operation string) error {
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", operation, err)
	}
	return nil
}

func rollback(ctx context.Context, tx pgx.Tx) { _ = tx.Rollback(ctx) }

func startDatabaseSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return otel.Tracer("gophprofile/postgres").Start(ctx, "postgresql "+operation,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system.name", "postgresql"), attribute.String("db.operation.name", operation)))
}

func finishDatabaseSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(errors.New("PostgreSQL operation failed"))
		span.SetStatus(codes.Error, "PostgreSQL operation failed")
	}
	span.End()
}

// ObservabilityStats reads the persistent active object bytes and pending outbox count.
func (r *Repository) ObservabilityStats(ctx context.Context) (storageBytes int64, queueDepth int64, acquired, idle, total int32, err error) {
	ctx, span := startDatabaseSpan(ctx, "observability_stats")
	defer func() { finishDatabaseSpan(span, err) }()
	err = r.pool.QueryRow(ctx, `SELECT
  (SELECT COALESCE(sum(size_bytes + COALESCE((
   SELECT sum(COALESCE((thumbnail->>'size_bytes')::bigint, 0))
   FROM jsonb_array_elements(thumbnails) AS thumbnail
  ), 0)), 0)::bigint FROM avatars WHERE deleted_at IS NULL),
  (SELECT count(*) FROM outbox WHERE sent_at IS NULL)`).Scan(&storageBytes, &queueDepth)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("read observability statistics: %w", err)
	}
	if pool, ok := r.pool.(*pgxpool.Pool); ok {
		stats := pool.Stat()
		acquired, idle, total = stats.AcquiredConns(), stats.IdleConns(), stats.TotalConns()
	}
	return storageBytes, queueDepth, acquired, idle, total, nil
}
