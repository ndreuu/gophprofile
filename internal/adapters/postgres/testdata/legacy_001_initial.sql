CREATE TABLE avatars (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    file_name TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    s3_key TEXT NOT NULL,
    width INTEGER NOT NULL CHECK (width > 0),
    height INTEGER NOT NULL CHECK (height > 0),
    thumbnails JSONB NOT NULL DEFAULT '[]'::jsonb,
    processing_status TEXT NOT NULL DEFAULT 'processing',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX avatars_user_active_idx
    ON avatars (user_id, created_at DESC, id DESC) WHERE deleted_at IS NULL;
CREATE INDEX avatars_processing_status_idx ON avatars (processing_status);

CREATE TABLE outbox (
    id UUID PRIMARY KEY,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ
);

CREATE INDEX outbox_pending_idx ON outbox (created_at, id) WHERE sent_at IS NULL;

CREATE TABLE processed_messages (
    message_id UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
