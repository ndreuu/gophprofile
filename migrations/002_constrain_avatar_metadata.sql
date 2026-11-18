-- +goose Up
CREATE TYPE avatar_processing_status AS ENUM ('processing', 'completed', 'failed');

ALTER TABLE avatars
    ALTER COLUMN user_id TYPE VARCHAR(255),
    ALTER COLUMN file_name TYPE VARCHAR(255),
    ALTER COLUMN mime_type TYPE VARCHAR(100),
    ALTER COLUMN s3_key TYPE VARCHAR(500),
    ALTER COLUMN processing_status DROP DEFAULT;

ALTER TABLE avatars
    ALTER COLUMN processing_status TYPE avatar_processing_status
    USING processing_status::avatar_processing_status;

ALTER TABLE avatars
    ALTER COLUMN processing_status SET DEFAULT 'processing'::avatar_processing_status;

-- +goose Down
ALTER TABLE avatars ALTER COLUMN processing_status DROP DEFAULT;

ALTER TABLE avatars
    ALTER COLUMN processing_status TYPE TEXT USING processing_status::TEXT,
    ALTER COLUMN processing_status SET DEFAULT 'processing',
    ALTER COLUMN user_id TYPE TEXT,
    ALTER COLUMN file_name TYPE TEXT,
    ALTER COLUMN mime_type TYPE TEXT,
    ALTER COLUMN s3_key TYPE TEXT;

DROP TYPE avatar_processing_status;
