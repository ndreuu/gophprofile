package migrations

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

const advisoryLockID int64 = 4815162342

func NewProvider(db *sql.DB) (*goose.Provider, error) {
	locker, err := lock.NewPostgresSessionLocker(
		lock.WithLockID(advisoryLockID),
		lock.WithLockTimeout(1, 90),
		lock.WithUnlockTimeout(1, 5),
	)
	if err != nil {
		return nil, fmt.Errorf("configure migration lock: %w", err)
	}
	provider, err := goose.NewProvider(goose.DialectPostgres, db, Files,
		goose.WithSessionLocker(locker),
		goose.WithDisableGlobalRegistry(true),
	)
	if err != nil {
		return nil, fmt.Errorf("configure goose migrations: %w", err)
	}
	return provider, nil
}

func Up(ctx context.Context, db *sql.DB) error {
	provider, err := NewProvider(db)
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("apply goose migrations: %w", err)
	}
	return nil
}
