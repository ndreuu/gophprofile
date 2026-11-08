package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"gophprofile/internal/domain"
)

type Worker struct {
	repo                 domain.Repository
	storage              domain.Storage
	processor            domain.ImageProcessor
	attempts             int
	baseDelay, timeLimit time.Duration
	logger               *slog.Logger
}

func New(repo domain.Repository, storage domain.Storage, processor domain.ImageProcessor, attempts int, baseDelay, maxDelay time.Duration, logger *slog.Logger) *Worker {
	if attempts < 1 {
		attempts = 3
	}
	if baseDelay <= 0 {
		baseDelay = 500 * time.Millisecond
	}
	if maxDelay < baseDelay {
		maxDelay = 10 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{repo: repo, storage: storage, processor: processor, attempts: attempts, baseDelay: baseDelay, timeLimit: maxDelay, logger: logger}
}

func (w *Worker) Handle(ctx context.Context, event domain.Event) error {
	if err := domain.ValidateEvent(event); err != nil {
		return err
	}
	delay := w.baseDelay
	var last error
	for attempt := 0; attempt < w.attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		last = w.repo.ProcessEvent(ctx, event, func(ctx context.Context, avatar domain.Avatar) (domain.JobResult, error) {
			if event.Type == domain.EventDeleted {

				keys := []string{avatar.S3Key}
				for _, thumb := range avatar.Thumbnails {
					keys = append(keys, thumb.S3Key)
				}

				keys = append(keys, thumbnailKey(avatar.ID, "100x100"), thumbnailKey(avatar.ID, "300x300"))
				for _, key := range keys {
					if err := w.storage.Delete(ctx, key); err != nil {
						return domain.JobResult{}, fmt.Errorf("delete image %s: %w", key, err)
					}
				}
				return domain.JobResult{}, nil
			}
			reader, err := w.storage.Get(ctx, avatar.S3Key)
			if err != nil {
				return domain.JobResult{}, fmt.Errorf("download original: %w", err)
			}
			data, readErr := io.ReadAll(io.LimitReader(reader, domain.MaxFileSize+1))
			closeErr := reader.Close()
			if readErr != nil {
				return domain.JobResult{}, fmt.Errorf("read original: %w", readErr)
			}
			if closeErr != nil {
				return domain.JobResult{}, fmt.Errorf("close original: %w", closeErr)
			}
			if int64(len(data)) > domain.MaxFileSize {
				return domain.JobResult{}, domain.ErrTooLarge
			}
			variants, err := w.processor.Thumbnails(data)
			if err != nil {
				return domain.JobResult{}, err
			}
			thumbs := make([]domain.Thumbnail, 0, len(variants))
			for _, variant := range variants {
				key := thumbnailKey(avatar.ID, variant.Size)
				if err := w.storage.Put(ctx, key, bytes.NewReader(variant.Data), int64(len(variant.Data)), variant.MIMEType); err != nil {
					return domain.JobResult{}, fmt.Errorf("store thumbnail: %w", err)
				}
				thumbs = append(thumbs, domain.Thumbnail{Size: variant.Size, S3Key: key, URL: fmt.Sprintf("/api/v1/avatars/%s?size=%s", avatar.ID, variant.Size)})
			}
			return domain.JobResult{Thumbnails: thumbs, Status: domain.StatusCompleted}, nil
		})
		if last == nil {
			return nil
		}
		w.logger.Warn("background task failed", "message_id", event.ID, "avatar_id", event.AvatarID, "attempt", attempt+1, "error", last)
		if attempt+1 < w.attempts {
			if err := wait(ctx, delay); err != nil {
				return err
			}
			delay = min(delay*2, w.timeLimit)
		}
	}
	return fmt.Errorf("processing event %s after %d attempts: %w", event.ID, w.attempts, last)
}
func thumbnailKey(id, size string) string { return "thumbnails/" + id + "/" + size + ".jpg" }
func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func Dispatch(ctx context.Context, outbox domain.Outbox, publisher domain.Publisher, interval time.Duration, logger *slog.Logger) {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	if logger == nil {
		logger = slog.Default()
	}
	for ctx.Err() == nil {
		publishCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		sent, err := outbox.DispatchPending(publishCtx, publisher.Publish)
		cancel()
		if err != nil {
			logger.Warn("outbox delivery postponed", "error", err)
		}
		if err != nil || !sent {
			if wait(ctx, interval) != nil {
				return
			}
		}
	}
}
