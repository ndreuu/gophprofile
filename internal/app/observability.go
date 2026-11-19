package app

import (
	"context"
	"log/slog"
	"time"

	"gophprofile/internal/observability"
)

type statsSource interface {
	ObservabilityStats(context.Context) (int64, int64, int32, int32, int32, error)
}

func (r *Resources) ObserveMetrics(ctx context.Context, metrics *observability.Metrics, logger *slog.Logger) {
	source, ok := r.Repository.(statsSource)
	if !ok {
		return
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		collectCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		storage, queue, acquired, idle, total, err := source.ObservabilityStats(collectCtx)
		cancel()
		if err == nil {
			metrics.SetStorageBytes(storage)
			metrics.SetQueueDepth(queue)
			metrics.SetDBConnections(acquired, idle, total)
		} else if ctx.Err() == nil {
			logger.WarnContext(ctx, "collect infrastructure metrics", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
