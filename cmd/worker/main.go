package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gophprofile/internal/app"
	"gophprofile/internal/config"
	"gophprofile/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if cfg.AppMode == "demo" {
		logger.Info("demo worker runs inside cmd/server; start APP_MODE=demo go run ./cmd/server")
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 90*time.Second)
	resources, err := app.Open(startup, cfg, true, logger)
	cancel()
	if err != nil {
		return err
	}
	defer resources.Close()
	var jobs sync.WaitGroup
	jobs.Go(func() { worker.Dispatch(ctx, resources.Outbox, resources.Publisher, cfg.OutboxPollInterval, logger) })
	logger.Info("worker listening", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
	err = resources.Consumer.Run(ctx, resources.Worker.Handle)
	stop()
	jobs.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
