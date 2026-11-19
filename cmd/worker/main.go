package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gophprofile/internal/app"
	"gophprofile/internal/config"
	"gophprofile/internal/observability"
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
	serviceName := cfg.OTELServiceName
	if serviceName == "" {
		serviceName = "gophprofile-worker"
	}
	telemetry, err := observability.New(ctx, observability.Options{ServiceName: serviceName, Environment: cfg.Environment, OTLPEndpoint: cfg.OTLPEndpoint, SampleRatio: cfg.TraceSampleRatio, LogLevel: cfg.LogLevel})
	if err != nil {
		return err
	}
	logger = telemetry.Logger
	slog.SetDefault(logger)
	defer func() {
		flush, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := telemetry.Shutdown(flush); err != nil {
			logger.Error("flush telemetry", "error", err)
		}
	}()
	startup, cancel := context.WithTimeout(ctx, 90*time.Second)
	resources, err := app.Open(startup, cfg, true, logger)
	cancel()
	if err != nil {
		return err
	}
	defer resources.Close()
	resources.Worker.WithMetrics(telemetry.Metrics)
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(telemetry.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		checkCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		for _, check := range resources.Checks() {
			if check(checkCtx) != nil {
				http.Error(w, "unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	var jobs sync.WaitGroup
	jobs.Go(func() { resources.ObserveMetrics(ctx, telemetry.Metrics, logger) })
	errCh := make(chan error, 1)
	jobs.Go(func() {
		logger.Info("worker metrics listening", "address", cfg.MetricsAddr)
		if serveErr := server.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errCh <- serveErr
			stop()
		}
	})
	jobs.Go(func() { worker.Dispatch(ctx, resources.Outbox, resources.Publisher, cfg.OutboxPollInterval, logger) })
	logger.Info("worker listening", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
	err = resources.Consumer.Run(ctx, resources.Worker.Handle)
	stop()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()
	if shutdownErr := server.Shutdown(shutdown); shutdownErr != nil {
		_ = server.Close()
		logger.Error("stop metrics server", "error", shutdownErr)
	}
	jobs.Wait()
	select {
	case serveErr := <-errCh:
		return serveErr
	default:
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
