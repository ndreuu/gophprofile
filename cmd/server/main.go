package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gophprofile/internal/app"
	"gophprofile/internal/config"
	"gophprofile/internal/observability"
	"gophprofile/internal/transport/httpapi"
	"gophprofile/internal/worker"
)

func main() {
	health := flag.Bool("healthcheck", false, "check the running server and exit")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if *health {
		if err := healthcheck(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if err := run(); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
func healthcheck() error {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	client := http.Client{Timeout: 4 * time.Second}
	response, err := client.Get("http://" + addr + "/health")
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health returned %d", response.StatusCode)
	}
	return nil
}
func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serviceName := cfg.OTELServiceName
	if serviceName == "" {
		serviceName = "gophprofile-server"
	}
	telemetry, err := observability.New(ctx, observability.Options{ServiceName: serviceName, Environment: cfg.Environment, OTLPEndpoint: cfg.OTLPEndpoint, SampleRatio: cfg.TraceSampleRatio, LogLevel: cfg.LogLevel})
	if err != nil {
		return err
	}
	logger := telemetry.Logger
	slog.SetDefault(logger)
	defer func() {
		flush, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := telemetry.Shutdown(flush); err != nil {
			logger.Error("flush telemetry", "error", err)
		}
	}()
	startup, cancel := context.WithTimeout(ctx, 90*time.Second)
	resources, err := app.Open(startup, cfg, cfg.AppMode == "demo", logger)
	cancel()
	if err != nil {
		return err
	}
	defer resources.Close()
	resources.Service.WithMetrics(telemetry.Metrics)
	resources.Worker.WithMetrics(telemetry.Metrics)
	var jobs sync.WaitGroup
	jobs.Go(func() { resources.ObserveMetrics(ctx, telemetry.Metrics, logger) })
	if cfg.AppMode == "demo" {
		logger.Warn("demo mode: data is ephemeral; production uses PostgreSQL, MinIO and Kafka")
		jobs.Go(func() { worker.Dispatch(ctx, resources.Outbox, resources.Publisher, cfg.OutboxPollInterval, logger) })
		jobs.Go(func() {
			if err := resources.Consumer.Run(ctx, resources.Worker.Handle); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("demo worker stopped", "error", err)
				stop()
			}
		})
	}
	handler := httpapi.New(resources.Service, resources.Checks(), logger, httpapi.Options{AllowedOrigins: cfg.AllowedOrigins, RateLimitRequests: cfg.RateLimitRequests, RateLimitWindow: cfg.RateLimitWindow, Middleware: telemetry.Middleware, MetricsHandler: promhttp.HandlerFor(telemetry.Registry, promhttp.HandlerOpts{})})
	server := &http.Server{Addr: cfg.HTTPAddr, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 90 * time.Second, MaxHeaderBytes: 16 << 10,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-App-Mode", cfg.AppMode)
			handler.ServeHTTP(w, r)
		})}
	errCh := make(chan error, 1)
	go func() {
		logger.Info("HTTP server listening", "address", cfg.HTTPAddr, "mode", cfg.AppMode)
		errCh <- server.ListenAndServe()
	}()
	select {
	case err = <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	case <-ctx.Done():
	}
	stop()
	shutdown, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if shutdownErr := server.Shutdown(shutdown); shutdownErr != nil {
		_ = server.Close()
		if err == nil {
			err = shutdownErr
		}
	}
	jobs.Wait()
	return err
}
