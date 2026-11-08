package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"gophprofile/internal/adapters/kafka"
	"gophprofile/internal/adapters/memory"
	"gophprofile/internal/adapters/postgres"
	"gophprofile/internal/adapters/s3"
	"gophprofile/internal/config"
	"gophprofile/internal/domain"
	"gophprofile/internal/imageproc"
	"gophprofile/internal/service"
	"gophprofile/internal/worker"
)

type consumer interface {
	Run(context.Context, func(context.Context, domain.Event) error) error
}
type Resources struct {
	Service    *service.Service
	Worker     *worker.Worker
	Repository domain.Repository
	Storage    domain.Storage
	Publisher  domain.Publisher
	Outbox     domain.Outbox
	Consumer   consumer
	close      func()
}

func (r *Resources) Close() {
	if r.close != nil {
		r.close()
	}
}
func (r *Resources) Checks() map[string]func(context.Context) error {
	return map[string]func(context.Context) error{"database": r.Repository.Ping, "storage": r.Storage.Ping, "broker": r.Publisher.Ping}
}

func Open(ctx context.Context, cfg config.Config, withConsumer bool, logger *slog.Logger) (*Resources, error) {
	resources := &Resources{}
	processor := imageproc.New()
	if cfg.AppMode == "demo" {
		repo := memory.NewRepository()
		store := memory.NewStorage()
		bus := memory.NewBus()
		resources.Repository = repo
		resources.Outbox = repo
		resources.Storage = store
		resources.Publisher = bus
		resources.Consumer = bus
	} else {
		var repo *postgres.Repository
		if err := retry(ctx, func() error { var err error; repo, err = postgres.New(ctx, cfg.DatabaseURL); return err }); err != nil {
			return nil, fmt.Errorf("initialize metadata store: %w", err)
		}
		if err := repo.Migrate(ctx); err != nil {
			repo.Close()
			return nil, err
		}
		store, err := s3.New(ctx, cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3UseSSL)
		if err != nil {
			repo.Close()
			return nil, err
		}
		if err := retry(ctx, func() error { return store.EnsureBucket(ctx) }); err != nil {
			repo.Close()
			return nil, err
		}
		publisher, err := kafka.NewPublisher(cfg.KafkaBrokers, cfg.KafkaTopic)
		if err != nil {
			repo.Close()
			return nil, err
		}
		if err := retry(ctx, func() error { return publisher.EnsureTopics(ctx) }); err != nil {
			publisher.Close()
			repo.Close()
			return nil, err
		}
		resources.Repository = repo
		resources.Outbox = repo
		resources.Storage = store
		resources.Publisher = publisher
		resources.close = func() { publisher.Close(); repo.Close() }
		if withConsumer {
			c, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID)
			if err != nil {
				resources.Close()
				return nil, err
			}
			resources.Consumer = c
			resources.close = func() { c.Close(); publisher.Close(); repo.Close() }
		}
	}
	resources.Service = service.New(resources.Repository, resources.Storage, processor)
	resources.Worker = worker.New(resources.Repository, resources.Storage, processor, cfg.WorkerRetryAttempts, cfg.WorkerRetryBaseDelay, cfg.WorkerRetryMaxDelay, logger)
	return resources, nil
}
func retry(ctx context.Context, fn func() error) error {
	var last error
	for attempt := 0; attempt < 30; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("startup cancelled: %w", err)
		}
		if last = fn(); last == nil {
			return nil
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("startup: %w", ctx.Err())
		case <-timer.C:
		}
	}
	return last
}
