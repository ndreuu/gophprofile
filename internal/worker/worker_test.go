package worker_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/png"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"gophprofile/internal/adapters/memory"
	"gophprofile/internal/domain"
	"gophprofile/internal/imageproc"
	"gophprofile/internal/service"
	"gophprofile/internal/worker"
)

type flakyStorage struct {
	domain.Storage
	failures atomic.Int32
	puts     atomic.Int32
}

func (s *flakyStorage) Put(ctx context.Context, key string, r io.Reader, size int64, mime string) error {
	s.puts.Add(1)
	if s.failures.Add(-1) >= 0 {
		return errors.New("temporary S3 outage")
	}
	return s.Storage.Put(ctx, key, r, size, mime)
}
func fixture(t *testing.T) (*memory.Repository, *memory.Storage, domain.Event) {
	t.Helper()
	r := memory.NewRepository()
	s := memory.NewStorage()
	p := imageproc.New()
	svc := service.New(r, s, p)
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 20, 10))); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Upload(context.Background(), "u", "x.png", b.Bytes()); err != nil {
		t.Fatal(err)
	}
	var e domain.Event
	_, err := r.DispatchPending(context.Background(), func(_ context.Context, event domain.Event) error { e = event; return nil })
	if err != nil {
		t.Fatal(err)
	}
	return r, s, e
}
func logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func TestRetryEventuallySucceedsAndDeduplicates(t *testing.T) {
	r, s, e := fixture(t)
	flaky := &flakyStorage{Storage: s}
	flaky.failures.Store(1)
	w := worker.New(r, flaky, imageproc.New(), 3, time.Millisecond, 3*time.Millisecond, logger())
	if err := w.Handle(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if flaky.puts.Load() != 3 {
		t.Fatalf("puts=%d want failed+2 successful", flaky.puts.Load())
	}
	if err := w.Handle(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if flaky.puts.Load() != 3 {
		t.Fatal("duplicate work executed")
	}
}
func TestFailedWorkCanBeRetriedWithoutLostInbox(t *testing.T) {
	r, s, e := fixture(t)
	flaky := &flakyStorage{Storage: s}
	flaky.failures.Store(10)
	w := worker.New(r, flaky, imageproc.New(), 2, time.Millisecond, 2*time.Millisecond, logger())
	if err := w.Handle(context.Background(), e); err == nil {
		t.Fatal("expected failure")
	}
	a, _ := r.Get(context.Background(), e.AvatarID)
	if a.Status != domain.StatusProcessing {
		t.Fatal("failed job marked completed")
	}
	flaky.failures.Store(0)
	if err := w.Handle(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	a, _ = r.Get(context.Background(), e.AvatarID)
	if a.Status != domain.StatusCompleted {
		t.Fatal("retry did not finish")
	}
}
func TestCancellationAndInvalidMessage(t *testing.T) {
	r, s, e := fixture(t)
	w := worker.New(r, s, imageproc.New(), 0, 0, 0, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Handle(ctx, e); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	e.ID = "invalid"
	if err := w.Handle(context.Background(), e); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
}
func TestOutboxRetainsUnacknowledgedEvent(t *testing.T) {
	r := memory.NewRepository()
	id := uuid.NewString()
	e := domain.Event{ID: uuid.NewString(), AvatarID: id, UserID: "u", Type: domain.EventUploaded, CreatedAt: time.Now()}
	if err := r.Create(context.Background(), domain.Avatar{ID: id, UserID: "u"}, e); err != nil {
		t.Fatal(err)
	}
	sent, err := r.DispatchPending(context.Background(), func(context.Context, domain.Event) error { return errors.New("broker down") })
	if err == nil || sent {
		t.Fatal("failed publish marked sent")
	}
	bus := memory.NewBus()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { worker.Dispatch(ctx, r, bus, time.Millisecond, logger()); close(done) }()
	consumeCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	if err := bus.Run(consumeCtx, func(_ context.Context, received domain.Event) error {
		if received.ID != e.ID {
			t.Error("wrong message")
		}
		cancel()
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	<-done
	sent, err = r.DispatchPending(context.Background(), func(context.Context, domain.Event) error {
		t.Error("message delivered twice after success")
		return nil
	})
	if sent || err != nil {
		t.Fatal("nonempty outbox")
	}
}
