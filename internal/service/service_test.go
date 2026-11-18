package service_test

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"gophprofile/internal/adapters/memory"
	"gophprofile/internal/domain"
	"gophprofile/internal/imageproc"
	"gophprofile/internal/service"
	"gophprofile/internal/worker"
)

func picture(t *testing.T) []byte {
	t.Helper()
	im := image.NewRGBA(image.Rect(0, 0, 480, 240))
	for y := 0; y < 240; y++ {
		for x := 0; x < 480; x++ {
			im.Set(x, y, color.RGBA{R: uint8(x % 256), G: uint8(y), B: 130, A: 255})
		}
	}
	var b bytes.Buffer
	if err := png.Encode(&b, im); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func setup() (*service.Service, *memory.Repository, *memory.Storage, *worker.Worker) {
	r := memory.NewRepository()
	s := memory.NewStorage()
	p := imageproc.New()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return service.New(r, s, p), r, s, worker.New(r, s, p, 3, time.Millisecond, 4*time.Millisecond, logger)
}
func next(t *testing.T, r *memory.Repository) domain.Event {
	t.Helper()
	var e domain.Event
	sent, err := r.DispatchPending(context.Background(), func(_ context.Context, event domain.Event) error { e = event; return nil })
	if err != nil || !sent {
		t.Fatalf("next event: %v %v", sent, err)
	}
	return e
}
func TestAvatarLifecycle(t *testing.T) {
	ctx := context.Background()
	svc, repo, storage, w := setup()
	data := picture(t)
	a, err := svc.Upload(ctx, "user-1", "../../photo.png", data)
	if err != nil {
		t.Fatal(err)
	}
	if a.Status != domain.StatusProcessing || a.FileName != "photo.png" || a.Dimensions.Width != 480 {
		t.Fatalf("bad metadata: %+v", a)
	}
	_, body, mime, err := svc.Content(ctx, a.ID, "")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(body)
	_ = body.Close()
	if !bytes.Equal(got, data) || mime != "image/png" {
		t.Fatal("original changed")
	}
	_, _, _, err = svc.Content(ctx, a.ID, "100x100")
	if !errors.Is(err, domain.ErrProcessing) {
		t.Fatalf("wanted processing: %v", err)
	}
	e := next(t, repo)
	if err = w.Handle(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err = w.Handle(ctx, e); err != nil {
		t.Fatalf("duplicate: %v", err)
	}
	e.ID = uuid.NewString()
	if err = w.Handle(ctx, e); err != nil {
		t.Fatalf("repeated upload event: %v", err)
	}
	meta, err := svc.Get(ctx, a.ID)
	if err != nil || meta.Status != domain.StatusCompleted || len(meta.Thumbnails) != 2 {
		t.Fatalf("processed metadata: %+v %v", meta, err)
	}
	for _, size := range []string{"100x100", "300x300"} {
		_, r, ct, e := svc.Content(ctx, a.ID, size)
		if e != nil {
			t.Fatal(e)
		}
		cfg, format, e := image.DecodeConfig(r)
		_ = r.Close()
		if e != nil || format != "jpeg" || ct != "image/jpeg" || cfg.Width != cfg.Height {
			t.Fatalf("thumbnail: %+v %v", cfg, e)
		}
	}
	current, err := svc.Current(ctx, "user-1")
	if err != nil || current.ID != a.ID {
		t.Fatal("wrong current avatar")
	}
	list, err := svc.List(ctx, "user-1")
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v %v", list, err)
	}
	if err = svc.Delete(ctx, a.ID, "other"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("ownership: %v", err)
	}
	if err = svc.DeleteCurrent(ctx, "user-1", "other"); !errors.Is(err, domain.ErrForbidden) {
		t.Fatalf("current ownership: %v", err)
	}
	if err = svc.DeleteCurrent(ctx, "user-1", "user-1"); err != nil {
		t.Fatal(err)
	}
	if err = svc.Delete(ctx, a.ID, "user-1"); err != nil {
		t.Fatalf("repeat delete: %v", err)
	}
	if _, err = svc.Get(ctx, a.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("soft-deleted image visible")
	}
	deleted := next(t, repo)
	if err = w.Handle(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	if err = w.Handle(ctx, deleted); err != nil {
		t.Fatal(err)
	}
	if _, err = storage.Get(ctx, a.S3Key); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("original was not deleted")
	}
	for _, thumb := range meta.Thumbnails {
		if _, err = storage.Get(ctx, thumb.S3Key); !errors.Is(err, domain.ErrNotFound) {
			t.Fatal("thumbnail was not deleted")
		}
	}
	e.ID = uuid.NewString()
	if err = w.Handle(ctx, e); err != nil {
		t.Fatal(err)
	}
	if _, err = storage.Get(ctx, meta.Thumbnails[0].S3Key); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("delayed upload resurrected deleted avatar")
	}
	list, err = svc.List(ctx, "user-1")
	if err != nil || len(list) != 0 {
		t.Fatal("deleted image in gallery")
	}
}
func TestDeleteBeforeProcessing(t *testing.T) {
	ctx := context.Background()
	svc, r, s, w := setup()
	a, err := svc.Upload(ctx, "user", "x.png", picture(t))
	if err != nil {
		t.Fatal(err)
	}
	upload := next(t, r)
	if err = svc.Delete(ctx, a.ID, "user"); err != nil {
		t.Fatal(err)
	}
	del := next(t, r)
	if err = w.Handle(ctx, upload); err != nil {
		t.Fatal(err)
	}
	if err = w.Handle(ctx, del); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Get(ctx, a.S3Key); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("file survived deletion")
	}
}
func TestValidation(t *testing.T) {
	ctx := context.Background()
	svc, _, _, _ := setup()
	tests := []struct {
		name, user string
		data       []byte
		want       error
	}{
		{"missing user", "", []byte("x"), domain.ErrInvalid}, {"empty file", "u", nil, domain.ErrInvalid},
		{"unknown image", "u", []byte("not an image"), domain.ErrUnsupported}, {"large", "u", make([]byte, domain.MaxFileSize+1), domain.ErrTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := svc.Upload(ctx, tt.user, "x", tt.data)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v want %v", err, tt.want)
			}
		})
	}
	if _, err := svc.Get(ctx, "oops"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := svc.Current(ctx, ""); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := svc.List(ctx, ""); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if err := svc.Delete(ctx, "bad", "u"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if err := svc.Delete(ctx, uuid.NewString(), ""); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if err := svc.DeleteCurrent(ctx, "u", ""); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if err := svc.DeleteCurrent(ctx, "u", "u"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal(err)
	}
	if _, _, _, err := svc.Content(ctx, uuid.NewString(), "42x42"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, _, err := svc.Content(ctx, uuid.NewString(), "original"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal(err)
	}
}

type brokenRepo struct{ domain.Repository }

func (b brokenRepo) Create(context.Context, domain.Avatar, domain.Event) error {
	return errors.New("database offline")
}

type trackingStorage struct {
	*memory.Storage
	lastKey string
	fail    bool
}

func (s *trackingStorage) Put(ctx context.Context, key string, r io.Reader, n int64, mime string) error {
	s.lastKey = key
	if s.fail {
		return errors.New("s3 offline")
	}
	return s.Storage.Put(ctx, key, r, n, mime)
}
func TestUploadCompensation(t *testing.T) {
	ctx := context.Background()
	store := &trackingStorage{Storage: memory.NewStorage()}
	svc := service.New(brokenRepo{memory.NewRepository()}, store, imageproc.New())
	if _, err := svc.Upload(ctx, "user", "x.png", picture(t)); err == nil {
		t.Fatal("expected DB error")
	}
	if _, err := store.Get(ctx, store.lastKey); !errors.Is(err, domain.ErrNotFound) {
		t.Fatal("orphan original after failed insert")
	}
	store.fail = true
	if _, err := svc.Upload(ctx, "user", "x.png", picture(t)); err == nil {
		t.Fatal("expected S3 error")
	}
}

type uncertainCommitRepo struct{ *memory.Repository }

func (r uncertainCommitRepo) Create(ctx context.Context, a domain.Avatar, e domain.Event) error {
	if err := r.Repository.Create(ctx, a, e); err != nil {
		return err
	}
	return errors.New("connection lost after commit")
}
func TestAmbiguousCommitRetainsOriginal(t *testing.T) {
	repo := uncertainCommitRepo{memory.NewRepository()}
	store := &trackingStorage{Storage: memory.NewStorage()}
	svc := service.New(repo, store, imageproc.New())
	data := picture(t)
	if _, err := svc.Upload(context.Background(), "u", "x.png", data); err == nil {
		t.Fatal("expected uncertain commit error")
	}
	objects, err := repo.List(context.Background(), "u")
	if err != nil || len(objects) != 1 {
		t.Fatal("metadata not committed")
	}
	body, err := store.Get(context.Background(), objects[0].S3Key)
	if err != nil {
		t.Fatal("committed original deleted", err)
	}
	actual, _ := io.ReadAll(body)
	_ = body.Close()
	if !bytes.Equal(actual, data) {
		t.Fatal("original damaged")
	}
}

func TestContentFormattedConvertsAndReportsActualLength(t *testing.T) {
	ctx := context.Background()
	svc, repo, _, w := setup()
	original := picture(t)
	avatar, err := svc.Upload(ctx, "format-user", "source.png", original)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Handle(ctx, next(t, repo)); err != nil {
		t.Fatal(err)
	}
	for _, size := range []string{"original", "100x100", "300x300"} {
		for _, format := range []string{"jpeg", "png", "webp"} {
			t.Run(size+"/"+format, func(t *testing.T) {
				metadata, body, mime, err := svc.ContentFormatted(ctx, avatar.ID, size, format)
				if err != nil {
					t.Fatal(err)
				}
				actual, err := io.ReadAll(body)
				_ = body.Close()
				if err != nil {
					t.Fatal(err)
				}
				dimensions, decoded, err := image.DecodeConfig(bytes.NewReader(actual))
				if err != nil {
					t.Fatal(err)
				}
				if decoded != format || mime != "image/"+format || metadata.Size != int64(len(actual)) {
					t.Fatalf("wrong format or length: %s %s %d/%d", decoded, mime, metadata.Size, len(actual))
				}
				expected := 480
				if size == "100x100" {
					expected = 100
				}
				if size == "300x300" {
					expected = 300
				}
				if dimensions.Width != expected {
					t.Fatal("incorrect converted dimensions", dimensions)
				}
			})
		}
	}
	if _, _, _, err = svc.ContentFormatted(ctx, avatar.ID, "original", "gif"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal("unsupported format accepted", err)
	}
	_, body, _, err := svc.ContentFormatted(ctx, avatar.ID, "original", "")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(body)
	_ = body.Close()
	if !bytes.Equal(data, original) {
		t.Fatal("conversion mutated the original")
	}
}
