//go:build integration

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"gophprofile/internal/domain"
)

func TestRealS3StorageLifecycleAndPrivacy(t *testing.T) {
	endpoint := os.Getenv("S3_INTEGRATION_ENDPOINT")
	if endpoint == "" {
		t.Skip("set S3_INTEGRATION_ENDPOINT to run against a real S3/MinIO server")
	}
	secure := strings.EqualFold(os.Getenv("S3_INTEGRATION_SSL"), "true")
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")
	access, secret := os.Getenv("S3_INTEGRATION_ACCESS_KEY"), os.Getenv("S3_INTEGRATION_SECRET_KEY")
	if access == "" {
		access = "gophprofile"
	}
	if secret == "" {
		secret = "gophprofile_dev_secret"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bucket := "gophprofile-test-" + uuid.NewString()
	store, err := New(ctx, endpoint, access, secret, bucket, secure)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = store.Delete(cleanup, "image.png")
		if err := store.client.RemoveBucket(cleanup, bucket); err != nil {
			t.Errorf("remove temporary bucket: %v", err)
		}
	})
	if err := store.EnsureBucket(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureBucket(ctx); err != nil {
		t.Fatalf("repeat initialization: %v", err)
	}
	if err := store.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	data := []byte("an opaque test payload")
	if err := store.Put(ctx, "image.png", bytes.NewReader(data), int64(len(data)), "image/png"); err != nil {
		t.Fatal(err)
	}
	reader, err := store.Get(ctx, "image.png")
	if err != nil {
		t.Fatal(err)
	}
	actual, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(data, actual) {
		t.Fatalf("got=%q read=%v close=%v", actual, readErr, closeErr)
	}
	metadata, err := store.client.StatObject(ctx, bucket, "image.png", minio.StatObjectOptions{})
	if err != nil || metadata.ContentType != "image/png" {
		t.Fatalf("metadata=%+v err=%v", metadata, err)
	}
	anonymous, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4("", "", ""), Secure: secure, BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := anonymous.StatObject(ctx, bucket, "image.png", minio.StatObjectOptions{}); err == nil {
		t.Fatal("anonymous client can access a private avatar")
	}
	for range 2 {
		if err := store.Delete(ctx, "image.png"); err != nil {
			t.Fatal(err)
		}
	}
	reader, err = store.Get(ctx, "image.png")
	if !errors.Is(err, domain.ErrNotFound) || reader != nil {
		t.Fatalf("expected eager missing-object error: reader=%v err=%v", reader, err)
	}
}
