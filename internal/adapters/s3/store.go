package s3

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"gophprofile/internal/domain"
)

type Store struct {
	client *minio.Client
	bucket string
}

func New(_ context.Context, endpoint, accessKey, secretKey, bucket string, secure bool) (*Store, error) {
	if bucket == "" {
		return nil, fmt.Errorf("S3 bucket must not be empty")
	}
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:       secure,
		BucketLookup: minio.BucketLookupPath,
	})
	if err != nil {
		return nil, fmt.Errorf("create S3 client: %w", err)
	}
	return &Store{client: client, bucket: bucket}, nil
}

func (s *Store) EnsureBucket(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("check S3 bucket: %w", err)
	}
	if !exists {
		if err := s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{}); err != nil {

			code := minio.ToErrorResponse(err).Code
			if code != "BucketAlreadyOwnedByYou" {
				return fmt.Errorf("create S3 bucket: %w", err)
			}
		}
	}

	if err := s.client.SetBucketPolicy(ctx, s.bucket, ""); err != nil {
		return fmt.Errorf("make S3 bucket private: %w", err)
	}
	return nil
}

func (s *Store) Put(ctx context.Context, key string, reader io.Reader, size int64, mime string) error {
	if key == "" || size < 0 || reader == nil {
		return fmt.Errorf("invalid object parameters: %w", domain.ErrInvalid)
	}
	_, err := s.client.PutObject(ctx, s.bucket, key, reader, size, minio.PutObjectOptions{ContentType: mime})
	if err != nil {
		return fmt.Errorf("put S3 object: %w", err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, objectError("get", err)
	}

	if _, err := object.Stat(); err != nil {
		_ = object.Close()
		return nil, objectError("stat", err)
	}
	return object, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
	if err != nil {
		if errors.Is(objectError("delete", err), domain.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("delete S3 object: %w", err)
	}
	return nil
}

func (s *Store) Ping(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("ping S3: %w", err)
	}
	if !exists {
		return fmt.Errorf("S3 bucket %q does not exist", s.bucket)
	}
	return nil
}

func (s *Store) Close() {}

func objectError(operation string, err error) error {
	switch minio.ToErrorResponse(err).Code {
	case "NoSuchKey", "NoSuchObject", "NotFound":
		return fmt.Errorf("%s S3 object: %w", operation, domain.ErrNotFound)
	default:
		return fmt.Errorf("%s S3 object: %w", operation, err)
	}
}

var _ domain.Storage = (*Store)(nil)
