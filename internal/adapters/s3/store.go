package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

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

func (s *Store) EnsureBucket(ctx context.Context) (err error) {
	ctx, span := s.startSpan(ctx, "EnsureBucket")
	defer func() { finishStorageSpan(span, err) }()
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

func (s *Store) Put(ctx context.Context, key string, reader io.Reader, size int64, mime string) (err error) {
	ctx, span := s.startSpan(ctx, "PutObject")
	defer func() { finishStorageSpan(span, err) }()
	span.SetAttributes(attribute.Int64("aws.s3.object.size", size))
	if key == "" || size < 0 || reader == nil {
		return fmt.Errorf("invalid object parameters: %w", domain.ErrInvalid)
	}
	_, err = s.client.PutObject(ctx, s.bucket, key, reader, size, minio.PutObjectOptions{ContentType: mime})
	if err != nil {
		return fmt.Errorf("put S3 object: %w", err)
	}
	return nil
}

func (s *Store) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, span := s.startSpan(ctx, "GetObject")
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		finishStorageSpan(span, err)
		return nil, objectError("get", err)
	}

	if _, err := object.Stat(); err != nil {
		_ = object.Close()
		finishStorageSpan(span, err)
		return nil, objectError("stat", err)
	}
	return &tracedObject{ReadCloser: object, span: span}, nil
}

func (s *Store) Delete(ctx context.Context, key string) (err error) {
	ctx, span := s.startSpan(ctx, "DeleteObject")
	defer func() { finishStorageSpan(span, err) }()
	err = s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
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

func (s *Store) startSpan(ctx context.Context, operation string) (context.Context, trace.Span) {
	return otel.Tracer("gophprofile/s3").Start(ctx, "S3 "+operation,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("rpc.system", "aws-api"), attribute.String("rpc.service", "S3"),
			attribute.String("rpc.method", operation), attribute.String("aws.s3.bucket", s.bucket)))
}

func finishStorageSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(errors.New("S3 operation failed"))
		span.SetStatus(codes.Error, "S3 operation failed")
	}
	span.End()
}

type tracedObject struct {
	io.ReadCloser
	span trace.Span
	once sync.Once
}

func (o *tracedObject) Read(data []byte) (int, error) {
	n, err := o.ReadCloser.Read(data)
	if err != nil {
		if errors.Is(err, io.EOF) {
			o.finish(nil)
		} else {
			o.finish(err)
		}
	}
	return n, err
}

func (o *tracedObject) Close() error {
	err := o.ReadCloser.Close()
	o.finish(err)
	return err
}

func (o *tracedObject) finish(err error) {
	o.once.Do(func() { finishStorageSpan(o.span, err) })
}
