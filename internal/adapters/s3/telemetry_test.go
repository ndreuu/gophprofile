package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

func storageExporter(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		_ = provider.Shutdown(context.Background())
	})
	return exporter
}

func TestGetSpanCoversStreamAndPreservesWorkerContext(t *testing.T) {
	exporter := storageExporter(t)
	ctx, workerSpan := otel.Tracer("test").Start(context.Background(), "worker")
	workerContext := trace.SpanContextFromContext(ctx)
	content := "avatar bytes"
	var requestContexts []trace.SpanContext
	store := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		requestContexts = append(requestContexts, trace.SpanContextFromContext(r.Context()))
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.Header().Set("ETag", `"5eb63bbbe01eeed093cb22bb8f5acdc3"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		if r.Method == http.MethodGet {
			_, _ = io.WriteString(w, content)
		}
	})
	reader, err := store.Get(ctx, "private-key.png")
	if err != nil {
		t.Fatal(err)
	}
	if spans := exporter.GetSpans(); len(spans) != 0 {
		t.Fatal("download span ended before the stream was read")
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != content {
		t.Fatalf("content=%q err=%v", data, err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	workerSpan.End()
	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("expected one download and one worker span, got %d", len(spans))
	}
	download := spans[0]
	if download.Name != "S3 GetObject" || download.SpanKind != trace.SpanKindClient || download.Parent.SpanID() != workerContext.SpanID() {
		t.Fatalf("unexpected download span: %+v", download)
	}
	if len(requestContexts) < 2 {
		t.Fatalf("expected stat and streamed download requests, got %d", len(requestContexts))
	}
	for _, requestContext := range requestContexts {
		if requestContext.TraceID() != workerContext.TraceID() || requestContext.SpanID() != download.SpanContext.SpanID() {
			t.Fatal("S3 request lost the worker's tracing context")
		}
	}
	for _, attr := range download.Attributes {
		if attr.Value.AsString() == "private-key.png" {
			t.Fatal("object key must not be recorded as a span attribute")
		}
	}
}

func TestMissingObjectEndsSpanWithError(t *testing.T) {
	exporter := storageExporter(t)
	store := testStore(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-amz-error-code", "NoSuchKey")
		w.WriteHeader(http.StatusNotFound)
	})
	if _, err := store.Get(context.Background(), "missing"); err == nil {
		t.Fatal("expected missing object")
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error || len(spans[0].Events) == 0 {
		t.Fatalf("missing S3 error span: %+v", spans)
	}
}
