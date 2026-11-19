package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestJSONLogsIncludeContextualTraceAndSpanIDs(t *testing.T) {
	var output bytes.Buffer
	logger := newLogger(&output, slog.LevelWarn).With("service", "test")
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6},
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext)
	logger.InfoContext(ctx, "filtered")
	logger.ErrorContext(ctx, "failed", "operation", "upload")
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["trace_id"] != spanContext.TraceID().String() || record["span_id"] != spanContext.SpanID().String() {
		t.Fatalf("missing trace correlation: %s", output.String())
	}
	if record["service"] != "test" || record["operation"] != "upload" || record["level"] != "ERROR" {
		t.Fatalf("missing structured context: %s", output.String())
	}
}

func TestJSONLogsWithoutSpanOmitCorrelation(t *testing.T) {
	var output bytes.Buffer
	newLogger(&output, slog.LevelDebug).WithGroup("request").With("method", "GET").Debug("accepted")
	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if _, exists := record["trace_id"]; exists {
		t.Fatal("unexpected trace ID without span")
	}
	if record["request"].(map[string]any)["method"] != "GET" {
		t.Fatalf("group lost: %s", output.String())
	}
}
