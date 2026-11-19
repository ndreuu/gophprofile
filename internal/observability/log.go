package observability

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

type traceHandler struct {
	handler slog.Handler
}

func newLogger(output io.Writer, level slog.Level) *slog.Logger {
	return slog.New(&traceHandler{handler: slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level})})
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.handler.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, record slog.Record) error {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		record = record.Clone()
		record.AddAttrs(slog.String("trace_id", spanContext.TraceID().String()), slog.String("span_id", spanContext.SpanID().String()))
	}
	return h.handler.Handle(ctx, record)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{handler: h.handler.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{handler: h.handler.WithGroup(name)}
}
