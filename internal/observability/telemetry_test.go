package observability

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func testTelemetry(t *testing.T) (*Telemetry, *tracetest.InMemoryExporter, *bytes.Buffer) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	registry, metrics := newMetrics()
	var logs bytes.Buffer
	telemetry := &Telemetry{Logger: newLogger(&logs, slog.LevelDebug), Registry: registry, Metrics: metrics, provider: provider,
		propagator: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})}
	t.Cleanup(func() {
		if err := telemetry.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return telemetry, exporter, &logs
}

func restoreGlobals(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		otel.SetTracerProvider(noop.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	})
}

func TestShutdownFlushesOTLPHTTPWithResourceAndTraceCorrelation(t *testing.T) {
	restoreGlobals(t)
	exported := make(chan *collectortrace.ExportTraceServiceRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" || r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/x-protobuf" {
			t.Errorf("unexpected OTLP request: %s %s %v", r.Method, r.URL.Path, r.Header)
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var request collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(data, &request); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		exported <- &request
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	telemetry, err := New(context.Background(), Options{ServiceName: "avatar-worker", Environment: "testing", OTLPEndpoint: server.URL, SampleRatio: 1, LogLevel: "debug"})
	if err != nil {
		t.Fatal(err)
	}
	_, span := otel.Tracer("test").Start(context.Background(), "upload")
	spanContext := span.SpanContext()
	span.End()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := telemetry.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := telemetry.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	select {
	case request := <-exported:
		if len(request.ResourceSpans) != 1 || len(request.ResourceSpans[0].ScopeSpans) != 1 || len(request.ResourceSpans[0].ScopeSpans[0].Spans) != 1 {
			t.Fatalf("unexpected exported spans: %v", request)
		}
		resource := map[string]string{}
		for _, attribute := range request.ResourceSpans[0].Resource.Attributes {
			resource[attribute.Key] = attribute.Value.GetStringValue()
		}
		if resource["service.name"] != "avatar-worker" || resource["deployment.environment.name"] != "testing" {
			t.Fatalf("resource attributes = %v", resource)
		}
		traceID := spanContext.TraceID()
		if !bytes.Equal(request.ResourceSpans[0].ScopeSpans[0].Spans[0].TraceId, traceID[:]) {
			t.Fatal("exported trace ID changed")
		}
	default:
		t.Fatal("shutdown did not flush traces")
	}
}

func TestSamplingHonorsRatioAndRemoteParent(t *testing.T) {
	restoreGlobals(t)
	telemetry, err := New(context.Background(), Options{SampleRatio: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = telemetry.Shutdown(context.Background()) }()
	_, root := otel.Tracer("test").Start(context.Background(), "not-sampled")
	if root.SpanContext().IsSampled() || !root.SpanContext().IsValid() {
		t.Fatal("zero-ratio root should carry valid unsampled trace context")
	}
	root.End()
	remote := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, Remote: true, TraceFlags: trace.FlagsSampled,
	})
	_, child := otel.Tracer("test").Start(trace.ContextWithRemoteSpanContext(context.Background(), remote), "remote-child")
	defer child.End()
	if !child.SpanContext().IsSampled() || child.SpanContext().TraceID() != remote.TraceID() {
		t.Fatal("sampled remote parent was not respected")
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(trace.ContextWithSpanContext(context.Background(), remote), carrier)
	if carrier.Get("traceparent") == "" {
		t.Fatal("W3C trace context propagator is not installed")
	}
}

func TestNewRejectsInvalidTelemetryOptions(t *testing.T) {
	for _, options := range []Options{
		{SampleRatio: -1}, {SampleRatio: 1.1}, {SampleRatio: math.NaN()}, {SampleRatio: math.Inf(1)},
		{LogLevel: "verbose"}, {OTLPEndpoint: "jaeger:4318"}, {OTLPEndpoint: "ftp://jaeger:4318"},
		{OTLPEndpoint: "http://user:pass@jaeger:4318"}, {OTLPEndpoint: "http://jaeger:4318/?secret=value"},
	} {
		if _, err := New(context.Background(), options); err == nil {
			t.Fatalf("options accepted: %+v", options)
		}
	}
}

func TestTraceEndpointAppendsSignalPathToBaseEndpoint(t *testing.T) {
	for raw, want := range map[string]string{
		"http://jaeger:4318":        "http://jaeger:4318/v1/traces",
		"http://jaeger:4318/":       "http://jaeger:4318/v1/traces",
		"https://collector/custom":  "https://collector/custom/v1/traces",
		"https://collector/custom/": "https://collector/custom/v1/traces",
	} {
		got, err := traceEndpoint(raw)
		if err != nil || got != want {
			t.Fatalf("traceEndpoint(%q) = %q, %v", raw, got, err)
		}
	}
}

func TestRecordErrorPreservesSuccessfulSpansAndRecordsFailures(t *testing.T) {
	telemetry, exporter, _ := testTelemetry(t)
	_, successful := telemetry.provider.Tracer("test").Start(context.Background(), "success")
	RecordError(successful, nil)
	successful.End()
	_, failed := telemetry.provider.Tracer("test").Start(context.Background(), "failed")
	RecordError(failed, errors.New("postgres://user:secret@database/private-user"))
	failed.End()
	spans := exporter.GetSpans()
	if spans[0].Status.Code != codes.Unset || len(spans[0].Events) != 0 {
		t.Fatalf("nil error changed span: %v", spans[0])
	}
	if spans[1].Status.Code != codes.Error || len(spans[1].Events) != 1 || spans[1].Events[0].Name != "exception" {
		t.Fatalf("failure not recorded: %v", spans[1])
	}
	if strings.Contains(spans[1].Status.Description, "secret") || strings.Contains(spans[1].Events[0].Attributes[1].Value.AsString(), "secret") {
		t.Fatal("trace contains raw error text")
	}
	RecordError(nil, errors.New("ignored"))
}
