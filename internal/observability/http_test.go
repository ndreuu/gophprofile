package observability

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func TestMiddlewareUsesRouteTemplatesAndPropagatesContext(t *testing.T) {
	telemetry, exporter, logs := testTelemetry(t)
	router := chi.NewRouter()
	router.Use(telemetry.Middleware)
	router.Get("/avatars/{avatarID}", func(w http.ResponseWriter, r *http.Request) {
		if !trace.SpanContextFromContext(r.Context()).IsValid() || baggage.FromContext(r.Context()).Member("test").Value() != "present" {
			t.Error("trace context or baggage did not reach handler")
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = io.WriteString(w, "image")
	})
	for _, id := range []string{"private-id-one", "private-id-two"} {
		request := httptest.NewRequest(http.MethodGet, "/avatars/"+id, nil)
		request.Header.Set("traceparent", "00-0102030405060708090a0b0c0d0e0f10-1122334455667788-01")
		request.Header.Set("baggage", "test=present")
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK || recorder.Body.String() != "image" || recorder.Header().Get("Content-Type") != "image/png" {
			t.Fatalf("response changed: %v %s", recorder.Result(), recorder.Body.String())
		}
	}
	requests := metricFamily(t, telemetry.Registry, "gophprofile_http_requests_total")
	if len(requests.Metric) != 1 || requests.Metric[0].GetCounter().GetValue() != 2 {
		t.Fatalf("route metrics = %v", requests)
	}
	labels := labelValues(requests.Metric[0])
	if labels["route"] != "/avatars/{avatarID}" || labels["status"] != "200" || labels["method"] != "GET" {
		t.Fatalf("labels = %v", labels)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 || spans[0].Name != "GET /avatars/{avatarID}" || spans[0].SpanKind != trace.SpanKindServer || !spans[0].Parent.IsRemote() {
		t.Fatalf("HTTP spans = %v", spans)
	}
	if spans[0].SpanContext.TraceID().String() != "0102030405060708090a0b0c0d0e0f10" || spans[0].Parent.SpanID().String() != "1122334455667788" {
		t.Fatal("incoming trace parent changed")
	}
	if strings.Contains(logs.String(), "private-id-") {
		t.Fatalf("raw URL leaked into access logs: %s", logs.String())
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(strings.Split(strings.TrimSpace(logs.String()), "\n")[0]), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["trace_id"] != spans[0].SpanContext.TraceID().String() || entry["span_id"] != spans[0].SpanContext.SpanID().String() {
		t.Fatalf("access log correlation = %v", entry)
	}
}

func TestMiddlewareBoundsUnknownRoutesAndMethodsWhenWrappingRouter(t *testing.T) {
	telemetry, exporter, _ := testTelemetry(t)
	router := chi.NewRouter()
	router.Get("/known/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := telemetry.Middleware(router)
	for _, target := range []string{"/unknown/private-one", "/unknown/private-two"} {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("CUSTOM", target, nil))
	}
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/known/123", nil))
	requests := metricFamily(t, telemetry.Registry, "gophprofile_http_requests_total")
	if len(requests.Metric) != 2 {
		t.Fatalf("unknown path cardinality = %v", requests)
	}
	for _, metric := range requests.Metric {
		labels := labelValues(metric)
		if labels["route"] == "unmatched" && (labels["method"] != "OTHER" || metric.GetCounter().GetValue() != 2) {
			t.Fatalf("unknown route labels = %v", metric)
		}
	}
	if len(exporter.GetSpans()) != 3 {
		t.Fatalf("spans = %v", exporter.GetSpans())
	}
}

func TestMiddlewareBypassesMetricsAndHealth(t *testing.T) {
	telemetry, exporter, logs := testTelemetry(t)
	handler := telemetry.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	for _, path := range []string{"/health", "/metrics"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusNoContent {
			t.Fatalf("bypassed response = %d", recorder.Code)
		}
	}
	if len(exporter.GetSpans()) != 0 || logs.Len() != 0 {
		t.Fatal("metrics and health generated telemetry")
	}
	families, err := telemetry.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), "gophprofile_http_") {
			t.Fatalf("probe metrics exported: %s", family.GetName())
		}
	}
}

func TestMiddlewarePreservesFlushAndRecordsServerErrors(t *testing.T) {
	telemetry, exporter, _ := testTelemetry(t)
	handler := telemetry.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.(http.Flusher).Flush()
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/upload", nil))
	if !recorder.Flushed || recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("response writer capabilities lost: %v", recorder)
	}
	if spans := exporter.GetSpans(); len(spans) != 1 || spans[0].Status.Code != codes.Error {
		t.Fatalf("error span = %v", spans)
	}
}

func TestMiddlewareRecordsAndRethrowsPanics(t *testing.T) {
	telemetry, exporter, _ := testTelemetry(t)
	handler := telemetry.Middleware(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	func() {
		defer func() {
			if recovered := recover(); recovered != "boom" {
				t.Errorf("panic = %v", recovered)
			}
		}()
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/panic", nil))
	}()
	if spans := exporter.GetSpans(); len(spans) != 1 || spans[0].Status.Code != codes.Error || len(spans[0].Events) != 1 {
		t.Fatalf("panic span = %v", spans)
	}
	labels := labelValues(metricFamily(t, telemetry.Registry, "gophprofile_http_requests_total").Metric[0])
	if labels["status"] != "500" {
		t.Fatalf("panic status = %v", labels)
	}
}
