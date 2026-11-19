package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/trace"
	"gophprofile/internal/config"
	"gophprofile/internal/domain"
	"gophprofile/internal/observability"
	"gophprofile/internal/transport/httpapi"
)

func TestHTTPUploadOutboxWorkerTelemetry(t *testing.T) {
	ctx := context.Background()
	telemetry, err := observability.New(ctx, observability.Options{ServiceName: "test", SampleRatio: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = telemetry.Shutdown(ctx) })
	resources, err := Open(ctx, config.Config{AppMode: "demo"}, true, telemetry.Logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(resources.Close)
	resources.Service.WithMetrics(telemetry.Metrics)
	resources.Worker.WithMetrics(telemetry.Metrics)
	handler := httpapi.New(resources.Service, resources.Checks(), telemetry.Logger, httpapi.Options{
		Middleware:     telemetry.Middleware,
		MetricsHandler: promhttp.HandlerFor(telemetry.Registry, promhttp.HandlerOpts{}),
	})
	var imageBytes bytes.Buffer
	if err := png.Encode(&imageBytes, image.NewNRGBA(image.Rect(0, 0, 12, 12))); err != nil {
		t.Fatal(err)
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	file, err := writer.CreateFormFile("file", "avatar.png")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write(imageBytes.Bytes()); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	request.Header.Set("X-User-ID", "telemetry-user")
	const incomingTrace = "11111111111111111111111111111111"
	request.Header.Set("traceparent", "00-"+incomingTrace+"-2222222222222222-01")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("upload status %d: %s", response.Code, response.Body.String())
	}
	var avatar domain.Avatar
	if err := json.Unmarshal(response.Body.Bytes(), &avatar); err != nil {
		t.Fatal(err)
	}
	sent, err := resources.Outbox.DispatchPending(ctx, func(eventCtx context.Context, event domain.Event) error {
		if trace.SpanContextFromContext(eventCtx).TraceID().String() != incomingTrace {
			return fmt.Errorf("outbox lost HTTP trace context")
		}
		return resources.Worker.Handle(eventCtx, event)
	})
	if err != nil || !sent {
		t.Fatalf("dispatch: sent=%t err=%v", sent, err)
	}
	completed, err := resources.Service.Get(ctx, avatar.ID)
	if err != nil || completed.Status != domain.StatusCompleted || len(completed.Thumbnails) != 2 {
		t.Fatalf("worker result: %+v %v", completed, err)
	}
	for _, thumbnail := range completed.Thumbnails {
		if thumbnail.SizeBytes <= 0 {
			t.Fatal("thumbnail size was not preserved for storage accounting")
		}
	}
	metricsCtx, cancel := context.WithCancel(ctx)
	finished := make(chan struct{})
	go func() {
		resources.ObserveMetrics(metricsCtx, telemetry.Metrics, telemetry.Logger)
		close(finished)
	}()
	t.Cleanup(func() { cancel(); <-finished })
	deadline := time.Now().Add(time.Second)
	for {
		families, err := telemetry.Registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		storageObserved := false
		for _, family := range families {
			if family.GetName() == "gophprofile_storage_bytes" && family.Metric[0].GetGauge().GetValue() > float64(avatar.Size) {
				storageObserved = true
			}
		}
		if storageObserved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("infrastructure metrics did not include original and thumbnail sizes")
		}
		time.Sleep(time.Millisecond)
	}
	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, expected := range []string{
		`gophprofile_uploads_total{status="success"} 1`,
		`gophprofile_worker_events_total{event_type="avatar.uploaded",status="success"} 1`,
		`gophprofile_http_requests_total{method="POST",route="/api/v1/avatars",status="201"} 1`,
		`gophprofile_outbox_queue_depth 0`,
	} {
		if !strings.Contains(metricsResponse.Body.String(), expected) {
			t.Fatalf("metrics missing %s", expected)
		}
	}
}
