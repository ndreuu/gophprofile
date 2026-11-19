package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"gophprofile/internal/domain"
)

func databaseExporter(t *testing.T) *tracetest.InMemoryExporter {
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

func TestOutboxPersistsTraceAndRestoresItAfterRequestCompletes(t *testing.T) {
	exporter := databaseExporter(t)
	ctx, requestSpan := otel.Tracer("test").Start(context.Background(), "http request")
	requestContext := trace.SpanContextFromContext(ctx)
	repo, mock := repositoryMock(t)
	avatar, event := sampleAvatar(), sampleEvent()
	var payload []byte
	var storedEvent domain.Event
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO avatars").WithArgs(anyArgs(12)...).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectExec("INSERT INTO outbox").WithArgs(event.ID, matchArgument(func(value any) bool {
		var ok bool
		payload, ok = value.([]byte)
		return ok && json.Unmarshal(payload, &storedEvent) == nil && storedEvent.TraceContext["traceparent"] != ""
	}), event.CreatedAt).WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()
	if err := repo.Create(ctx, avatar, event); err != nil {
		t.Fatal(err)
	}
	requestSpan.End()
	persisted := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier(storedEvent.TraceContext)))
	if persisted.TraceID() != requestContext.TraceID() {
		t.Fatal("outbox did not persist the request trace")
	}
	mock.ExpectBegin()
	mock.ExpectQuery("FOR UPDATE SKIP LOCKED").WillReturnRows(pgxmock.NewRows([]string{"id", "payload"}).AddRow(event.ID, payload))
	mock.ExpectExec("UPDATE outbox SET sent_at").WithArgs(event.ID).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()
	var publishContext trace.SpanContext
	if sent, err := repo.DispatchPending(context.Background(), func(ctx context.Context, got domain.Event) error {
		publishContext = trace.SpanContextFromContext(ctx)
		if got.TraceContext["traceparent"] != storedEvent.TraceContext["traceparent"] {
			t.Fatal("dispatch changed the persisted event context")
		}
		return nil
	}); err != nil || !sent {
		t.Fatalf("sent=%v err=%v", sent, err)
	}
	if publishContext.TraceID() != requestContext.TraceID() {
		t.Fatal("dispatch started a disconnected trace")
	}
	spans := exporter.GetSpans()
	if len(spans) != 4 {
		t.Fatalf("expected request/create/outbox/dispatch spans, got %d", len(spans))
	}
	dispatch := spans[len(spans)-1]
	if dispatch.Parent.SpanID() != persisted.SpanID() || dispatch.SpanContext.SpanID() != publishContext.SpanID() {
		t.Fatalf("dispatch parent does not match stored span: %+v", dispatch)
	}
}

func TestObservabilityStatsReadsPersistentValuesAndSurfacesFailure(t *testing.T) {
	repo, mock := repositoryMock(t)
	mock.ExpectQuery("jsonb_array_elements").WillReturnRows(pgxmock.NewRows([]string{"storage_bytes", "queue_depth"}).AddRow(int64(12345), int64(7)))
	bytes, depth, acquired, idle, total, err := repo.ObservabilityStats(context.Background())
	if err != nil || bytes != 12345 || depth != 7 || acquired != 0 || idle != 0 || total != 0 {
		t.Fatalf("bytes=%d depth=%d pool=%d/%d/%d err=%v", bytes, depth, acquired, idle, total, err)
	}
	failure := errors.New("database unavailable")
	mock.ExpectQuery("jsonb_array_elements").WillReturnError(failure)
	if _, _, _, _, _, err := repo.ObservabilityStats(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("statistics failure was lost: %v", err)
	}
}

func TestDatabaseFailureMarksSpanWithoutSQL(t *testing.T) {
	exporter := databaseExporter(t)
	repo, mock := repositoryMock(t)
	mock.ExpectQuery("FROM avatars WHERE id").WithArgs(testAvatarID).WillReturnError(errors.New("secret SQL statement"))
	if _, err := repo.Get(context.Background(), testAvatarID); err == nil {
		t.Fatal("expected database failure")
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error || len(spans[0].Events) == 0 {
		t.Fatalf("database failure missing from span: %+v", spans)
	}
	for _, attr := range spans[0].Attributes {
		if attr.Key == "db.statement" || attr.Key == "db.query.text" {
			t.Fatal("span recorded SQL text")
		}
	}
}
