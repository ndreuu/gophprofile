package kafka

import (
	"context"
	"encoding/json"
	"errors"
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

	"gophprofile/internal/domain"
)

func messagingExporter(t *testing.T) *tracetest.InMemoryExporter {
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

func incomingMessagingContext() context.Context {
	state, _ := trace.ParseTraceState("vendor=value")
	return trace.ContextWithRemoteSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3}, SpanID: trace.SpanID{4, 5, 6}, TraceFlags: trace.FlagsSampled,
		TraceState: state, Remote: true,
	}))
}

func TestPublisherAndConsumerPropagatePersistedW3CContext(t *testing.T) {
	exporter := messagingExporter(t)
	ctx := incomingMessagingContext()
	original := trace.SpanContextFromContext(ctx)
	event := validEvent()
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	event.TraceContext = carrier
	client := &fakeClient{}
	publisher := &Publisher{client: client, topic: "avatars"}
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	record := client.records[0]
	producerContext := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), recordCarrier{record}))
	if producerContext.TraceID() != original.TraceID() || producerContext.TraceState().String() != "vendor=value" {
		t.Fatalf("publisher lost persisted context: %v", producerContext)
	}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond, logger: testLogger()}
	var handlerContext trace.SpanContext
	if err := consumer.handleRecord(context.Background(), record, func(ctx context.Context, _ domain.Event) error {
		handlerContext = trace.SpanContextFromContext(ctx)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("expected producer and consumer spans, got %d", len(spans))
	}
	producer, consumerSpan := spans[0], spans[1]
	if producer.SpanKind != trace.SpanKindProducer || producer.Parent.SpanID() != original.SpanID() {
		t.Fatalf("incorrect producer parent: %+v", producer)
	}
	if consumerSpan.SpanKind != trace.SpanKindConsumer || consumerSpan.Parent.SpanID() != producer.SpanContext.SpanID() {
		t.Fatalf("incorrect consumer parent: %+v", consumerSpan)
	}
	if handlerContext.TraceID() != original.TraceID() || handlerContext.SpanID() != consumerSpan.SpanContext.SpanID() {
		t.Fatalf("handler received wrong context: %v", handlerContext)
	}
}

func TestMalformedEventKeepsTraceContextThroughDeadLetter(t *testing.T) {
	exporter := messagingExporter(t)
	ctx := incomingMessagingContext()
	record := validRecord(t)
	record.Value = []byte("malformed")
	propagation.TraceContext{}.Inject(ctx, recordCarrier{record})
	client := &fakeClient{}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond, logger: testLogger()}
	if err := consumer.handleRecord(context.Background(), record, func(context.Context, domain.Event) error {
		t.Fatal("malformed record reached handler")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	dead := client.records[0]
	deadContext := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), recordCarrier{dead}))
	if deadContext.TraceID() != trace.SpanContextFromContext(ctx).TraceID() || deadContext.TraceState().String() != "vendor=value" {
		t.Fatalf("dead letter lost trace context: %v", deadContext)
	}
	spans := exporter.GetSpans()
	if len(spans) != 2 || spans[0].SpanKind != trace.SpanKindProducer || spans[1].SpanKind != trace.SpanKindConsumer {
		t.Fatalf("unexpected dead-letter trace: %+v", spans)
	}
	if spans[0].Parent.SpanID() != spans[1].SpanContext.SpanID() || deadContext.SpanID() != spans[0].SpanContext.SpanID() {
		t.Fatal("dead-letter producer must continue the consumer span")
	}
	if spans[1].Status.Code != codes.Error || len(spans[1].Events) == 0 {
		t.Fatal("rejected message must be recorded as a consumer error")
	}
}

func TestPublisherSpanRecordsSafeFailure(t *testing.T) {
	exporter := messagingExporter(t)
	client := &fakeClient{produceErrs: []error{errors.New("sensitive-token")}}
	publisher := &Publisher{client: client, topic: "avatars"}
	if err := publisher.Publish(context.Background(), validEvent()); err == nil {
		t.Fatal("expected broker failure")
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error || len(spans[0].Events) == 0 {
		t.Fatalf("failure missing from span: %+v", spans)
	}
	for _, event := range spans[0].Events {
		for _, attr := range event.Attributes {
			if strings.Contains(attr.Value.AsString(), "sensitive-token") {
				t.Fatal("span exposed the raw infrastructure error")
			}
		}
	}
}

func TestConsumerUsesEventContextWithoutBrokerHeaders(t *testing.T) {
	exporter := messagingExporter(t)
	event := validEvent()
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(incomingMessagingContext(), carrier)
	event.TraceContext = carrier
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	ambient, ambientSpan := otel.Tracer("test").Start(context.Background(), "consumer runtime")
	defer ambientSpan.End()
	client := &fakeClient{}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond, logger: testLogger()}
	record := validRecord(t)
	record.Value = data
	if err := consumer.handleRecord(ambient, record, func(ctx context.Context, _ domain.Event) error {
		if trace.SpanContextFromContext(ctx).TraceID() != trace.SpanContextFromContext(incomingMessagingContext()).TraceID() {
			t.Fatal("consumer used the runtime span instead of the persisted event trace")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Parent.SpanID() != trace.SpanContextFromContext(incomingMessagingContext()).SpanID() {
		t.Fatalf("event trace was not restored: %+v", spans)
	}
}
