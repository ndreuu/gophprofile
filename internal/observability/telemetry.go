package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Options struct {
	ServiceName  string
	Environment  string
	OTLPEndpoint string
	SampleRatio  float64
	LogLevel     string
}

type Telemetry struct {
	Logger     *slog.Logger
	Registry   *prometheus.Registry
	Metrics    *Metrics
	provider   *sdktrace.TracerProvider
	propagator propagation.TextMapPropagator
}

func New(ctx context.Context, options Options) (*Telemetry, error) {
	if options.SampleRatio < 0 || options.SampleRatio > 1 || math.IsNaN(options.SampleRatio) || math.IsInf(options.SampleRatio, 0) {
		return nil, fmt.Errorf("trace sample ratio must be between 0 and 1")
	}
	var level slog.Level
	if options.LogLevel != "" {
		if err := level.UnmarshalText([]byte(strings.TrimSpace(options.LogLevel))); err != nil {
			return nil, fmt.Errorf("invalid log level: %w", err)
		}
	}
	serviceName := strings.TrimSpace(options.ServiceName)
	if serviceName == "" {
		serviceName = "gophprofile"
	}
	environment := strings.TrimSpace(options.Environment)
	if environment == "" {
		environment = "development"
	}
	providerOptions := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", serviceName), attribute.String("deployment.environment.name", environment))),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(options.SampleRatio))),
	}
	if options.OTLPEndpoint != "" {
		endpoint, err := traceEndpoint(options.OTLPEndpoint)
		if err != nil {
			return nil, err
		}
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint), otlptracehttp.WithTimeout(5*time.Second))
		if err != nil {
			return nil, fmt.Errorf("initialize OTLP trace exporter: %w", err)
		}
		providerOptions = append(providerOptions, sdktrace.WithBatcher(exporter))
	}
	provider := sdktrace.NewTracerProvider(providerOptions...)
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	registry, metrics := newMetrics()
	logger := newLogger(os.Stdout, level).With("service", serviceName, "environment", environment)
	telemetry := &Telemetry{Logger: logger, Registry: registry, Metrics: metrics, provider: provider, propagator: propagator}
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagator)
	return telemetry, nil
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	if t == nil || t.provider == nil {
		return nil
	}
	return t.provider.Shutdown(ctx)
}

func RecordError(span trace.Span, err error) {
	if err != nil && span != nil {
		span.RecordError(errors.New("operation failed"), trace.WithAttributes(attribute.String("error.type", fmt.Sprintf("%T", err))))
		span.SetStatus(codes.Error, "operation failed")
	}
}

func traceEndpoint(raw string) (string, error) {
	endpoint, err := url.Parse(raw)
	if err != nil || endpoint == nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Fragment != "" || endpoint.RawQuery != "" || endpoint.ForceQuery {
		return "", fmt.Errorf("OTLP endpoint must be an http or https URL without credentials, query or fragment")
	}
	return endpoint.JoinPath("v1/traces").String(), nil
}
