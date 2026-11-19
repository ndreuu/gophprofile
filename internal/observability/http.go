package observability

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func (t *Telemetry) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" || r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		started := time.Now()
		ctx := t.propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		routeContext := chi.RouteContext(ctx)
		if routeContext == nil {
			routeContext = chi.NewRouteContext()
			if routes, ok := next.(chi.Routes); ok {
				routeContext.Routes = routes
			}
			ctx = context.WithValue(ctx, chi.RouteCtxKey, routeContext)
		}
		method := methodLabel(r.Method)
		ctx, span := t.provider.Tracer("gophprofile/http").Start(ctx, method,
			trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attribute.String("http.request.method", method)))
		defer span.End()
		wrapped := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		defer func() {
			recovered := recover()
			status := wrapped.Status()
			if recovered != nil {
				if status == 0 {
					status = http.StatusInternalServerError
				}
				RecordError(span, errors.New("HTTP handler panic"))
			}
			if status == 0 {
				status = http.StatusOK
			}
			route := routeContext.RoutePattern()
			if route == "" {
				route = "unmatched"
			}
			seconds := time.Since(started).Seconds()
			t.Metrics.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
			t.Metrics.httpDuration.WithLabelValues(method, route, strconv.Itoa(status)).Observe(seconds)
			span.SetName(method + " " + route)
			span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.response.status_code", status))
			if status >= http.StatusInternalServerError {
				span.SetStatus(codes.Error, http.StatusText(status))
			}
			level := slog.LevelInfo
			if status >= http.StatusInternalServerError {
				level = slog.LevelError
			}
			t.Logger.LogAttrs(ctx, level, "HTTP request", slog.String("method", method), slog.String("route", route),
				slog.Int("status", status), slog.Float64("duration_seconds", seconds), slog.Int("response_bytes", wrapped.BytesWritten()))
			span.End()
			if recovered != nil {
				panic(recovered)
			}
		}()
		next.ServeHTTP(wrapped, r.WithContext(ctx))
	})
}

func methodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodConnect, http.MethodOptions, http.MethodTrace:
		return method
	default:
		return "OTHER"
	}
}
