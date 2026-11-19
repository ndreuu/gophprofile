package observability

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Metrics struct {
	httpRequests   *prometheus.CounterVec
	httpDuration   *prometheus.HistogramVec
	uploads        *prometheus.CounterVec
	uploadDuration *prometheus.HistogramVec
	workerEvents   *prometheus.CounterVec
	workerDuration *prometheus.HistogramVec
	storageBytes   prometheus.Gauge
	queueDepth     prometheus.Gauge
	dbConnections  *prometheus.GaugeVec
}

func newMetrics() (*prometheus.Registry, *Metrics) {
	registry := prometheus.NewRegistry()
	metrics := &Metrics{
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gophprofile_http_requests_total", Help: "Completed HTTP requests by route template and response status.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gophprofile_http_request_duration_seconds", Help: "HTTP request duration in seconds.", Buckets: prometheus.DefBuckets,
		}, []string{"method", "route", "status"}),
		uploads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gophprofile_uploads_total", Help: "Avatar upload attempts by outcome.",
		}, []string{"status"}),
		uploadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gophprofile_upload_duration_seconds", Help: "Avatar upload duration in seconds.", Buckets: prometheus.DefBuckets,
		}, []string{"status"}),
		workerEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "gophprofile_worker_events_total", Help: "Worker event processing attempts by event type and outcome.",
		}, []string{"event_type", "status"}),
		workerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "gophprofile_worker_event_duration_seconds", Help: "Worker event processing duration in seconds.", Buckets: prometheus.DefBuckets,
		}, []string{"event_type", "status"}),
		storageBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gophprofile_storage_bytes", Help: "Logical bytes of active avatar originals and known thumbnail sizes.",
		}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "gophprofile_outbox_queue_depth", Help: "Number of unsent events in the transactional outbox.",
		}),
		dbConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "gophprofile_db_connections", Help: "PostgreSQL pool connections by state.",
		}, []string{"state"}),
	}
	registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		metrics.httpRequests, metrics.httpDuration, metrics.uploads, metrics.uploadDuration,
		metrics.workerEvents, metrics.workerDuration, metrics.storageBytes, metrics.queueDepth, metrics.dbConnections)
	metrics.SetDBConnections(0, 0, 0)
	return registry, metrics
}

func (m *Metrics) ObserveUpload(status string, seconds float64) {
	if m == nil {
		return
	}
	status = outcomeLabel(status)
	m.uploads.WithLabelValues(status).Inc()
	m.uploadDuration.WithLabelValues(status).Observe(durationValue(seconds))
}

func (m *Metrics) ObserveWorker(eventType, status string, seconds float64) {
	if m == nil {
		return
	}
	switch eventType {
	case "avatar.uploaded", "avatar.deleted":
	default:
		eventType = "other"
	}
	status = outcomeLabel(status)
	m.workerEvents.WithLabelValues(eventType, status).Inc()
	m.workerDuration.WithLabelValues(eventType, status).Observe(durationValue(seconds))
}

func (m *Metrics) SetStorageBytes(value int64) {
	if m != nil {
		m.storageBytes.Set(float64(max(value, 0)))
	}
}

func (m *Metrics) SetQueueDepth(value int64) {
	if m != nil {
		m.queueDepth.Set(float64(max(value, 0)))
	}
}

func (m *Metrics) SetDBConnections(acquired, idle, total int32) {
	if m != nil {
		m.dbConnections.WithLabelValues("acquired").Set(float64(max(acquired, 0)))
		m.dbConnections.WithLabelValues("idle").Set(float64(max(idle, 0)))
		m.dbConnections.WithLabelValues("total").Set(float64(max(total, 0)))
	}
}

func outcomeLabel(status string) string {
	switch status {
	case "success", "error", "canceled", "rejected":
		return status
	default:
		return "unknown"
	}
}

func durationValue(seconds float64) float64 {
	if seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0
	}
	return seconds
}
