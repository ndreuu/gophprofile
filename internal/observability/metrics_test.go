package observability

import (
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func metricFamily(t *testing.T, registry *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q missing", name)
	return nil
}

func labelValues(metric *dto.Metric) map[string]string {
	labels := make(map[string]string)
	for _, label := range metric.GetLabel() {
		labels[label.GetName()] = label.GetValue()
	}
	return labels
}

func TestMetricsUsePrivateRegistriesAndBoundedBusinessLabels(t *testing.T) {
	registry, metrics := newMetrics()
	_, other := newMetrics()
	metrics.ObserveUpload("success", 0.25)
	metrics.ObserveUpload("user-specific-error-123", math.NaN())
	metrics.ObserveUpload("another-arbitrary-status", -1)
	metrics.ObserveWorker("avatar.uploaded", "error", 2)
	metrics.ObserveWorker("user-specific-event", "rejected", math.Inf(1))
	metrics.ObserveWorker("another-arbitrary-event", "rejected", 0)
	metrics.SetStorageBytes(512)
	metrics.SetQueueDepth(3)
	metrics.SetDBConnections(2, 4, 6)
	if value := metricFamily(t, registry, "gophprofile_storage_bytes").Metric[0].GetGauge().GetValue(); value != 512 {
		t.Fatalf("storage bytes = %v", value)
	}
	if value := metricFamily(t, registry, "gophprofile_outbox_queue_depth").Metric[0].GetGauge().GetValue(); value != 3 {
		t.Fatalf("queue depth = %v", value)
	}
	uploads := metricFamily(t, registry, "gophprofile_uploads_total")
	if len(uploads.Metric) != 2 {
		t.Fatalf("unbounded upload outcomes: %v", uploads)
	}
	for _, metric := range uploads.Metric {
		labels := labelValues(metric)
		if labels["status"] == "unknown" && metric.GetCounter().GetValue() != 2 {
			t.Fatalf("unknown upload count = %v", metric)
		}
	}
	workers := metricFamily(t, registry, "gophprofile_worker_events_total")
	if len(workers.Metric) != 2 {
		t.Fatalf("unbounded worker event types: %v", workers)
	}
	for _, metric := range metricFamily(t, registry, "gophprofile_upload_duration_seconds").Metric {
		if math.IsNaN(metric.GetHistogram().GetSampleSum()) || metric.GetHistogram().GetSampleSum() < 0 {
			t.Fatalf("invalid duration sample: %v", metric)
		}
	}
	connections := map[string]float64{}
	for _, metric := range metricFamily(t, registry, "gophprofile_db_connections").Metric {
		connections[labelValues(metric)["state"]] = metric.GetGauge().GetValue()
	}
	if connections["acquired"] != 2 || connections["idle"] != 4 || connections["total"] != 6 {
		t.Fatalf("connections = %v", connections)
	}
	if value := other.storageBytes; value == metrics.storageBytes {
		t.Fatal("registries share a gauge")
	}
	metricFamily(t, registry, "go_goroutines")
}

func TestMetricsNeverExportNegativeSizes(t *testing.T) {
	registry, metrics := newMetrics()
	metrics.SetStorageBytes(-1)
	metrics.SetQueueDepth(-2)
	metrics.SetDBConnections(-1, -1, -1)
	for _, name := range []string{"gophprofile_storage_bytes", "gophprofile_outbox_queue_depth", "gophprofile_db_connections"} {
		for _, metric := range metricFamily(t, registry, name).Metric {
			if metric.GetGauge().GetValue() != 0 {
				t.Fatalf("negative gauge: %v", metric)
			}
		}
	}
	var absent *Metrics
	absent.ObserveUpload("success", 1)
	absent.ObserveWorker("avatar.uploaded", "success", 1)
	absent.SetStorageBytes(1)
	absent.SetQueueDepth(1)
	absent.SetDBConnections(1, 1, 2)
}
