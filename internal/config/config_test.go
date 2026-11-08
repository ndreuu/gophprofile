package config

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func clearEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"APP_MODE", "HTTP_ADDR", "DATABASE_URL", "KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_GROUP_ID",
		"S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_BUCKET", "S3_USE_SSL", "SHUTDOWN_TIMEOUT",
		"OUTBOX_POLL_INTERVAL", "WORKER_RETRY_ATTEMPTS", "WORKER_RETRY_BASE_DELAY", "WORKER_RETRY_MAX_DELAY",
		"CORS_ALLOWED_ORIGINS", "RATE_LIMIT_REQUESTS", "RATE_LIMIT_WINDOW",
	} {
		old, had := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(key, old)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnvironment(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AppMode != "production" || cfg.HTTPAddr != ":8080" ||
		cfg.S3Endpoint != "localhost:9000" || cfg.S3Bucket != "avatars" || cfg.S3UseSSL ||
		cfg.KafkaTopic != "avatar-events" || cfg.KafkaGroupID != "avatar-workers" ||
		!reflect.DeepEqual(cfg.KafkaBrokers, []string{"localhost:19092"}) ||
		cfg.S3AccessKey != "gophprofile" || cfg.S3SecretKey != "gophprofile_dev_secret" ||
		cfg.WorkerRetryAttempts != 3 || cfg.WorkerRetryBaseDelay != 500*time.Millisecond ||
		cfg.WorkerRetryMaxDelay != 10*time.Second || cfg.ShutdownTimeout != 10*time.Second ||
		cfg.OutboxPollInterval != 250*time.Millisecond || !strings.HasPrefix(cfg.DatabaseURL, "postgres://") {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.AllowedOrigins != nil || cfg.RateLimitRequests != 120 || cfg.RateLimitWindow != time.Minute {
		t.Fatalf("unexpected security defaults: %+v", cfg)
	}
}

func TestLoadOverrides(t *testing.T) {
	clearEnvironment(t)
	for key, value := range map[string]string{
		"APP_MODE": "demo", "HTTP_ADDR": "127.0.0.1:8089", "DATABASE_URL": "postgresql://db:5432/custom",
		"KAFKA_BROKERS": "broker-a:9092, broker-b:9093", "KAFKA_TOPIC": "custom", "KAFKA_GROUP_ID": "workers",
		"S3_ENDPOINT": "minio:9000", "S3_ACCESS_KEY": "key", "S3_SECRET_KEY": "secret", "S3_BUCKET": "custom",
		"S3_USE_SSL": "true", "SHUTDOWN_TIMEOUT": "4s", "OUTBOX_POLL_INTERVAL": "75ms",
		"WORKER_RETRY_ATTEMPTS": "3", "WORKER_RETRY_BASE_DELAY": "100ms", "WORKER_RETRY_MAX_DELAY": "2s",
		"CORS_ALLOWED_ORIGINS": "https://app.example.com, http://localhost:5173", "RATE_LIMIT_REQUESTS": "75", "RATE_LIMIT_WINDOW": "30s",
	} {
		t.Setenv(key, value)
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AppMode != "demo" || cfg.HTTPAddr != "127.0.0.1:8089" || cfg.DatabaseURL != "postgresql://db:5432/custom" ||
		!reflect.DeepEqual(cfg.KafkaBrokers, []string{"broker-a:9092", "broker-b:9093"}) ||
		cfg.KafkaTopic != "custom" || cfg.KafkaGroupID != "workers" || cfg.S3Endpoint != "minio:9000" ||
		cfg.S3AccessKey != "key" || cfg.S3SecretKey != "secret" || cfg.S3Bucket != "custom" || !cfg.S3UseSSL ||
		cfg.ShutdownTimeout != 4*time.Second || cfg.OutboxPollInterval != 75*time.Millisecond ||
		cfg.WorkerRetryAttempts != 3 || cfg.WorkerRetryBaseDelay != 100*time.Millisecond || cfg.WorkerRetryMaxDelay != 2*time.Second {
		t.Fatalf("environment overrides were not applied: %+v", cfg)
	}
	if !reflect.DeepEqual(cfg.AllowedOrigins, []string{"https://app.example.com", "http://localhost:5173"}) ||
		cfg.RateLimitRequests != 75 || cfg.RateLimitWindow != 30*time.Second {
		t.Fatalf("security overrides were not applied: %+v", cfg)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct{ key, value string }{
		{"APP_MODE", "staging"}, {"HTTP_ADDR", "localhost"}, {"HTTP_ADDR", ":0"}, {"HTTP_ADDR", ":65536"},
		{"DATABASE_URL", "https://db:5432"}, {"DATABASE_URL", "postgres:///db"}, {"DATABASE_URL", "%invalid"},
		{"KAFKA_BROKERS", ""}, {"KAFKA_BROKERS", "kafka:9092,"}, {"KAFKA_BROKERS", "kafka:abc"},
		{"KAFKA_TOPIC", ""}, {"KAFKA_GROUP_ID", ""}, {"S3_ENDPOINT", "http://minio:9000"},
		{"S3_ACCESS_KEY", ""}, {"S3_SECRET_KEY", ""}, {"S3_BUCKET", ""}, {"S3_USE_SSL", "sometimes"},
		{"SHUTDOWN_TIMEOUT", "0s"}, {"OUTBOX_POLL_INTERVAL", "eventually"},
		{"WORKER_RETRY_ATTEMPTS", "0"}, {"WORKER_RETRY_ATTEMPTS", "101"}, {"WORKER_RETRY_ATTEMPTS", "many"},
		{"WORKER_RETRY_BASE_DELAY", "-1s"}, {"WORKER_RETRY_MAX_DELAY", "100ms"},
		{"RATE_LIMIT_REQUESTS", "0"}, {"RATE_LIMIT_REQUESTS", "10001"}, {"RATE_LIMIT_REQUESTS", "lots"},
		{"RATE_LIMIT_WINDOW", "0s"}, {"RATE_LIMIT_WINDOW", "-1m"}, {"RATE_LIMIT_WINDOW", "forever"},
		{"CORS_ALLOWED_ORIGINS", "*"}, {"CORS_ALLOWED_ORIGINS", "https://*.example.com"},
		{"CORS_ALLOWED_ORIGINS", "null"}, {"CORS_ALLOWED_ORIGINS", "ftp://example.com"},
		{"CORS_ALLOWED_ORIGINS", "https://example.com/path"}, {"CORS_ALLOWED_ORIGINS", "https://example.com/"},
		{"CORS_ALLOWED_ORIGINS", "https://example.com?x=1"}, {"CORS_ALLOWED_ORIGINS", "https://example.com?"},
		{"CORS_ALLOWED_ORIGINS", "https://example.com#fragment"},
		{"CORS_ALLOWED_ORIGINS", "https://example.com#"},
		{"CORS_ALLOWED_ORIGINS", "https://user:password@example.com"},
		{"CORS_ALLOWED_ORIGINS", "https://example.com,"}, {"CORS_ALLOWED_ORIGINS", "https://"},
		{"CORS_ALLOWED_ORIGINS", "https://example.com:0"}, {"CORS_ALLOWED_ORIGINS", "http://example.com:65536"},
		{"CORS_ALLOWED_ORIGINS", "https://example.com:"}, {"CORS_ALLOWED_ORIGINS", "https://bad host"},
	} {
		t.Run(test.key+"="+test.value, func(t *testing.T) {
			clearEnvironment(t)
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), test.key) {
				t.Fatalf("expected error mentioning %s, got %v", test.key, err)
			}
		})
	}
}

func TestAllowedOriginsCanonicalizesAndDeduplicates(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://APP.example.com:443,https://app.example.com,http://localhost:80,http://[::1]:5173")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"https://app.example.com", "http://localhost", "http://[::1]:5173"}
	if !reflect.DeepEqual(cfg.AllowedOrigins, want) {
		t.Fatalf("got origins %v, want %v", cfg.AllowedOrigins, want)
	}
}

func TestAllowedOriginsEmptyKeepsSameOriginOnly(t *testing.T) {
	clearEnvironment(t)
	t.Setenv("CORS_ALLOWED_ORIGINS", "  ")
	cfg, err := Load()
	if err != nil || cfg.AllowedOrigins != nil {
		t.Fatalf("empty origins must keep same-origin policy: origins=%v err=%v", cfg.AllowedOrigins, err)
	}
}
