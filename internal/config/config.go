package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	AppMode              string
	HTTPAddr             string
	DatabaseURL          string
	KafkaBrokers         []string
	KafkaTopic           string
	KafkaGroupID         string
	S3Endpoint           string
	S3AccessKey          string
	S3SecretKey          string
	S3Bucket             string
	S3UseSSL             bool
	ShutdownTimeout      time.Duration
	OutboxPollInterval   time.Duration
	WorkerRetryAttempts  int
	WorkerRetryBaseDelay time.Duration
	WorkerRetryMaxDelay  time.Duration
	AllowedOrigins       []string
	RateLimitRequests    int
	RateLimitWindow      time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		AppMode:             env("APP_MODE", "production"),
		HTTPAddr:            env("HTTP_ADDR", ":8080"),
		DatabaseURL:         env("DATABASE_URL", "postgres://gophprofile:gophprofile_dev@localhost:5432/gophprofile?sslmode=disable"),
		KafkaTopic:          env("KAFKA_TOPIC", "avatar-events"),
		KafkaGroupID:        env("KAFKA_GROUP_ID", "avatar-workers"),
		S3Endpoint:          env("S3_ENDPOINT", "localhost:9000"),
		S3AccessKey:         env("S3_ACCESS_KEY", "gophprofile"),
		S3SecretKey:         env("S3_SECRET_KEY", "gophprofile_dev_secret"),
		S3Bucket:            env("S3_BUCKET", "avatars"),
		WorkerRetryAttempts: 3,
		RateLimitRequests:   120,
	}
	for _, broker := range strings.Split(env("KAFKA_BROKERS", "localhost:19092"), ",") {
		broker = strings.TrimSpace(broker)
		if broker == "" {
			return Config{}, fmt.Errorf("KAFKA_BROKERS must contain non-empty host:port addresses")
		}
		if err := validAddress(broker); err != nil {
			return Config{}, fmt.Errorf("KAFKA_BROKERS: %w", err)
		}
		cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
	}
	if cfg.AppMode != "production" && cfg.AppMode != "demo" {
		return Config{}, fmt.Errorf("APP_MODE must be production or demo")
	}
	if err := validAddress(cfg.HTTPAddr); err != nil {
		return Config{}, fmt.Errorf("HTTP_ADDR: %w", err)
	}
	dsn, err := url.Parse(cfg.DatabaseURL)
	if err != nil || (dsn.Scheme != "postgres" && dsn.Scheme != "postgresql") || dsn.Host == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must be a postgres:// or postgresql:// URL with a host")
	}
	if err := validAddress(cfg.S3Endpoint); err != nil {
		return Config{}, fmt.Errorf("S3_ENDPOINT must be host:port without a URL scheme: %w", err)
	}
	if cfg.S3AccessKey == "" || cfg.S3SecretKey == "" || cfg.S3Bucket == "" {
		return Config{}, fmt.Errorf("S3_ACCESS_KEY, S3_SECRET_KEY and S3_BUCKET must not be empty")
	}
	if cfg.KafkaTopic == "" || cfg.KafkaGroupID == "" {
		return Config{}, fmt.Errorf("KAFKA_TOPIC and KAFKA_GROUP_ID must not be empty")
	}
	cfg.AllowedOrigins, err = originEnv("CORS_ALLOWED_ORIGINS")
	if err != nil {
		return Config{}, err
	}
	if raw, ok := os.LookupEnv("RATE_LIMIT_REQUESTS"); ok {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 10000 {
			return Config{}, fmt.Errorf("RATE_LIMIT_REQUESTS must be an integer between 1 and 10000")
		}
		cfg.RateLimitRequests = value
	}
	cfg.S3UseSSL, err = boolEnv("S3_USE_SSL", false)
	if err != nil {
		return Config{}, err
	}
	for _, item := range []struct {
		name     string
		fallback time.Duration
		target   *time.Duration
	}{
		{"SHUTDOWN_TIMEOUT", 10 * time.Second, &cfg.ShutdownTimeout},
		{"OUTBOX_POLL_INTERVAL", 250 * time.Millisecond, &cfg.OutboxPollInterval},
		{"WORKER_RETRY_BASE_DELAY", 500 * time.Millisecond, &cfg.WorkerRetryBaseDelay},
		{"WORKER_RETRY_MAX_DELAY", 10 * time.Second, &cfg.WorkerRetryMaxDelay},
		{"RATE_LIMIT_WINDOW", time.Minute, &cfg.RateLimitWindow},
	} {
		value, err := durationEnv(item.name, item.fallback)
		if err != nil {
			return Config{}, err
		}
		*item.target = value
	}
	if raw, ok := os.LookupEnv("WORKER_RETRY_ATTEMPTS"); ok {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			return Config{}, fmt.Errorf("WORKER_RETRY_ATTEMPTS must be an integer between 1 and 100")
		}
		cfg.WorkerRetryAttempts = value
	}
	if cfg.WorkerRetryMaxDelay < cfg.WorkerRetryBaseDelay {
		return Config{}, fmt.Errorf("WORKER_RETRY_MAX_DELAY must be at least WORKER_RETRY_BASE_DELAY")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func validAddress(address string) error {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("invalid host:port address")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	return nil
}

func boolEnv(key string, fallback bool) (bool, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return value, nil
}

func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, for example 500ms or 10s", key)
	}
	return value, nil
}

func originEnv(key string) ([]string, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil, nil
	}
	origins := make([]string, 0)
	seen := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		origin, err := url.Parse(strings.TrimSpace(item))
		if err != nil || origin == nil ||
			(origin.Scheme != "http" && origin.Scheme != "https") || origin.Hostname() == "" ||
			origin.User != nil || origin.Path != "" || origin.RawPath != "" ||
			origin.RawQuery != "" || origin.ForceQuery || origin.Fragment != "" ||
			origin.RawFragment != "" || strings.ContainsAny(item, "*?#") || strings.HasSuffix(origin.Host, ":") {
			return nil, fmt.Errorf("%s must contain explicit http/https origins without paths, credentials, query strings, fragments or wildcards", key)
		}
		host := strings.ToLower(origin.Hostname())
		port := origin.Port()
		if port != "" {
			number, err := strconv.Atoi(port)
			if err != nil || number < 1 || number > 65535 {
				return nil, fmt.Errorf("%s contains an invalid origin port", key)
			}
			if (origin.Scheme == "http" && number == 80) || (origin.Scheme == "https" && number == 443) {
				port = ""
			}
		}
		if port != "" {
			host = net.JoinHostPort(host, port)
		} else if strings.Contains(host, ":") {
			host = "[" + host + "]"
		}
		canonical := origin.Scheme + "://" + host
		if _, ok := seen[canonical]; !ok {
			origins = append(origins, canonical)
			seen[canonical] = struct{}{}
		}
	}
	return origins, nil
}
