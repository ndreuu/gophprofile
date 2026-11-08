package httpapi

import (
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Options struct {
	AllowedOrigins    []string
	RateLimitRequests int
	RateLimitWindow   time.Duration
}

func defaultOptions(options []Options) Options {
	var result Options
	if len(options) > 0 {
		result = options[0]
		result.AllowedOrigins = append([]string(nil), result.AllowedOrigins...)
	}
	if result.RateLimitRequests <= 0 {
		result.RateLimitRequests = 120
	}
	if result.RateLimitWindow <= 0 {
		result.RateLimitWindow = time.Minute
	}
	return result
}

func apiScope(r *http.Request) bool {
	return r.URL.Path == "/api" || strings.HasPrefix(r.URL.Path, "/api/") ||
		(r.URL.Path == "/web/upload" && (r.Method == http.MethodPost || r.Method == http.MethodOptions))
}

func (h *handler) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !apiScope(r) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Add("Vary", "Origin")
		origin := r.Header.Get("Origin")
		if origin == "" {
			next.ServeHTTP(w, r)
			return
		}
		allowed := len(r.Header.Values("Origin")) == 1 && h.originAllowed(r, origin)
		preflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
		if !allowed {
			if preflight || (r.Method != http.MethodGet && r.Method != http.MethodHead) {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "Origin not allowed"})
				return
			}
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Expose-Headers", "ETag, Location, Retry-After")
		if !preflight {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Add("Vary", "Access-Control-Request-Method")
		w.Header().Add("Vary", "Access-Control-Request-Headers")
		method := r.Header.Get("Access-Control-Request-Method")
		if method != http.MethodGet && method != http.MethodPost && method != http.MethodDelete {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "CORS method not allowed"})
			return
		}
		for _, value := range strings.Split(r.Header.Get("Access-Control-Request-Headers"), ",") {
			value = strings.ToLower(strings.TrimSpace(value))
			if value != "" && value != "content-type" && value != "x-user-id" {
				writeJSON(w, http.StatusForbidden, map[string]string{"error": "CORS header not allowed"})
				return
			}
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-User-ID")
		w.Header().Set("Access-Control-Max-Age", "600")
		w.WriteHeader(http.StatusNoContent)
	})
}

func (h *handler) originAllowed(r *http.Request, origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.User != nil || u.Host == "" || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if u.Scheme == scheme && strings.EqualFold(u.Host, r.Host) {
		return true
	}
	for _, configured := range h.options.AllowedOrigins {
		if configured != "*" && origin == configured {
			return true
		}
	}
	return false
}

type windowEntry struct {
	count   int
	expires time.Time
}

type windowLimiter struct {
	mu         sync.Mutex
	entries    map[string]windowEntry
	limit      int
	window     time.Duration
	maxEntries int
	nextSweep  time.Time
}

func newWindowLimiter(limit int, window time.Duration) *windowLimiter {
	return &windowLimiter{entries: make(map[string]windowEntry), limit: limit, window: window, maxEntries: 10000}
}

func (l *windowLimiter) allow(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !now.Before(l.nextSweep) {
		for key, entry := range l.entries {
			if !now.Before(entry.expires) {
				delete(l.entries, key)
			}
		}
		l.nextSweep = now.Add(min(l.window, time.Minute))
	}
	entry, exists := l.entries[key]
	if exists && !now.Before(entry.expires) {
		delete(l.entries, key)
		exists = false
	}
	if !exists {
		if len(l.entries) >= l.maxEntries {
			return false, l.window
		}
		entry = windowEntry{expires: now.Add(l.window)}
	}
	if entry.count >= l.limit {
		return false, entry.expires.Sub(now)
	}
	entry.count++
	l.entries[key] = entry
	return true, 0
}

func (h *handler) rateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !apiScope(r) || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		key := "unknown"
		if ip := net.ParseIP(host); ip != nil {
			key = ip.String()
		}
		if allowed, retry := h.limiter.allow(key, time.Now()); !allowed {
			seconds := max(1, int(math.Ceil(retry.Seconds())))
			w.Header().Set("Retry-After", strconv.Itoa(seconds))
			w.Header().Set("Cache-Control", "no-store")
			writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "Too many requests"})
			return
		}
		next.ServeHTTP(w, r)
	})
}
