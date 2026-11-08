package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gophprofile/internal/domain"
)

func corsRequest(h http.Handler, method, path, origin, preflightMethod, preflightHeaders string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	if preflightMethod != "" {
		r.Header.Set("Access-Control-Request-Method", preflightMethod)
		r.Header.Set("Access-Control-Request-Headers", preflightHeaders)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestCORSPreflightAndCrossOriginMutations(t *testing.T) {
	f := &fakeService{list: func(context.Context, string) ([]domain.Avatar, error) { return nil, nil }}
	h := New(f, nil, nil, Options{AllowedOrigins: []string{"https://ui.example"}})
	for _, path := range []string{"/api/v1/avatars", "/web/upload"} {
		w := corsRequest(h, http.MethodOptions, path, "https://ui.example", "POST", "x-user-id, Content-Type")
		requireStatus(t, w, http.StatusNoContent)
		if w.Header().Get("Access-Control-Allow-Origin") != "https://ui.example" || w.Header().Get("Access-Control-Allow-Credentials") != "" {
			t.Fatalf("wrong CORS permission: %v", w.Header())
		}
		if !strings.Contains(strings.Join(w.Header().Values("Vary"), ","), "Origin") {
			t.Fatal("CORS response must vary by origin")
		}
		for _, tc := range []struct{ origin, method, headers string }{
			{"https://untrusted.example", "POST", "X-User-ID"},
			{"https://ui.example", "PUT", "X-User-ID"},
			{"https://ui.example", "POST", "Authorization"},
			{"null", "POST", "X-User-ID"},
		} {
			requireStatus(t, corsRequest(h, http.MethodOptions, path, tc.origin, tc.method, tc.headers), http.StatusForbidden)
		}
		requireStatus(t, corsRequest(h, http.MethodPost, path, "https://untrusted.example", "", ""), http.StatusForbidden)
	}
	w := corsRequest(h, http.MethodGet, "/api/v1/users/owner/avatars", "https://ui.example", "", "")
	requireStatus(t, w, http.StatusOK)
	if w.Header().Get("Access-Control-Allow-Origin") != "https://ui.example" {
		t.Fatal("allowed browser cannot read API response")
	}
	w = corsRequest(h, http.MethodGet, "/api/v1/users/owner/avatars", "https://untrusted.example", "", "")
	requireStatus(t, w, http.StatusOK)
	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("untrusted browser was allowed to read the response")
	}
}

func TestCORSDefaultsToSameOriginWithoutWildcard(t *testing.T) {
	h := New(&fakeService{}, nil, nil, Options{AllowedOrigins: []string{"*"}})
	requireStatus(t, corsRequest(h, http.MethodOptions, "/api/v1/avatars", "http://example.com", "POST", "X-User-ID"), http.StatusNoContent)
	requireStatus(t, corsRequest(h, http.MethodOptions, "/api/v1/avatars", "https://other.example", "POST", "X-User-ID"), http.StatusForbidden)
	requireStatus(t, corsRequest(h, http.MethodPost, "/api/v1/avatars", "http://example.com", "", ""), http.StatusBadRequest)
	for _, origin := range []string{"http://example.com/path", "http://user@example.com", "http://example.com#fragment", "http://example.com?query=1"} {
		requireStatus(t, corsRequest(h, http.MethodPost, "/api/v1/avatars", origin, "", ""), http.StatusForbidden)
	}
}

func TestRateLimitUsesRemoteIPAndDoesNotLimitHealthOrStatic(t *testing.T) {
	f := &fakeService{list: func(context.Context, string) ([]domain.Avatar, error) { return nil, nil }}
	h := New(f, nil, nil, Options{RateLimitRequests: 2, RateLimitWindow: time.Minute})
	do := func(ip, spoofed string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/users/owner/avatars", nil)
		r.RemoteAddr = ip
		r.Header.Set("X-Forwarded-For", spoofed)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	requireStatus(t, do("127.0.0.1:1000", "10.0.0.1"), http.StatusOK)
	requireStatus(t, do("127.0.0.1:2000", "10.0.0.2"), http.StatusOK)
	w := do("127.0.0.1:3000", "10.0.0.3")
	requireStatus(t, w, http.StatusTooManyRequests)
	if w.Header().Get("Retry-After") == "" || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Body.String(), "Too many requests") {
		t.Fatal("rate limit needs Retry-After and JSON")
	}
	requireStatus(t, do("127.0.0.2:3000", "10.0.0.3"), http.StatusOK)
	for i := 0; i < 4; i++ {
		for _, path := range []string{"/health", "/", "/assets/app.js", "/web/upload"} {
			r := httptest.NewRequest(http.MethodGet, path, nil)
			r.RemoteAddr = "127.0.0.1:3000"
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			requireStatus(t, w, http.StatusOK)
		}
	}
}

func TestWindowLimiterExpiresAndBoundsIdentityMemory(t *testing.T) {
	l := newWindowLimiter(2, time.Second)
	l.maxEntries = 2
	now := time.Unix(1, 0)
	for _, key := range []string{"one", "one", "two"} {
		if ok, _ := l.allow(key, now); !ok {
			t.Fatal("valid request rejected")
		}
	}
	if ok, retry := l.allow("one", now.Add(time.Millisecond)); ok || retry <= 0 {
		t.Fatal("limit was not applied")
	}
	if ok, _ := l.allow("three", now); ok || len(l.entries) > 2 {
		t.Fatal("limiter must not create unbounded identity entries")
	}
	if ok, _ := l.allow("three", now.Add(time.Second)); !ok {
		t.Fatal("expired identities were not removed")
	}
	if len(l.entries) != 1 {
		t.Fatalf("expired limiter entries remain: %d", len(l.entries))
	}
}

func TestWindowLimiterConcurrentRequests(t *testing.T) {
	l := newWindowLimiter(10, time.Minute)
	now := time.Unix(1, 0)
	var wg sync.WaitGroup
	var mu sync.Mutex
	accepted := 0
	for i := 0; i < 100; i++ {
		wg.Go(func() {
			if ok, _ := l.allow("one", now); ok {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	if accepted != 10 {
		t.Fatalf("concurrent limiter accepted %d, want 10", accepted)
	}
}
