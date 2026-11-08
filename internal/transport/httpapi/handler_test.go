package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gophprofile/internal/domain"
)

type fakeService struct {
	upload        func(context.Context, string, string, []byte) (domain.Avatar, error)
	get           func(context.Context, string) (domain.Avatar, error)
	current       func(context.Context, string) (domain.Avatar, error)
	list          func(context.Context, string) ([]domain.Avatar, error)
	delete        func(context.Context, string, string) error
	deleteCurrent func(context.Context, string, string) error
	content       func(context.Context, string, string) (domain.Avatar, io.ReadCloser, string, error)
	formatted     func(context.Context, string, string, string) (domain.Avatar, io.ReadCloser, string, error)
}

func (f *fakeService) Upload(c context.Context, u, n string, b []byte) (domain.Avatar, error) {
	return f.upload(c, u, n, b)
}
func (f *fakeService) Get(c context.Context, id string) (domain.Avatar, error) { return f.get(c, id) }
func (f *fakeService) Current(c context.Context, u string) (domain.Avatar, error) {
	return f.current(c, u)
}
func (f *fakeService) List(c context.Context, u string) ([]domain.Avatar, error) { return f.list(c, u) }
func (f *fakeService) Delete(c context.Context, id, u string) error              { return f.delete(c, id, u) }
func (f *fakeService) DeleteCurrent(c context.Context, target, u string) error {
	return f.deleteCurrent(c, target, u)
}
func (f *fakeService) Content(c context.Context, id, s string) (domain.Avatar, io.ReadCloser, string, error) {
	return f.content(c, id, s)
}

func (f *fakeService) ContentFormatted(c context.Context, id, size, format string) (domain.Avatar, io.ReadCloser, string, error) {
	if f.formatted != nil {
		return f.formatted(c, id, size, format)
	}
	return f.Content(c, id, size)
}

func testHandler(f *fakeService) http.Handler {
	return New(f, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func request(handler http.Handler, method, path, user string, body io.Reader) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, body)
	if user != "" {
		r.Header.Set("X-User-ID", user)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func multipartRequest(t *testing.T, handler http.Handler, path, user, field string, data []byte) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	if field != "" {
		part, err := w.CreateFormFile(field, "photo.png")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, path, &body)
	r.Header.Set("Content-Type", w.FormDataContentType())
	if user != "" {
		r.Header.Set("X-User-ID", user)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, r)
	return response
}

func requireStatus(t *testing.T, w *httptest.ResponseRecorder, expected int) {
	t.Helper()
	if w.Code != expected {
		t.Fatalf("status = %d, want %d; body: %s", w.Code, expected, w.Body.String())
	}
}

func TestUploadRoutesAndPayload(t *testing.T) {
	for _, route := range []string{"/api/v1/avatars", "/web/upload"} {
		t.Run(route, func(t *testing.T) {
			f := &fakeService{upload: func(_ context.Context, user, name string, data []byte) (domain.Avatar, error) {
				if user != "andrew" || name != "photo.png" || string(data) != "image-data" {
					t.Fatalf("wrong upload arguments: %q %q %q", user, name, data)
				}
				return domain.Avatar{ID: "avatar-id", UserID: user, Status: domain.StatusProcessing}, nil
			}}
			w := multipartRequest(t, testHandler(f), route, "andrew", "file", []byte("image-data"))
			requireStatus(t, w, http.StatusCreated)
			if w.Header().Get("Location") != "/api/v1/avatars/avatar-id/metadata" {
				t.Fatal("missing metadata location")
			}
			var avatar domain.Avatar
			if err := json.Unmarshal(w.Body.Bytes(), &avatar); err != nil {
				t.Fatal(err)
			}
			if avatar.ID != "avatar-id" || avatar.Status != domain.StatusProcessing {
				t.Fatalf("unexpected avatar: %+v", avatar)
			}
		})
	}
}

func TestUploadRejectsInvalidBodiesBeforeService(t *testing.T) {
	h := testHandler(&fakeService{upload: func(context.Context, string, string, []byte) (domain.Avatar, error) {
		t.Fatal("invalid request reached service")
		return domain.Avatar{}, nil
	}})
	t.Run("missing identity", func(t *testing.T) {
		requireStatus(t, multipartRequest(t, h, "/api/v1/avatars", "", "file", []byte("data")), http.StatusBadRequest)
	})
	t.Run("missing file", func(t *testing.T) {
		requireStatus(t, multipartRequest(t, h, "/api/v1/avatars", "user", "", nil), http.StatusBadRequest)
	})
	t.Run("wrong field", func(t *testing.T) {
		requireStatus(t, multipartRequest(t, h, "/api/v1/avatars", "user", "image", []byte("data")), http.StatusBadRequest)
	})
	t.Run("wrong content type", func(t *testing.T) {
		requireStatus(t, request(h, http.MethodPost, "/api/v1/avatars", "user", strings.NewReader("invalid")), http.StatusBadRequest)
	})
	t.Run("file over ten MiB", func(t *testing.T) {
		w := multipartRequest(t, h, "/api/v1/avatars", "user", "file", make([]byte, domain.MaxFileSize+1))
		requireStatus(t, w, http.StatusRequestEntityTooLarge)
		if !strings.Contains(w.Body.String(), "10485760") {
			t.Fatal("missing documented max_size")
		}
	})
	t.Run("request body limit", func(t *testing.T) {
		requireStatus(t, multipartRequest(t, h, "/api/v1/avatars", "user", "file", make([]byte, domain.MaxFileSize+(2<<20))), http.StatusRequestEntityTooLarge)
	})
}

func TestErrorMappingDoesNotLeakInternalErrors(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		code     int
		expected string
	}{
		{"not found", domain.ErrNotFound, 404, "Avatar not found"},
		{"forbidden", domain.ErrForbidden, 403, "Forbidden"},
		{"too large", domain.ErrTooLarge, 413, "File too large"},
		{"unsupported", domain.ErrUnsupported, 400, "Invalid file format"},
		{"invalid", domain.ErrInvalid, 400, "Invalid request"},
		{"processing", domain.ErrProcessing, 409, "Thumbnail is still processing"},
		{"wrapped", fmt.Errorf("repository: %w", domain.ErrNotFound), 404, "Avatar not found"},
		{"internal", errors.New("password=super-secret at postgres.internal"), 500, "Internal server error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeService{get: func(context.Context, string) (domain.Avatar, error) { return domain.Avatar{}, tc.err }}
			w := request(testHandler(f), http.MethodGet, "/api/v1/avatars/id/metadata", "", nil)
			requireStatus(t, w, tc.code)
			if !strings.Contains(w.Body.String(), tc.expected) {
				t.Fatalf("unexpected error: %s", w.Body)
			}
			if strings.Contains(w.Body.String(), "super-secret") || strings.Contains(w.Body.String(), "repository") {
				t.Fatal("internal error leaked")
			}
			if !strings.Contains(w.Header().Get("Content-Type"), "application/json") {
				t.Fatal("errors must be JSON")
			}
			if tc.err == domain.ErrProcessing && w.Header().Get("Retry-After") != "2" {
				t.Fatal("missing retry-after")
			}
		})
	}
}

func TestMetadataAndListsHideStorageKeys(t *testing.T) {
	avatar := domain.Avatar{ID: "a", S3Key: "secret-key", Thumbnails: []domain.Thumbnail{{Size: "100x100", S3Key: "secret-thumb", URL: "/api/v1/avatars/a?size=100x100"}}}
	f := &fakeService{
		get: func(_ context.Context, id string) (domain.Avatar, error) {
			if id != "a" {
				t.Fatal(id)
			}
			return avatar, nil
		},
		list: func(_ context.Context, user string) ([]domain.Avatar, error) {
			if user != "andrew" {
				t.Fatal(user)
			}
			return []domain.Avatar{avatar}, nil
		},
	}
	for _, path := range []string{"/api/v1/avatars/a/metadata", "/api/v1/users/andrew/avatars"} {
		w := request(testHandler(f), http.MethodGet, path, "", nil)
		requireStatus(t, w, 200)
		if strings.Contains(w.Body.String(), "secret") {
			t.Fatal("storage key leaked")
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("mutable metadata was cacheable")
		}
	}
	f.list = func(context.Context, string) ([]domain.Avatar, error) { return nil, nil }
	w := request(testHandler(f), http.MethodGet, "/api/v1/users/andrew/avatars", "", nil)
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatalf("empty list must be an array, got %s", w.Body)
	}
	f.list = func(context.Context, string) ([]domain.Avatar, error) { return nil, domain.ErrInvalid }
	requireStatus(t, request(testHandler(f), http.MethodGet, "/api/v1/users/andrew/avatars", "", nil), 400)
}

type trackingReader struct {
	io.Reader
	closed bool
}

func (r *trackingReader) Close() error { r.closed = true; return nil }

func TestContentStreamsAndClosesObject(t *testing.T) {
	for _, size := range []string{"", "original", "100x100", "300x300"} {
		t.Run(size, func(t *testing.T) {
			reader := &trackingReader{Reader: strings.NewReader("image-data")}
			f := &fakeService{content: func(_ context.Context, id, actual string) (domain.Avatar, io.ReadCloser, string, error) {
				expected := size
				if expected == "" {
					expected = "original"
				}
				if id != "a" || actual != expected {
					t.Fatalf("wrong content arguments: %q %q", id, actual)
				}
				return domain.Avatar{ID: id, Size: 10, UpdatedAt: time.Unix(1, 0)}, reader, "image/png", nil
			}}
			w := request(testHandler(f), http.MethodGet, "/api/v1/avatars/a?size="+size, "", nil)
			requireStatus(t, w, 200)
			if w.Body.String() != "image-data" || w.Header().Get("Content-Type") != "image/png" {
				t.Fatal("bad image response")
			}
			if !reader.closed {
				t.Fatal("storage reader not closed")
			}
			if w.Header().Get("ETag") == "" || w.Header().Get("Last-Modified") == "" {
				t.Fatal("missing cache validators")
			}
		})
	}
}

func TestContentCacheValidation(t *testing.T) {
	var readers []*trackingReader
	f := &fakeService{content: func(context.Context, string, string) (domain.Avatar, io.ReadCloser, string, error) {
		r := &trackingReader{Reader: strings.NewReader("data")}
		readers = append(readers, r)
		return domain.Avatar{ID: "a", UpdatedAt: time.Unix(1, 0)}, r, "image/png", nil
	}}
	h := testHandler(f)
	w := request(h, http.MethodGet, "/api/v1/avatars/a", "", nil)
	etag := w.Header().Get("ETag")
	for _, header := range []string{etag, "W/" + etag, `"other", ` + etag, "*"} {
		r := httptest.NewRequest(http.MethodGet, "/api/v1/avatars/a", nil)
		r.Header.Set("If-None-Match", header)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		requireStatus(t, w, 304)
		if w.Body.Len() > 0 || !readers[len(readers)-1].closed {
			t.Fatal("304 must close reader and have no body")
		}
	}
}

func TestCurrentAvatarAndContentErrors(t *testing.T) {
	f := &fakeService{
		current: func(_ context.Context, user string) (domain.Avatar, error) {
			if user != "andrew" {
				t.Fatal(user)
			}
			return domain.Avatar{ID: "current"}, nil
		},
		content: func(_ context.Context, id, _ string) (domain.Avatar, io.ReadCloser, string, error) {
			if id != "current" {
				t.Fatal(id)
			}
			return domain.Avatar{}, nil, "", domain.ErrProcessing
		},
	}
	requireStatus(t, request(testHandler(f), http.MethodGet, "/api/v1/users/andrew/avatar?size=100x100", "", nil), 409)
	f.current = func(context.Context, string) (domain.Avatar, error) { return domain.Avatar{}, domain.ErrNotFound }
	requireStatus(t, request(testHandler(f), http.MethodGet, "/api/v1/users/andrew/avatar", "", nil), 404)
	requireStatus(t, request(testHandler(&fakeService{}), http.MethodGet, "/api/v1/avatars/id?size=999", "", nil), 400)
}

func TestDeleteUsesRequesterIdentity(t *testing.T) {
	f := &fakeService{
		delete: func(_ context.Context, id, user string) error {
			if id != "a" || user != "owner" {
				return domain.ErrForbidden
			}
			return nil
		},
		deleteCurrent: func(_ context.Context, target, user string) error {
			if target != user {
				return domain.ErrForbidden
			}
			return nil
		},
	}
	for _, route := range []string{"/api/v1/avatars/a", "/api/v1/users/owner/avatar"} {
		h := testHandler(f)
		requireStatus(t, request(h, http.MethodDelete, route, "", nil), 400)
		requireStatus(t, request(h, http.MethodDelete, route, "stranger", nil), 403)
		w := request(h, http.MethodDelete, route, "owner", nil)
		requireStatus(t, w, 204)
		if w.Body.Len() != 0 {
			t.Fatal("204 has a response body")
		}
	}
}

func TestHealthAllComponentsAndFailureStatus(t *testing.T) {
	for _, healthy := range []bool{true, false} {
		t.Run(fmt.Sprint(healthy), func(t *testing.T) {
			called := make(map[string]bool)
			checks := map[string]func(context.Context) error{}
			for _, name := range []string{"database", "s3", "kafka"} {
				checks[name] = func(ctx context.Context) error {
					called[name] = true
					if _, ok := ctx.Deadline(); !ok {
						t.Fatal("healthcheck needs timeout")
					}
					if !healthy && name == "s3" {
						return errors.New("secret credentials")
					}
					return nil
				}
			}
			h := New(&fakeService{}, checks, slog.New(slog.NewTextHandler(io.Discard, nil)))
			w := request(h, http.MethodGet, "/health", "", nil)
			expected := 200
			if !healthy {
				expected = 503
			}
			requireStatus(t, w, expected)
			if len(called) != 3 {
				t.Fatal("not all dependencies checked")
			}
			if strings.Contains(w.Body.String(), "credentials") {
				t.Fatal("health leaked sensitive error")
			}
		})
	}
}

func TestEmbeddedWebRoutesAndSecurityHeaders(t *testing.T) {
	h := testHandler(&fakeService{})
	for _, route := range []string{"/", "/web/upload", "/web/gallery/andrew", "/assets/style.css", "/assets/app.js"} {
		w := request(h, http.MethodGet, route, "", nil)
		requireStatus(t, w, 200)
		if w.Body.Len() == 0 || w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Content-Security-Policy") == "" {
			t.Fatal("missing asset or security headers")
		}
	}
	requireStatus(t, request(h, http.MethodGet, "/missing", "", nil), 404)
	requireStatus(t, request(h, http.MethodPut, "/api/v1/avatars/a", "", nil), 405)
	requireStatus(t, request(h, http.MethodGet, "/assets/nope", "", nil), 404)
}

func TestPanicReturnsGenericJSONError(t *testing.T) {
	f := &fakeService{get: func(context.Context, string) (domain.Avatar, error) { panic("secret") }}
	w := request(New(f, nil, nil), http.MethodGet, "/api/v1/avatars/a/metadata", "", nil)
	requireStatus(t, w, 500)
	if strings.Contains(w.Body.String(), "secret") {
		t.Fatal("panic leaked internals")
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "application/json") {
		t.Fatal("panic must be JSON")
	}
}
