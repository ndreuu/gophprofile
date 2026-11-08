package httpapi

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gophprofile/internal/domain"
)

func TestMutationHeadersAreValidatedBeforeService(t *testing.T) {
	for _, endpoint := range []struct{ method, path string }{
		{http.MethodPost, "/api/v1/avatars"},
		{http.MethodPost, "/web/upload"},
		{http.MethodDelete, "/api/v1/avatars/a"},
		{http.MethodDelete, "/api/v1/users/owner/avatar"},
	} {
		t.Run(endpoint.method+endpoint.path, func(t *testing.T) {
			for _, userID := range []string{" owner ", "\towner", "owner\n", strings.Repeat("u", 256)} {
				w := request(testHandler(&fakeService{}), endpoint.method, endpoint.path, userID, nil)
				requireStatus(t, w, http.StatusBadRequest)
			}
			r := httptest.NewRequest(endpoint.method, endpoint.path, nil)
			r.Header.Add("X-User-ID", "owner")
			r.Header.Add("X-User-ID", "another-owner")
			w := httptest.NewRecorder()
			testHandler(&fakeService{}).ServeHTTP(w, r)
			requireStatus(t, w, http.StatusBadRequest)
		})
	}
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, errors.New("secret storage URL unavailable") }

func TestContentReadFailureIsJSONAndNotAnEmptySuccessfulImage(t *testing.T) {
	for _, body := range []io.Reader{failedReader{}, strings.NewReader("")} {
		reader := &trackingReader{Reader: body}
		f := &fakeService{content: func(context.Context, string, string) (domain.Avatar, io.ReadCloser, string, error) {
			return domain.Avatar{ID: "a", Size: 8, UpdatedAt: time.Unix(1, 0)}, reader, "image/png", nil
		}}
		w := request(testHandler(f), http.MethodGet, "/api/v1/avatars/a", "", nil)
		requireStatus(t, w, http.StatusInternalServerError)
		if !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
			t.Fatal("storage read failure must be JSON")
		}
		if strings.Contains(w.Body.String(), "secret") {
			t.Fatal("storage error details leaked")
		}
		if w.Header().Get("ETag") != "" || w.Header().Get("Content-Length") != "" || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("error has successful image headers: %v", w.Header())
		}
		if !reader.closed {
			t.Fatal("failed object reader not closed")
		}
	}
}

func TestUploadAllowsExactlyTenMiBWithMultipartOverhead(t *testing.T) {
	f := &fakeService{upload: func(_ context.Context, _, _ string, body []byte) (domain.Avatar, error) {
		if int64(len(body)) != domain.MaxFileSize {
			t.Fatalf("wrong exact-limit body length: %d", len(body))
		}
		return domain.Avatar{ID: "a", Status: domain.StatusProcessing}, nil
	}}
	w := multipartRequest(t, testHandler(f), "/api/v1/avatars", "owner", "file", make([]byte, domain.MaxFileSize))
	requireStatus(t, w, http.StatusCreated)
}

func TestFormatQueryIsValidatedAndPassedToService(t *testing.T) {
	for _, format := range []string{"jpeg", "png", "webp"} {
		t.Run(format, func(t *testing.T) {
			f := &fakeService{formatted: func(_ context.Context, id, size, got string) (domain.Avatar, io.ReadCloser, string, error) {
				if id != "a" || size != "100x100" || got != format {
					t.Fatalf("wrong conversion arguments: %q %q %q", id, size, got)
				}
				return domain.Avatar{ID: "a", Size: 9}, io.NopCloser(strings.NewReader("converted")), "image/" + format, nil
			}}
			w := request(testHandler(f), http.MethodGet, "/api/v1/avatars/a?size=100x100&format="+format, "", nil)
			requireStatus(t, w, http.StatusOK)
			if w.Body.String() != "converted" || w.Header().Get("Content-Type") != "image/"+format || w.Header().Get("Content-Length") != "9" {
				t.Fatalf("wrong converted response: %v %s", w.Header(), w.Body)
			}
			if !strings.Contains(w.Header().Get("ETag"), "-"+format+"-") {
				t.Fatal("ETag must distinguish formats")
			}
		})
	}
	requireStatus(t, request(testHandler(&fakeService{}), http.MethodGet, "/api/v1/avatars/a?format=gif", "", nil), http.StatusBadRequest)
}
