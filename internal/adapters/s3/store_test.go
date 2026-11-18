package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"gophprofile/internal/domain"
)

type fakeTransport func(*http.Request) (*http.Response, error)

func (f fakeTransport) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func testStore(t *testing.T, handler http.HandlerFunc) *Store {
	t.Helper()
	client, err := minio.New("s3.example.test", &minio.Options{
		Creds:  credentials.NewStaticV4("access", "secret", ""),
		Region: "us-east-1", BucketLookup: minio.BucketLookupPath,
		Transport: fakeTransport(func(request *http.Request) (*http.Response, error) {
			recorder := httptest.NewRecorder()
			handler(recorder, request)
			response := recorder.Result()
			response.Request = request
			return response, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	return &Store{client: client, bucket: "avatars"}
}

func TestEnsureBucketRemovesPublicPolicy(t *testing.T) {
	var methods []string
	store := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path+"?"+r.URL.RawQuery)
		if r.Method == http.MethodDelete && r.URL.Query().Has("policy") {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusBadRequest)
	})
	if err := store.EnsureBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 2 {
		t.Fatalf("want existence check and private policy, got %v", methods)
	}
}

func TestGetSurfacesMissingObjectBeforeReturningReader(t *testing.T) {
	store := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("expected HEAD before returning a reader; got %s", r.Method)
		}
		w.Header().Set("x-amz-error-code", "NoSuchKey")
		w.WriteHeader(http.StatusNotFound)
	})
	reader, err := store.Get(context.Background(), "missing.png")
	if reader != nil || !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("reader=%v err=%v", reader, err)
	}
}

func TestPutGetAndIdempotentDelete(t *testing.T) {
	content := []byte("avatar bytes")
	var stored []byte
	var contentType string
	store := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			stored, _ = io.ReadAll(r.Body)
			contentType = r.Header.Get("Content-Type")
			w.Header().Set("ETag", `"5eb63bbbe01eeed093cb22bb8f5acdc3"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			w.Header().Set("Content-Length", fmt.Sprint(len(content)))
			w.Header().Set("ETag", `"5eb63bbbe01eeed093cb22bb8f5acdc3"`)
			w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
			w.Header().Set("ETag", `"5eb63bbbe01eeed093cb22bb8f5acdc3"`)
			w.Header().Set("Content-Length", fmt.Sprint(len(content)))
			_, _ = w.Write(content)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	if err := store.Put(context.Background(), "test.png", bytes.NewReader(content), int64(len(content)), "image/png"); err != nil {
		t.Fatal(err)
	}

	if !bytes.Contains(stored, content) || contentType != "image/png" {
		t.Fatalf("stored=%q contentType=%s", stored, contentType)
	}
	reader, err := store.Get(context.Background(), "test.png")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got, err := io.ReadAll(reader)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("got %q err=%v", got, err)
	}
	for range 2 {
		if err := store.Delete(context.Background(), "test.png"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPingMissingBucketAndAuthorizationFailure(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			store := testStore(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) })
			if err := store.Ping(context.Background()); err == nil {
				t.Fatal("expected health check failure")
			}
		})
	}
}

func TestPutRejectsInvalidParameters(t *testing.T) {
	store := testStore(t, func(w http.ResponseWriter, _ *http.Request) { t.Error("invalid put must not reach storage") })
	if err := store.Put(context.Background(), "", bytes.NewReader(nil), 0, "image/png"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), "a", nil, 0, "image/png"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), "a", bytes.NewReader(nil), -1, "image/png"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestNewValidatesEndpointAndBucket(t *testing.T) {
	if _, err := New(context.Background(), "http://bad/path", "a", "b", "avatars", false); err == nil {
		t.Fatal("want endpoint error")
	}
	if _, err := New(context.Background(), "localhost:9000", "a", "b", "", false); err == nil {
		t.Fatal("want bucket error")
	}
	if _, err := New(context.Background(), "localhost:9000", "a", "b", "avatars", false); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureBucketCreatesMissingPrivateBucket(t *testing.T) {
	var actions []string
	store := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		actions = append(actions, r.Method)
		switch r.Method {
		case http.MethodHead:
			w.WriteHeader(http.StatusNotFound)
		case http.MethodPut:
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	if err := store.EnsureBucket(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(actions) != "[HEAD PUT DELETE]" {
		t.Fatal(actions)
	}
}

func TestStorageSurfacesAuthorizationErrors(t *testing.T) {
	store := testStore(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-amz-error-code", "AccessDenied")
		w.WriteHeader(http.StatusForbidden)
	})
	if err := store.EnsureBucket(context.Background()); err == nil {
		t.Fatal("expected bucket permission error")
	}
	if err := store.Put(context.Background(), "a.png", bytes.NewReader([]byte("a")), 1, "image/png"); err == nil {
		t.Fatal("expected put permission error")
	}
	if reader, err := store.Get(context.Background(), "a.png"); err == nil || reader != nil || errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("reader=%v err=%v", reader, err)
	}
	if err := store.Delete(context.Background(), "a.png"); err == nil {
		t.Fatal("expected delete permission error")
	}
}

func TestEnsureBucketFailsWhenPrivacyCannotBeEnsured(t *testing.T) {
	store := testStore(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("x-amz-error-code", "AccessDenied")
		w.WriteHeader(http.StatusForbidden)
	})
	if err := store.EnsureBucket(context.Background()); err == nil {
		t.Fatal("expected privacy policy permission error")
	}
}
