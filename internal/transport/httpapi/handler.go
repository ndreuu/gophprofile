package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"gophprofile/internal/domain"
	"gophprofile/web"
)

type AvatarService interface {
	Upload(context.Context, string, string, []byte) (domain.Avatar, error)
	Get(context.Context, string) (domain.Avatar, error)
	Current(context.Context, string) (domain.Avatar, error)
	List(context.Context, string) ([]domain.Avatar, error)
	Delete(context.Context, string, string) error
	DeleteCurrent(context.Context, string, string) error
	Content(context.Context, string, string) (domain.Avatar, io.ReadCloser, string, error)
	ContentFormatted(context.Context, string, string, string) (domain.Avatar, io.ReadCloser, string, error)
}

type handler struct {
	service AvatarService
	checks  map[string]func(context.Context) error
	logger  *slog.Logger
	options Options
	limiter *windowLimiter
}

func New(service AvatarService, checks map[string]func(context.Context) error, logger *slog.Logger, options ...Options) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	settings := defaultOptions(options)
	h := &handler{service: service, checks: checks, logger: logger, options: settings,
		limiter: newWindowLimiter(settings.RateLimitRequests, settings.RateLimitWindow)}
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(h.security)
	r.Use(h.recover)
	r.Use(h.cors)
	r.Use(h.rateLimit)
	r.Get("/health", h.health)
	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/avatars", h.upload)
		r.Get("/avatars/{avatarID}", h.content)
		r.Get("/avatars/{avatarID}/metadata", h.metadata)
		r.Delete("/avatars/{avatarID}", h.delete)
		r.Get("/users/{userID}/avatar", h.currentContent)
		r.Get("/users/{userID}/avatars", h.list)
		r.Delete("/users/{userID}/avatar", h.deleteCurrent)
	})
	r.Post("/web/upload", h.upload)
	r.Get("/", web.Index)
	r.Get("/web/upload", web.Index)
	r.Get("/web/gallery/{userID}", web.Index)
	r.Handle("/assets/*", http.StripPrefix("/assets/", web.Assets()))
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Not found"})
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
	})
	return r
}

func (h *handler) security(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' blob:; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'")
		next.ServeHTTP(w, r)
	})
}

func (h *handler) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				h.logger.ErrorContext(r.Context(), "HTTP handler panic", "request_id", middleware.GetReqID(r.Context()), "panic", err)
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (h *handler) upload(w http.ResponseWriter, r *http.Request) {
	userID, valid := h.requestUserID(w, r)
	if !valid {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, domain.MaxFileSize+(1<<20))
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.fail(w, r, domain.ErrTooLarge)
		} else {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Expected multipart/form-data with a file field"})
		}
		return
	}
	defer func() {
		if err := r.MultipartForm.RemoveAll(); err != nil {
			h.logger.WarnContext(r.Context(), "remove temporary upload", "error", err)
		}
	}()
	files := r.MultipartForm.File["file"]
	if len(files) != 1 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Exactly one file field is required"})
		return
	}
	if files[0].Size > domain.MaxFileSize {
		h.fail(w, r, domain.ErrTooLarge)
		return
	}
	file, err := files[0].Open()
	if err != nil {
		h.fail(w, r, err)
		return
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, domain.MaxFileSize+1))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if int64(len(data)) > domain.MaxFileSize {
		h.fail(w, r, domain.ErrTooLarge)
		return
	}
	avatar, err := h.service.Upload(r.Context(), userID, files[0].Filename, data)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/avatars/"+avatar.ID+"/metadata")
	writeJSON(w, http.StatusCreated, avatar)
}

func (h *handler) metadata(w http.ResponseWriter, r *http.Request) {
	avatar, err := h.service.Get(r.Context(), chi.URLParam(r, "avatarID"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, avatar)
}

func (h *handler) list(w http.ResponseWriter, r *http.Request) {
	avatars, err := h.service.List(r.Context(), chi.URLParam(r, "userID"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	if avatars == nil {
		avatars = []domain.Avatar{}
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, avatars)
}

func (h *handler) content(w http.ResponseWriter, r *http.Request) {
	h.serveContent(w, r, chi.URLParam(r, "avatarID"))
}

func (h *handler) currentContent(w http.ResponseWriter, r *http.Request) {
	avatar, err := h.service.Current(r.Context(), chi.URLParam(r, "userID"))
	if err != nil {
		h.fail(w, r, err)
		return
	}
	h.serveContent(w, r, avatar.ID)
}

func (h *handler) serveContent(w http.ResponseWriter, r *http.Request, id string) {
	size := r.URL.Query().Get("size")
	if size == "" {
		size = "original"
	}
	if size != "original" && size != "100x100" && size != "300x300" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid size", "details": "Supported sizes: original, 100x100, 300x300"})
		return
	}
	format := r.URL.Query().Get("format")
	if format != "" && format != "jpeg" && format != "png" && format != "webp" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid format", "details": "Supported formats: jpeg, png, webp"})
		return
	}
	avatar, reader, mime, err := h.service.ContentFormatted(r.Context(), id, size, format)
	if err != nil {
		h.fail(w, r, err)
		return
	}
	defer func() { _ = reader.Close() }()
	etag := fmt.Sprintf("\"%s-%s-%s-%d\"", avatar.ID, size, format, avatar.UpdatedAt.UnixNano())
	w.Header().Set("ETag", etag)
	w.Header().Set("Cache-Control", "public, max-age=60")
	if !avatar.UpdatedAt.IsZero() {
		w.Header().Set("Last-Modified", avatar.UpdatedAt.UTC().Format(http.TimeFormat))
	}
	for _, candidate := range strings.Split(r.Header.Get("If-None-Match"), ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || strings.TrimPrefix(candidate, "W/") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	buffered := bufio.NewReader(reader)
	if _, err := buffered.Peek(1); err != nil {
		w.Header().Del("ETag")
		w.Header().Del("Last-Modified")
		w.Header().Set("Cache-Control", "no-store")
		h.fail(w, r, fmt.Errorf("read image body: %w", err))
		return
	}
	w.Header().Set("Content-Type", mime)
	w.Header().Set("Content-Disposition", "inline")
	if (size == "original" || format != "") && avatar.Size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(avatar.Size, 10))
	}
	if _, err := io.Copy(w, buffered); err != nil {
		h.logger.WarnContext(r.Context(), "stream avatar", "avatar_id", id, "error", err)
	}
}

func (h *handler) delete(w http.ResponseWriter, r *http.Request) {
	userID, valid := h.requestUserID(w, r)
	if !valid {
		return
	}
	if err := h.service.Delete(r.Context(), chi.URLParam(r, "avatarID"), userID); err != nil {
		h.fail(w, r, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) deleteCurrent(w http.ResponseWriter, r *http.Request) {
	userID, valid := h.requestUserID(w, r)
	if !valid {
		return
	}
	if err := h.service.DeleteCurrent(r.Context(), chi.URLParam(r, "userID"), userID); err != nil {
		h.fail(w, r, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) requestUserID(w http.ResponseWriter, r *http.Request) (string, bool) {
	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "X-User-ID header is required"})
		return "", false
	}
	if len(r.Header.Values("X-User-ID")) != 1 {
		h.fail(w, r, domain.ErrInvalid)
		return "", false
	}
	if err := domain.ValidateUserID(userID); err != nil {
		h.fail(w, r, err)
		return "", false
	}
	return userID, true
}

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	status, code := "ok", http.StatusOK
	components := make(map[string]string, len(h.checks))
	names := make([]string, 0, len(h.checks))
	for name := range h.checks {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		components[name] = "ok"
		if err := h.checks[name](ctx); err != nil {
			components[name] = "unavailable"
			status, code = "unavailable", http.StatusServiceUnavailable
			h.logger.WarnContext(ctx, "healthcheck failed", "component", name, "error", err)
		}
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, code, map[string]any{"status": status, "components": components})
}

func (h *handler) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Avatar not found"})
	case errors.Is(err, domain.ErrForbidden):
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "Forbidden", "details": "You can only delete your own avatars"})
	case errors.Is(err, domain.ErrTooLarge):
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "File too large", "max_size": domain.MaxFileSize})
	case errors.Is(err, domain.ErrUnsupported):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid file format", "details": "Supported formats: jpeg, png, webp"})
	case errors.Is(err, domain.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	case errors.Is(err, domain.ErrProcessing):
		w.Header().Set("Retry-After", "2")
		writeJSON(w, http.StatusConflict, map[string]string{"error": "Thumbnail is still processing"})
	default:
		h.logger.ErrorContext(r.Context(), "request failed", "request_id", middleware.GetReqID(r.Context()), "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Internal server error"})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
