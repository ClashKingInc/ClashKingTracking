package scripts

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	"github.com/google/uuid"
)

const maxMediaUploadSize = 25 * 1024 * 1024

var mediaAllowedExtensions = map[string]bool{
	"png": true, "jpg": true, "jpeg": true, "gif": true, "webp": true, "svg": true,
}

type mobilePushHandlers struct {
	app   *platform.App
	store mobilePushStore
}

// newMobilePushHTTPServer builds the admin JSON API for Posts. A separate
// server/port from the shared stats mux in internal/platform/app.go, so
// this domain is entirely self-contained and doesn't touch platform
// bootstrapping code every other domain relies on.
func newMobilePushHTTPServer(app *platform.App, store mobilePushStore) *http.Server {
	h := &mobilePushHandlers{app: app, store: store}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /posts/public", h.listPublicPosts)
	mux.HandleFunc("GET /posts", h.requireAdmin(h.listPosts))
	mux.HandleFunc("POST /posts", h.requireAdmin(h.createPost))
	mux.HandleFunc("GET /posts/{id}", h.requireAdmin(h.getPost))
	mux.HandleFunc("PATCH /posts/{id}", h.requireAdmin(h.updatePost))
	mux.HandleFunc("DELETE /posts/{id}", h.requireAdmin(h.archivePost))
	mux.HandleFunc("POST /posts/{id}/publish", h.requireAdmin(h.publishPost))
	mux.HandleFunc("GET /posts/{id}/audience", h.requireAdmin(h.audience))
	mux.HandleFunc("POST /media/upload", h.requireAdmin(h.uploadMedia))

	return &http.Server{
		Addr:    app.Config.MobilePushHTTPAddr,
		Handler: withCORS(mux),
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireAdmin checks Authorization: Bearer <MobilePushAdminToken> when that
// token is configured. Unset means open — matches the optional-token
// pattern already used by the Python admin-api for this same class of
// local/admin tool.
func (h *mobilePushHandlers) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token := h.app.Config.MobilePushAdminToken
		if token != "" {
			header := r.Header.Get("Authorization")
			if header != "Bearer "+token {
				writeError(w, http.StatusUnauthorized, "invalid or missing admin token")
				return
			}
		}
		next(w, r)
	}
}

func (h *mobilePushHandlers) listPosts(w http.ResponseWriter, r *http.Request) {
	posts, err := h.store.ListPosts(r.Context(), r.URL.Query().Get("status"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, posts)
}

func (h *mobilePushHandlers) listPublicPosts(w http.ResponseWriter, r *http.Request) {
	posts, err := h.store.PublicPosts(r.Context(), time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, posts)
}

func (h *mobilePushHandlers) getPost(w http.ResponseWriter, r *http.Request) {
	post, found, err := h.store.GetPost(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	writeJSON(w, http.StatusOK, post)
}

func (h *mobilePushHandlers) createPost(w http.ResponseWriter, r *http.Request) {
	var input models.AdminPostInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if input.Title == nil || strings.TrimSpace(*input.Title) == "" {
		writeError(w, http.StatusBadRequest, "title is required")
		return
	}
	post, err := h.store.CreatePost(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, post)
}

func (h *mobilePushHandlers) updatePost(w http.ResponseWriter, r *http.Request) {
	var input models.AdminPostInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	post, found, err := h.store.UpdatePost(r.Context(), r.PathValue("id"), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	writeJSON(w, http.StatusOK, post)
}

func (h *mobilePushHandlers) archivePost(w http.ResponseWriter, r *http.Request) {
	found, err := h.store.ArchivePost(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// publishPost is "Publish now" — the same publish-and-maybe-push path the
// scheduler's polling loop uses for scheduled posts (see publishAndNotify
// in mobile_push.go), just triggered by an admin action instead of a due
// starts_at.
func (h *mobilePushHandlers) publishPost(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	post, found, err := h.store.GetPost(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	if post.Status != "draft" && post.Status != "scheduled" {
		writeError(w, http.StatusConflict, fmt.Sprintf("post is %q, cannot publish", post.Status))
		return
	}
	result, err := publishAndNotify(r.Context(), h.app, h.store, post)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *mobilePushHandlers) audience(w http.ResponseWriter, r *http.Request) {
	post, found, err := h.store.GetPost(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	count, err := h.store.AudienceCount(r.Context(), post.Platforms)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"estimated_recipients": count})
}

func (h *mobilePushHandlers) uploadMedia(w http.ResponseWriter, r *http.Request) {
	if h.app.Config.BunnyAccessKey == "" {
		writeError(w, http.StatusServiceUnavailable, "image upload is not configured (BUNNY_ACCESS_KEY unset)")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxMediaUploadSize)
	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "a 'file' field is required")
		return
	}
	defer file.Close()

	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(header.Filename), "."))
	if !mediaAllowedExtensions[ext] {
		writeError(w, http.StatusUnsupportedMediaType, fmt.Sprintf("unsupported file type: .%s", ext))
		return
	}
	data, err := io.ReadAll(file)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "file too large (max 25 MB)")
			return
		}
		writeError(w, http.StatusBadRequest, "failed to read uploaded file")
		return
	}

	url, err := bunnyUpload(h.app.Config.BunnyAccessKey, "admin-posts/"+uuid.New().String(), ext, data)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to upload file to CDN")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"url": url})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"detail": message})
}
