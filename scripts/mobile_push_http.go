package scripts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
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
	mux.HandleFunc("POST /posts/{id}/push", h.requireAdmin(h.pushPost))
	mux.HandleFunc("POST /posts/{id}/duplicate", h.requireAdmin(h.duplicatePost))
	mux.HandleFunc("GET /posts/{id}/revisions", h.requireAdmin(h.listPostRevisions))
	mux.HandleFunc("POST /posts/{id}/revisions/{revision}/restore", h.requireAdmin(h.restorePostRevision))
	mux.HandleFunc("GET /posts/{id}/deliveries", h.requireAdmin(h.listPostDeliveries))
	mux.HandleFunc("GET /posts/{id}/audience", h.requireAdmin(h.audience))
	mux.HandleFunc("POST /media/upload", h.requireAdmin(h.uploadMedia))
	mux.HandleFunc("POST /stories/upload", h.requireAdmin(h.uploadStory))
	mux.HandleFunc("POST /push/test", h.requireAdmin(h.sendTestPush))
	mux.HandleFunc("GET /push/audience", h.requireAdmin(h.testPushAudience))
	mux.HandleFunc("GET /campaigns", h.requireAdmin(h.listCampaigns))

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

func (h *mobilePushHandlers) listCampaigns(w http.ResponseWriter, r *http.Request) {
	campaigns, err := h.store.ListCampaigns(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, campaigns)
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
	input, err := decodeAdminPostInput(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if input.Title == nil || strings.TrimSpace(*input.Title) == "" {
		writeError(w, http.StatusBadRequest, "title is required")
		return
	}
	if input.Summary == nil || strings.TrimSpace(*input.Summary) == "" {
		writeError(w, http.StatusBadRequest, "summary is required")
		return
	}
	if err := validatePostInput(input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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
	input, err := decodeAdminPostInput(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := validatePostInput(input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
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

func (h *mobilePushHandlers) duplicatePost(w http.ResponseWriter, r *http.Request) {
	post, found, err := h.store.GetPost(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	input := adminPostToInput(post)
	title := post.Title + " (copy)"
	input.Title = &title
	input.StartsAt, input.EndsAt = nil, nil
	input.AlsoPushOnPublish = postPtr(false)
	input.CreatedBy = postPtr(post.CreatedBy)
	copy, err := h.store.CreatePost(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, copy)
}

func (h *mobilePushHandlers) listPostRevisions(w http.ResponseWriter, r *http.Request) {
	revisions, err := h.store.ListPostRevisions(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, revisions)
}

func (h *mobilePushHandlers) restorePostRevision(w http.ResponseWriter, r *http.Request) {
	revisionNumber, err := strconv.Atoi(r.PathValue("revision"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid revision")
		return
	}
	revision, found, err := h.store.GetPostRevision(r.Context(), r.PathValue("id"), revisionNumber)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "revision not found")
		return
	}
	var snapshot models.AdminPost
	if err := json.Unmarshal(revision.Snapshot, &snapshot); err != nil {
		writeError(w, http.StatusInternalServerError, "invalid revision snapshot")
		return
	}
	restored, found, err := h.store.UpdatePost(r.Context(), r.PathValue("id"), adminPostToInput(snapshot))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	writeJSON(w, http.StatusOK, restored)
}

func (h *mobilePushHandlers) listPostDeliveries(w http.ResponseWriter, r *http.Request) {
	attempts, err := h.store.ListDeliveryAttempts(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, attempts)
}

func postPtr[T any](value T) *T { return &value }

func adminPostToInput(post models.AdminPost) models.AdminPostInput {
	input := models.AdminPostInput{
		Title: postPtr(post.Title), Summary: postPtr(post.Summary), HeroImageURL: post.HeroImageURL,
		BodyBlocks: post.BodyBlocks, PresentationType: postPtr(post.PresentationType), StoryURL: post.StoryURL,
		ShowOnHome: postPtr(post.ShowOnHome), PinnedOnHome: postPtr(post.PinnedOnHome), TargetRoute: post.TargetRoute,
		Platforms: post.Platforms, Dismissible: postPtr(post.Dismissible), Priority: postPtr(post.Priority),
		StartsAt: post.StartsAt, EndsAt: post.EndsAt, AlsoPushOnPublish: postPtr(post.AlsoPushOnPublish),
		PushTitle: post.PushTitle, PushBody: post.PushBody, CreatedBy: postPtr(post.CreatedBy),
	}
	input.ClearHeroImageURL = post.HeroImageURL == nil
	input.ClearStoryURL = post.StoryURL == nil
	input.ClearTargetRoute = post.TargetRoute == nil
	input.ClearStartsAt = post.StartsAt == nil
	input.ClearEndsAt = post.EndsAt == nil
	input.ClearPushTitle = post.PushTitle == nil
	input.ClearPushBody = post.PushBody == nil
	return input
}

func decodeAdminPostInput(reader io.Reader) (models.AdminPostInput, error) {
	raw, err := io.ReadAll(reader)
	if err != nil {
		return models.AdminPostInput{}, err
	}
	var input models.AdminPostInput
	if err := json.Unmarshal(raw, &input); err != nil {
		return models.AdminPostInput{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return models.AdminPostInput{}, err
	}
	isNull := func(name string) bool {
		value, ok := fields[name]
		return ok && string(value) == "null"
	}
	input.ClearHeroImageURL = isNull("hero_image_url")
	input.ClearStoryURL = isNull("story_url")
	input.ClearTargetRoute = isNull("target_route")
	input.ClearStartsAt = isNull("starts_at")
	input.ClearEndsAt = isNull("ends_at")
	input.ClearPushTitle = isNull("push_title")
	input.ClearPushBody = isNull("push_body")
	return input, nil
}

func validatePostInput(input models.AdminPostInput) error {
	if input.Title != nil && strings.TrimSpace(*input.Title) == "" {
		return errors.New("title cannot be empty")
	}
	if input.Summary != nil && strings.TrimSpace(*input.Summary) == "" {
		return errors.New("summary cannot be empty")
	}
	allowedPlatforms := map[string]bool{"ios": true, "android": true, "web": true}
	for _, platform := range input.Platforms {
		if !allowedPlatforms[platform] {
			return fmt.Errorf("unsupported platform: %s", platform)
		}
	}
	if input.Platforms != nil && len(input.Platforms) == 0 {
		return errors.New("at least one platform is required")
	}
	if input.StartsAt != nil && input.EndsAt != nil && !input.EndsAt.After(*input.StartsAt) {
		return errors.New("ends_at must be after starts_at")
	}
	if err := validatePostPresentation(
		valueOr(input.PresentationType, "article"),
		input.StoryURL,
		valueOr(input.ShowOnHome, true),
		valueOr(input.PinnedOnHome, false),
	); err != nil {
		return err
	}
	return nil
}

func validatePostPresentation(presentationType string, storyURL *string, showOnHome, pinnedOnHome bool) error {
	if presentationType != "article" && presentationType != "story" {
		return errors.New("presentation_type must be article or story")
	}
	if pinnedOnHome && !showOnHome {
		return errors.New("a pinned post must be shown on home")
	}
	if presentationType != "story" {
		return nil
	}
	if storyURL == nil || strings.TrimSpace(*storyURL) == "" {
		return errors.New("story_url is required for a story post")
	}
	parsed, err := url.Parse(strings.TrimSpace(*storyURL))
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("story_url must be a valid HTTPS URL")
	}
	return nil
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

// pushPost retries the notification for an already-published post without
// republishing or changing its publication date.
func (h *mobilePushHandlers) pushPost(w http.ResponseWriter, r *http.Request) {
	post, found, err := h.store.GetPost(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "post not found")
		return
	}
	if post.Status != "live" {
		writeError(w, http.StatusConflict, "only a live post can send a push notification")
		return
	}
	sent, skipped, err := deliverPostPush(r.Context(), h.app, h.store, post, "manual")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"push_sent": sent, "push_skipped": skipped})
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

type testPushInput struct {
	CampaignID  string   `json:"campaign_id"`
	Title       string   `json:"title"`
	Body        string   `json:"body"`
	TargetRoute string   `json:"target_route"`
	Platforms   []string `json:"platforms"`
}

func (h *mobilePushHandlers) testPushAudience(w http.ResponseWriter, r *http.Request) {
	platforms := strings.Split(strings.TrimSpace(r.URL.Query().Get("platforms")), ",")
	if len(platforms) == 0 || (len(platforms) == 1 && platforms[0] == "") {
		platforms = []string{"ios", "android"}
	}
	devices, err := h.store.DevicesForPlatforms(r.Context(), platforms)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	count := 0
	for _, device := range devices {
		if device.Environment == "sandbox" {
			count++
		}
	}
	writeJSON(w, http.StatusOK, map[string]int{"estimated_recipients": count})
}

// sendTestPush deliberately targets sandbox devices only. This keeps the
// dashboard's "Send test" action from ever broadcasting to production users.
func (h *mobilePushHandlers) sendTestPush(w http.ResponseWriter, r *http.Request) {
	var input testPushInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(input.Title) == "" || strings.TrimSpace(input.Body) == "" {
		writeError(w, http.StatusBadRequest, "title and body are required")
		return
	}
	if len(input.Platforms) == 0 {
		input.Platforms = []string{"ios", "android"}
	}
	devices, err := h.store.DevicesForPlatforms(r.Context(), input.Platforms)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	sandboxDevices := make([]models.PushDevice, 0, len(devices))
	for _, device := range devices {
		if device.Environment == "sandbox" {
			sandboxDevices = append(sandboxDevices, device)
		}
	}
	data := map[string]string{"type": "campaign_test", "campaign_id": input.CampaignID}
	if strings.TrimSpace(input.TargetRoute) != "" {
		data["route"] = input.TargetRoute
	}
	sent, skipped := sendPushToDevices(r.Context(), h.app, sandboxDevices, pushMessage{
		Title: input.Title, Body: input.Body, Data: data,
	})
	writeJSON(w, http.StatusOK, map[string]int{
		"push_sent": sent, "push_skipped": skipped, "eligible_devices": len(sandboxDevices),
	})
}

type storyUploadResponse struct {
	URL             string `json:"url"`
	Version         int    `json:"version"`
	StorageProvider string `json:"storage_provider"`
	Key             string `json:"key"`
	SizeBytes       int    `json:"size_bytes"`
	Checksum        string `json:"checksum"`
}

// uploadStory stores every HTML upload under a new immutable key. Replacing a
// story therefore never overwrites the currently published version and the app
// can safely cache each revision.
func (h *mobilePushHandlers) uploadStory(w http.ResponseWriter, r *http.Request) {
	if h.app.R2 == nil && h.app.Config.BunnyAccessKey == "" {
		writeError(w, http.StatusServiceUnavailable, "story upload is not configured (R2 or Bunny CDN required)")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxMediaUploadSize)
	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "a 'file' field is required")
		return
	}
	defer file.Close()
	if strings.ToLower(filepath.Ext(header.Filename)) != ".html" {
		writeError(w, http.StatusUnsupportedMediaType, "story file must use the .html extension")
		return
	}
	data, err := io.ReadAll(file)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "file too large (max 25 MB)")
			return
		}
		writeError(w, http.StatusBadRequest, "failed to read uploaded story")
		return
	}
	lower := strings.ToLower(string(data[:min(len(data), 4096)]))
	if !strings.Contains(lower, "<html") && !strings.Contains(lower, "<!doctype html") {
		writeError(w, http.StatusUnsupportedMediaType, "file does not look like an HTML document")
		return
	}

	version := 1
	postID := strings.TrimSpace(r.FormValue("post_id"))
	if postID != "" {
		post, found, getErr := h.store.GetPost(r.Context(), postID)
		if getErr != nil {
			writeError(w, http.StatusInternalServerError, getErr.Error())
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "post not found")
			return
		}
		version = max(post.StoryVersion+1, 1)
	}
	slug := safeStorySlug(r.FormValue("slug"))
	key := fmt.Sprintf("admin-stories/%s/v%d-%s.html", slug, version, uuid.New().String())
	provider := "r2"
	publicURL := ""
	if h.app.R2 != nil && h.app.Config.R2PublicBaseURL != "" {
		if err := h.app.R2.PutObject(r.Context(), key, data, "text/html; charset=utf-8"); err != nil {
			writeError(w, http.StatusInternalServerError, "failed to upload story to R2")
			return
		}
		publicKey := strings.Trim(strings.Trim(h.app.Config.R2Prefix, "/")+"/"+key, "/")
		publicURL = strings.TrimRight(h.app.Config.R2PublicBaseURL, "/") + "/" + publicKey
	} else if h.app.Config.BunnyAccessKey != "" {
		provider = "bunny"
		publicURL, err = bunnyUpload(h.app.Config.BunnyAccessKey, strings.TrimSuffix(key, ".html"), "html", data)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to upload story to CDN")
			return
		}
	} else {
		writeError(w, http.StatusServiceUnavailable, "R2_PUBLIC_BASE_URL is required for story upload")
		return
	}
	digest := sha256.Sum256(data)
	writeJSON(w, http.StatusOK, storyUploadResponse{
		URL: publicURL, Version: version, StorageProvider: provider, Key: key,
		SizeBytes: len(data), Checksum: hex.EncodeToString(digest[:]),
	})
}

func safeStorySlug(value string) string {
	value = strings.Trim(nonSlugChars.ReplaceAllString(strings.ToLower(value), "-"), "-")
	if value == "" {
		return "story"
	}
	return value
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"detail": message})
}
