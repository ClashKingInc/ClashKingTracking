package scripts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
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
	mux.HandleFunc("GET /auth/discord/start", h.discordLogin)
	mux.HandleFunc("GET /auth/discord/callback", h.discordCallback)
	mux.HandleFunc("POST /auth/logout", h.requireAdmin(h.logout))
	mux.HandleFunc("GET /auth/me", h.requireAdmin(h.me))
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
	mux.HandleFunc("POST /campaigns", h.requireAdmin(h.createCampaign))
	mux.HandleFunc("PATCH /campaigns/{id}", h.requireAdmin(h.updateCampaign))
	mux.HandleFunc("GET /admin/dashboard", h.requireAdmin(h.adminDashboard))
	mux.HandleFunc("GET /admin/proxy/stats", h.requireAdmin(h.proxyStats))
	mux.HandleFunc("GET /admin/audit", h.requireAdmin(h.listAuditEvents))
	mux.HandleFunc("GET /feature-flags", h.requireAdmin(h.listFeatureFlags))
	mux.HandleFunc("POST /feature-flags", h.requireAdmin(h.createFeatureFlag))
	mux.HandleFunc("PATCH /feature-flags/{key}", h.requireAdmin(h.updateFeatureFlag))

	return &http.Server{
		Addr:    app.Config.MobilePushHTTPAddr,
		Handler: withCORS(mux),
	}
}

func (h *mobilePushHandlers) listAuditEvents(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 500 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		limit = parsed
	}
	events, err := h.store.ListAuditEvents(r.Context(), limit, r.URL.Query().Get("resource_type"), r.URL.Query().Get("actor"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (h *mobilePushHandlers) recordAudit(r *http.Request, action, resourceType, resourceID, summary string, metadata map[string]any) {
	actor := "discord-admin"
	if admin, ok := adminFromRequest(r); ok {
		actor = admin.User.Username + " (Discord " + admin.User.DiscordUserID + ")"
	}
	h.recordAuditAs(r, actor, action, resourceType, resourceID, summary, metadata)
}

func (h *mobilePushHandlers) recordAuditAs(r *http.Request, actor, action, resourceType, resourceID, summary string, metadata map[string]any) {
	ipAddress := strings.TrimSpace(r.Header.Get("CF-Connecting-IP"))
	if ipAddress == "" {
		ipAddress, _, _ = net.SplitHostPort(r.RemoteAddr)
		if ipAddress == "" {
			ipAddress = r.RemoteAddr
		}
	}
	err := h.store.RecordAuditEvent(r.Context(), models.AdminAuditEventInput{Actor: actor, Action: action,
		ResourceType: resourceType, ResourceID: resourceID, Summary: summary, Metadata: metadata,
		IPAddress: ipAddress, UserAgent: r.UserAgent()})
	if err != nil {
		h.app.Logger.Error("mobile_push: audit event failed", "action", action, "resource_id", resourceID, "err", err)
	}
}

func (h *mobilePushHandlers) listFeatureFlags(w http.ResponseWriter, r *http.Request) {
	flags, err := h.store.ListFeatureFlags(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, flags)
}

func (h *mobilePushHandlers) createFeatureFlag(w http.ResponseWriter, r *http.Request) {
	input, err := decodeFeatureFlagInput(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateFeatureFlagInput(input, true); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	flag, err := h.store.CreateFeatureFlag(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.recordAudit(r, "feature_flag.create", "feature_flag", flag.Key, "Created feature flag "+flag.Name, map[string]any{"enabled": flag.Enabled, "rollout_percentage": flag.RolloutPercentage})
	writeJSON(w, http.StatusCreated, flag)
}

func (h *mobilePushHandlers) updateFeatureFlag(w http.ResponseWriter, r *http.Request) {
	input, err := decodeFeatureFlagInput(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := validateFeatureFlagInput(input, false); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	flag, found, err := h.store.UpdateFeatureFlag(r.Context(), r.PathValue("key"), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "feature flag not found")
		return
	}
	h.recordAudit(r, "feature_flag.update", "feature_flag", flag.Key, "Updated feature flag "+flag.Name, map[string]any{"enabled": flag.Enabled, "rollout_percentage": flag.RolloutPercentage})
	writeJSON(w, http.StatusOK, flag)
}

func decodeFeatureFlagInput(reader io.Reader) (models.AdminFeatureFlagInput, error) {
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(reader).Decode(&raw); err != nil {
		return models.AdminFeatureFlagInput{}, err
	}
	data, _ := json.Marshal(raw)
	var input models.AdminFeatureFlagInput
	if err := json.Unmarshal(data, &input); err != nil {
		return input, err
	}
	if value, ok := raw["minAppVersion"]; ok && string(value) == "null" {
		input.ClearMinAppVersion = true
	}
	if value, ok := raw["startsAt"]; ok && string(value) == "null" {
		input.ClearStartsAt = true
	}
	if value, ok := raw["endsAt"]; ok && string(value) == "null" {
		input.ClearEndsAt = true
	}
	return input, nil
}

func validateFeatureFlagInput(input models.AdminFeatureFlagInput, creating bool) error {
	if creating && (input.Key == nil || strings.TrimSpace(*input.Key) == "") {
		return errors.New("key is required")
	}
	if creating && (input.Name == nil || strings.TrimSpace(*input.Name) == "") {
		return errors.New("name is required")
	}
	if input.Key != nil {
		matched, _ := regexp.MatchString(`^[a-z][a-z0-9_]{2,63}$`, strings.TrimSpace(*input.Key))
		if !matched {
			return errors.New("key must use lowercase letters, numbers, and underscores")
		}
	}
	if input.RolloutPercentage != nil && (*input.RolloutPercentage < 0 || *input.RolloutPercentage > 100) {
		return errors.New("rolloutPercentage must be between 0 and 100")
	}
	if input.PublicExposure != nil && *input.PublicExposure != "safe" && *input.PublicExposure != "sensitive" {
		return errors.New("publicExposure must be safe or sensitive")
	}
	if len(input.Platforms) > 0 {
		for _, platform := range input.Platforms {
			if platform != "ios" && platform != "android" && platform != "web" {
				return errors.New("unsupported platform")
			}
		}
	}
	return nil
}

func (h *mobilePushHandlers) adminDashboard(w http.ResponseWriter, r *http.Request) {
	days := 30
	if raw := r.URL.Query().Get("days"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 365 {
			writeError(w, http.StatusBadRequest, "days must be between 1 and 365")
			return
		}
		days = parsed
	}
	snapshot, err := h.store.AdminDashboard(r.Context(), days, time.Now().UTC())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Admin-Actor")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// requireAdmin accepts only a database-backed session issued after Discord
// verified one of the two allowlisted user IDs.
func (h *mobilePushHandlers) requireAdmin(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rawToken := bearerToken(r)
		if rawToken == "" {
			writeError(w, http.StatusUnauthorized, "invalid or missing admin session")
			return
		}
		tokenHash := sessionTokenHash(rawToken)
		session, found, err := h.store.GetAdminSession(r.Context(), tokenHash, time.Now().UTC())
		if err != nil {
			writeError(w, http.StatusInternalServerError, "unable to validate admin session")
			return
		}
		if !found {
			writeError(w, http.StatusUnauthorized, "invalid or missing admin session")
			return
		}
		ctx := context.WithValue(r.Context(), adminContextKey{}, authenticatedAdmin{User: session.User, TokenHash: tokenHash})
		next(w, r.WithContext(ctx))
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

func (h *mobilePushHandlers) createCampaign(w http.ResponseWriter, r *http.Request) {
	input, err := decodeCampaignInput(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if input.Title == nil || strings.TrimSpace(*input.Title) == "" {
		writeError(w, http.StatusBadRequest, "title is required")
		return
	}
	if input.Body == nil || strings.TrimSpace(*input.Body) == "" {
		writeError(w, http.StatusBadRequest, "body is required")
		return
	}
	if err := validateCampaignInput(input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	campaign, err := h.store.CreateCampaign(r.Context(), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.recordAudit(r, "campaign.create", "campaign", campaign.ID, "Created campaign "+campaign.Title, map[string]any{"status": campaign.Status, "trigger_type": campaign.TriggerType})
	writeJSON(w, http.StatusCreated, campaign)
}

func (h *mobilePushHandlers) updateCampaign(w http.ResponseWriter, r *http.Request) {
	input, err := decodeCampaignInput(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := validateCampaignInput(input); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	campaign, found, err := h.store.UpdateCampaign(r.Context(), r.PathValue("id"), input)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "campaign not found")
		return
	}
	h.recordAudit(r, "campaign.update", "campaign", campaign.ID, "Updated campaign "+campaign.Title, map[string]any{"status": campaign.Status, "trigger_type": campaign.TriggerType})
	writeJSON(w, http.StatusOK, campaign)
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
	h.recordAudit(r, "post.create", "post", post.ID, "Created post "+post.Title, map[string]any{"status": post.Status, "presentation_type": post.PresentationType})
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
	h.recordAudit(r, "post.update", "post", post.ID, "Updated post "+post.Title, map[string]any{"status": post.Status, "revision": post.RevisionNumber})
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
	h.recordAudit(r, "post.duplicate", "post", copy.ID, "Duplicated post "+post.Title, map[string]any{"source_post_id": post.ID})
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
	h.recordAudit(r, "post.restore", "post", restored.ID, "Restored post revision", map[string]any{"revision": revisionNumber})
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

func decodeCampaignInput(reader io.Reader) (models.NotificationCampaignInput, error) {
	raw, err := io.ReadAll(reader)
	if err != nil {
		return models.NotificationCampaignInput{}, err
	}
	var input models.NotificationCampaignInput
	if err := json.Unmarshal(raw, &input); err != nil {
		return models.NotificationCampaignInput{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return models.NotificationCampaignInput{}, err
	}
	isNull := func(name string) bool {
		value, ok := fields[name]
		return ok && string(value) == "null"
	}
	input.ClearTargetRoute = isNull("target_route")
	input.ClearDayOfMonth = isNull("day_of_month")
	input.ClearSendAt = isNull("send_at")
	input.ClearSendTime = isNull("send_time")
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

func validateCampaignInput(input models.NotificationCampaignInput) error {
	if input.Title != nil && strings.TrimSpace(*input.Title) == "" {
		return errors.New("title cannot be empty")
	}
	if input.Body != nil && strings.TrimSpace(*input.Body) == "" {
		return errors.New("body cannot be empty")
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
	for _, locale := range input.TargetLocales {
		if normalizeCampaignLocale(locale) == "" {
			return fmt.Errorf("unsupported locale: %s", locale)
		}
	}
	if input.Status != nil && !allowedValue(*input.Status, "draft", "scheduled", "sent", "paused") {
		return errors.New("status must be draft, scheduled, sent, or paused")
	}
	if input.TriggerType != nil && !allowedValue(*input.TriggerType, "manual", "monthly") {
		return errors.New("trigger_type must be manual or monthly")
	}
	if input.DayOfMonth != nil && (*input.DayOfMonth < 1 || *input.DayOfMonth > 28) {
		return errors.New("day_of_month must be between 1 and 28")
	}
	if input.SendTime != nil && !validCampaignSendTime(*input.SendTime) {
		return errors.New("send_time must use HH:mm format")
	}
	return nil
}

func validCampaignSendTime(value string) bool {
	parts := strings.Split(strings.TrimSpace(value), ":")
	if len(parts) != 2 || len(parts[0]) != 2 || len(parts[1]) != 2 {
		return false
	}
	hour, hourErr := strconv.Atoi(parts[0])
	minute, minuteErr := strconv.Atoi(parts[1])
	return hourErr == nil && minuteErr == nil && hour >= 0 && hour <= 23 && minute >= 0 && minute <= 59
}

func allowedValue(value string, allowed ...string) bool {
	for _, item := range allowed {
		if value == item {
			return true
		}
	}
	return false
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
	h.recordAudit(r, "post.archive", "post", r.PathValue("id"), "Archived post", nil)
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
	h.recordAudit(r, "post.publish", "post", result.ID, "Published post "+result.Title, map[string]any{"push_sent": result.PushSent, "push_skipped": result.PushSkipped})
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
	h.recordAudit(r, "post.push", "post", post.ID, "Sent post notification "+post.Title, map[string]any{"sent": sent, "skipped": skipped})
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
	count, err := h.store.AudienceCount(r.Context(), post.Platforms, nil)
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
	CampaignID    string                                            `json:"campaign_id"`
	Title         string                                            `json:"title"`
	Body          string                                            `json:"body"`
	TargetRoute   string                                            `json:"target_route"`
	Platforms     []string                                          `json:"platforms"`
	TargetLocales []string                                          `json:"target_locales"`
	Translations  map[string]models.NotificationCampaignTranslation `json:"translations"`
}

func (h *mobilePushHandlers) testPushAudience(w http.ResponseWriter, r *http.Request) {
	platforms := strings.Split(strings.TrimSpace(r.URL.Query().Get("platforms")), ",")
	if len(platforms) == 0 || (len(platforms) == 1 && platforms[0] == "") {
		platforms = []string{"ios", "android"}
	}
	locales := strings.Split(strings.TrimSpace(r.URL.Query().Get("locales")), ",")
	if len(locales) == 1 && locales[0] == "" {
		locales = nil
	}
	devices, err := h.store.DevicesForPlatforms(r.Context(), platforms, locales)
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
	devices, err := h.store.DevicesForPlatforms(r.Context(), input.Platforms, input.TargetLocales)
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
	sent, skipped := sendLocalizedPush(r.Context(), h.app, sandboxDevices, func(locale string) pushMessage {
		title, body := input.Title, input.Body
		if translation, ok := input.Translations[locale]; ok {
			if translation.Title != "" {
				title = translation.Title
			}
			if translation.Body != "" {
				body = translation.Body
			}
		}
		return pushMessage{Title: title, Body: body, Data: data}
	})
	h.recordAudit(r, "push.test", "campaign", input.CampaignID, "Sent sandbox test notification", map[string]any{"eligible": len(sandboxDevices), "sent": sent, "skipped": skipped})
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
	if isPersistentR2Store(h.app.R2) && h.app.Config.R2PublicBaseURL != "" {
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

func isPersistentR2Store(store platform.ObjectStore) bool {
	_, ok := store.(*platform.R2ObjectStore)
	return ok
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
