package scripts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"clashking_tracking/models"
)

const adminSessionLifetime = 7 * 24 * time.Hour

var allowedAdminDiscordUserIDs = map[string]bool{
	"706149153431879760": true,
	"506210109790093342": true,
}

var (
	discordAuthorizeEndpoint   = "https://discord.com/oauth2/authorize"
	discordTokenEndpoint       = "https://discord.com/api/v10/oauth2/token"
	discordCurrentUserEndpoint = "https://discord.com/api/v10/users/@me"
)

type adminContextKey struct{}

type authenticatedAdmin struct {
	User      models.AdminUser
	TokenHash string
}

type discordOAuthUser struct {
	ID         string  `json:"id"`
	Username   string  `json:"username"`
	GlobalName *string `json:"global_name"`
	Avatar     *string `json:"avatar"`
}

func newSessionToken() (raw, hash string, err error) {
	bytes := make([]byte, 32)
	if _, err = rand.Read(bytes); err != nil {
		return "", "", err
	}
	raw = base64.RawURLEncoding.EncodeToString(bytes)
	hashBytes := sha256.Sum256([]byte(raw))
	return raw, hex.EncodeToString(hashBytes[:]), nil
}

func sessionTokenHash(raw string) string {
	hash := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(hash[:])
}

func bearerToken(r *http.Request) string {
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(header) < 8 || !strings.EqualFold(header[:7], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(header[7:])
}

func adminFromRequest(r *http.Request) (authenticatedAdmin, bool) {
	admin, ok := r.Context().Value(adminContextKey{}).(authenticatedAdmin)
	return admin, ok
}

func requestIPAddress(r *http.Request) string {
	if value := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); value != "" {
		return value
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func (h *mobilePushHandlers) discordLogin(w http.ResponseWriter, r *http.Request) {
	if !h.discordOAuthConfigured() {
		writeError(w, http.StatusServiceUnavailable, "Discord login is not configured")
		return
	}
	state, _, err := newSessionToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to start Discord login")
		return
	}
	secure := strings.HasPrefix(strings.ToLower(h.app.Config.MobilePushDiscordRedirectURL), "https://")
	http.SetCookie(w, &http.Cookie{Name: "ck_admin_oauth_state", Value: state, Path: "/auth/discord", HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode, MaxAge: 600})
	authorizeURL, _ := url.Parse(discordAuthorizeEndpoint)
	query := authorizeURL.Query()
	query.Set("client_id", h.app.Config.MobilePushDiscordClientID)
	query.Set("redirect_uri", h.app.Config.MobilePushDiscordRedirectURL)
	query.Set("response_type", "code")
	query.Set("scope", "identify")
	query.Set("state", state)
	authorizeURL.RawQuery = query.Encode()
	http.Redirect(w, r, authorizeURL.String(), http.StatusFound)
}

func (h *mobilePushHandlers) discordCallback(w http.ResponseWriter, r *http.Request) {
	if !h.discordOAuthConfigured() {
		h.redirectOAuthError(w, r, "not_configured")
		return
	}
	stateCookie, err := r.Cookie("ck_admin_oauth_state")
	secure := strings.HasPrefix(strings.ToLower(h.app.Config.MobilePushDiscordRedirectURL), "https://")
	http.SetCookie(w, &http.Cookie{Name: "ck_admin_oauth_state", Value: "", Path: "/auth/discord", HttpOnly: true, Secure: secure, SameSite: http.SameSiteLaxMode, MaxAge: -1})
	state := r.URL.Query().Get("state")
	if err != nil || state == "" || subtle.ConstantTimeCompare([]byte(state), []byte(stateCookie.Value)) != 1 {
		h.redirectOAuthError(w, r, "invalid_state")
		return
	}
	if r.URL.Query().Get("error") != "" {
		h.redirectOAuthError(w, r, "cancelled")
		return
	}
	code := r.URL.Query().Get("code")
	if code == "" {
		h.redirectOAuthError(w, r, "missing_code")
		return
	}
	accessToken, err := h.exchangeDiscordCode(r.Context(), code)
	if err != nil {
		h.app.Logger.Warn("admin Discord code exchange failed", "err", err)
		h.redirectOAuthError(w, r, "exchange_failed")
		return
	}
	profile, err := fetchDiscordUser(r.Context(), accessToken)
	if err != nil {
		h.app.Logger.Warn("admin Discord profile failed", "err", err)
		h.redirectOAuthError(w, r, "profile_failed")
		return
	}
	if !allowedAdminDiscordUserIDs[profile.ID] {
		h.recordAuditAs(r, "discord:"+profile.ID, "auth.denied", "admin_user", profile.ID, "Denied Discord admin sign-in", nil)
		h.redirectOAuthError(w, r, "not_allowed")
		return
	}
	user, err := h.store.UpsertDiscordAdminUser(r.Context(), profile)
	if err != nil {
		h.app.Logger.Error("admin Discord user upsert failed", "err", err)
		h.redirectOAuthError(w, r, "session_failed")
		return
	}
	rawToken, tokenHash, err := newSessionToken()
	if err != nil {
		h.redirectOAuthError(w, r, "session_failed")
		return
	}
	session, err := h.store.CreateAdminSession(r.Context(), user.ID, tokenHash, requestIPAddress(r), r.UserAgent(), time.Now().UTC().Add(adminSessionLifetime))
	if err != nil {
		h.app.Logger.Error("admin Discord session failed", "err", err)
		h.redirectOAuthError(w, r, "session_failed")
		return
	}
	h.recordAuditAs(r, user.DisplayName+" (Discord "+user.DiscordUserID+")", "auth.login", "admin_user", user.ID, "Signed in with Discord", nil)
	destination, err := url.Parse(h.app.Config.MobilePushAdminPanelURL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "admin panel URL is invalid")
		return
	}
	destination.Fragment = "session=" + url.QueryEscape(rawToken) + "&expires_at=" + url.QueryEscape(session.ExpiresAt.Format(time.RFC3339))
	http.Redirect(w, r, destination.String(), http.StatusFound)
}

func (h *mobilePushHandlers) exchangeDiscordCode(ctx context.Context, code string) (string, error) {
	form := url.Values{"client_id": {h.app.Config.MobilePushDiscordClientID}, "client_secret": {h.app.Config.MobilePushDiscordClientSecret}, "grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {h.app.Config.MobilePushDiscordRedirectURL}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, discordTokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := (&http.Client{Timeout: 12 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	var payload struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return "", err
	}
	if response.StatusCode != http.StatusOK || payload.AccessToken == "" {
		return "", fmt.Errorf("Discord token endpoint returned %s", response.Status)
	}
	return payload.AccessToken, nil
}

func fetchDiscordUser(ctx context.Context, accessToken string) (models.DiscordAdminProfile, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, discordCurrentUserEndpoint, nil)
	if err != nil {
		return models.DiscordAdminProfile{}, err
	}
	request.Header.Set("Authorization", "Bearer "+accessToken)
	response, err := (&http.Client{Timeout: 12 * time.Second}).Do(request)
	if err != nil {
		return models.DiscordAdminProfile{}, err
	}
	defer response.Body.Close()
	var user discordOAuthUser
	if err := json.NewDecoder(response.Body).Decode(&user); err != nil {
		return models.DiscordAdminProfile{}, err
	}
	if response.StatusCode != http.StatusOK || user.ID == "" {
		return models.DiscordAdminProfile{}, fmt.Errorf("Discord users/@me returned %s", response.Status)
	}
	displayName := user.Username
	if user.GlobalName != nil && strings.TrimSpace(*user.GlobalName) != "" {
		displayName = strings.TrimSpace(*user.GlobalName)
	}
	avatarURL := ""
	if user.Avatar != nil && *user.Avatar != "" {
		avatarURL = fmt.Sprintf("https://cdn.discordapp.com/avatars/%s/%s.png?size=128", user.ID, *user.Avatar)
	}
	return models.DiscordAdminProfile{ID: user.ID, Username: user.Username, DisplayName: displayName, AvatarURL: avatarURL}, nil
}

func (h *mobilePushHandlers) discordOAuthConfigured() bool {
	return h.app.Config.MobilePushDiscordClientID != "" && h.app.Config.MobilePushDiscordClientSecret != "" && h.app.Config.MobilePushDiscordRedirectURL != "" && h.app.Config.MobilePushAdminPanelURL != ""
}

func (h *mobilePushHandlers) redirectOAuthError(w http.ResponseWriter, r *http.Request, code string) {
	destination, err := url.Parse(h.app.Config.MobilePushAdminPanelURL)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "Discord authentication failed")
		return
	}
	query := destination.Query()
	query.Set("auth_error", code)
	destination.RawQuery = query.Encode()
	http.Redirect(w, r, destination.String(), http.StatusFound)
}

func (h *mobilePushHandlers) logout(w http.ResponseWriter, r *http.Request) {
	admin, _ := adminFromRequest(r)
	if admin.TokenHash != "" {
		_ = h.store.DeleteAdminSession(r.Context(), admin.TokenHash)
	}
	h.recordAudit(r, "auth.logout", "admin_user", admin.User.ID, "Signed out of the admin panel", nil)
	w.WriteHeader(http.StatusNoContent)
}

func (h *mobilePushHandlers) me(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	admin, _ := adminFromRequest(r)
	writeJSON(w, http.StatusOK, admin.User)
}
