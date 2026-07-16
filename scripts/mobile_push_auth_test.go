package scripts

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"
)

func TestDiscordLoginBuildsIdentifyAuthorizationRequest(t *testing.T) {
	app := testDiscordApp("http://tracking.test/auth/discord/callback", "http://panel.test")
	h := &mobilePushHandlers{app: app, store: newMemoryMobilePushStore()}
	request := httptest.NewRequest(http.MethodGet, "/auth/discord/start", nil)
	response := httptest.NewRecorder()
	h.discordLogin(response, request)
	if response.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", response.Code)
	}
	destination, err := url.Parse(response.Header().Get("Location"))
	if err != nil {
		t.Fatal(err)
	}
	if destination.Query().Get("scope") != "identify" || destination.Query().Get("state") == "" {
		t.Fatalf("invalid Discord authorization redirect: %s", destination.String())
	}
	if len(response.Result().Cookies()) != 1 || !response.Result().Cookies()[0].HttpOnly {
		t.Fatal("OAuth state must use an HttpOnly cookie")
	}
}

func TestDiscordAdminAllowlistContainsExactlyApprovedUsers(t *testing.T) {
	want := []string{"706149153431879760", "506210109790093342"}
	if len(allowedAdminDiscordUserIDs) != len(want) {
		t.Fatalf("expected exactly %d approved Discord users, got %d", len(want), len(allowedAdminDiscordUserIDs))
	}
	for _, id := range want {
		if !allowedAdminDiscordUserIDs[id] {
			t.Fatalf("Discord user %s must be approved", id)
		}
	}
}

func TestDiscordCallbackAllowsOnlyExplicitIDs(t *testing.T) {
	originalToken, originalUser := discordTokenEndpoint, discordCurrentUserEndpoint
	defer func() { discordTokenEndpoint, discordCurrentUserEndpoint = originalToken, originalUser }()
	currentID := "706149153431879760"
	discord := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "discord-access"})
			return
		}
		if r.Header.Get("Authorization") != "Bearer discord-access" {
			t.Fatalf("missing Discord bearer token")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"id": currentID, "username": "valen", "global_name": "Valen", "avatar": "avatarhash"})
	}))
	defer discord.Close()
	discordTokenEndpoint, discordCurrentUserEndpoint = discord.URL+"/token", discord.URL+"/me"

	store := newMemoryMobilePushStore()
	app := testDiscordApp("http://tracking.test/auth/discord/callback", "http://panel.test/proxy")
	h := &mobilePushHandlers{app: app, store: store}

	response := performDiscordCallback(t, h, "oauth-state")
	if response.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d: %s", response.Code, response.Body.String())
	}
	destination, _ := url.Parse(response.Header().Get("Location"))
	if destination.Host != "panel.test" || !strings.Contains(destination.Fragment, "session=") {
		t.Fatalf("missing panel session redirect: %s", destination.String())
	}
	if len(store.adminUsers) != 1 {
		t.Fatalf("expected one allowed user, got %d", len(store.adminUsers))
	}

	currentID = "111111111111111111"
	deniedStore := newMemoryMobilePushStore()
	denied := performDiscordCallback(t, &mobilePushHandlers{app: app, store: deniedStore}, "other-state")
	if denied.Code != http.StatusFound || !strings.Contains(denied.Header().Get("Location"), "auth_error=not_allowed") {
		t.Fatalf("expected access denial redirect, got %s", denied.Header().Get("Location"))
	}
	if len(deniedStore.adminUsers) != 0 {
		t.Fatal("denied Discord user must not be persisted")
	}
}

func TestAdminMiddlewareRequiresDiscordIssuedSession(t *testing.T) {
	store := newMemoryMobilePushStore()
	app := testDiscordApp("http://tracking.test/auth/discord/callback", "http://panel.test")
	h := &mobilePushHandlers{app: app, store: store}
	protected := h.requireAdmin(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	missing := httptest.NewRecorder()
	protected(missing, httptest.NewRequest(http.MethodGet, "/admin/dashboard", nil))
	if missing.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", missing.Code)
	}
	user, _ := store.UpsertDiscordAdminUser(context.Background(), models.DiscordAdminProfile{ID: "706149153431879760", Username: "valen", DisplayName: "Valen"})
	raw, hash, _ := newSessionToken()
	_, _ = store.CreateAdminSession(context.Background(), user.ID, hash, "", "", time.Now().Add(time.Hour))
	request := httptest.NewRequest(http.MethodGet, "/admin/dashboard", nil)
	request.Header.Set("Authorization", "Bearer "+raw)
	allowed := httptest.NewRecorder()
	protected(allowed, request)
	if allowed.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", allowed.Code)
	}
}

func performDiscordCallback(t *testing.T, h *mobilePushHandlers, state string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/auth/discord/callback?code=oauth-code&state="+state, nil)
	request.AddCookie(&http.Cookie{Name: "ck_admin_oauth_state", Value: state})
	response := httptest.NewRecorder()
	h.discordCallback(response, request)
	return response
}

func testDiscordApp(redirectURL, panelURL string) *platform.App {
	return &platform.App{Config: platform.Config{MobilePushDiscordClientID: "client-id", MobilePushDiscordClientSecret: "client-secret", MobilePushDiscordRedirectURL: redirectURL, MobilePushAdminPanelURL: panelURL}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
