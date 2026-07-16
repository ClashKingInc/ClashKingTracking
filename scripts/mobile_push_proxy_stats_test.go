package scripts

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"clashking_tracking/internal/platform"
)

func TestProxyStatsForwardsValidatedQuery(t *testing.T) {
	var received string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"now":"2026-07-15T00:00:00Z","windows":{}}`))
	}))
	defer upstream.Close()

	h := &mobilePushHandlers{app: &platform.App{Config: platform.Config{MobilePushProxyStatsURL: upstream.URL}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	request := httptest.NewRequest(http.MethodGet, "/admin/proxy/stats?series=5m&lookback=24h&endpoints=7d&limit=25", nil)
	response := httptest.NewRecorder()
	h.proxyStats(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if received != "endpoints=7d&limit=25&lookback=24h&series=5m" {
		t.Fatalf("unexpected forwarded query: %s", received)
	}
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("expected no-store response")
	}
}

func TestProxyStatsRejectsUnsupportedQuery(t *testing.T) {
	h := &mobilePushHandlers{app: &platform.App{Config: platform.Config{MobilePushProxyStatsURL: "https://proxy.example/stats"}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	request := httptest.NewRequest(http.MethodGet, "/admin/proxy/stats?series=2s", nil)
	response := httptest.NewRecorder()
	h.proxyStats(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", response.Code)
	}
}
