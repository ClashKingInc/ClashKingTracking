package scripts

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const maxProxyStatsResponseSize = 4 * 1024 * 1024

var proxyStatsAllowedValues = map[string]map[string]bool{
	"series":    {"1m": true, "5m": true, "15m": true, "30m": true, "1h": true},
	"lookback":  {"1h": true, "6h": true, "12h": true, "24h": true, "48h": true},
	"endpoints": {"24h": true, "7d": true},
}

func (h *mobilePushHandlers) proxyStats(w http.ResponseWriter, r *http.Request) {
	upstream, err := url.Parse(h.app.Config.MobilePushProxyStatsURL)
	if err != nil || (upstream.Scheme != "http" && upstream.Scheme != "https") || upstream.Host == "" {
		writeError(w, http.StatusServiceUnavailable, "proxy stats endpoint is not configured")
		return
	}
	query := upstream.Query()
	for key, values := range proxyStatsAllowedValues {
		value := r.URL.Query().Get(key)
		if value == "" {
			continue
		}
		if !values[value] {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("unsupported %s value", key))
			return
		}
		query.Set(key, value)
	}
	if rawLimit := r.URL.Query().Get("limit"); rawLimit != "" {
		limit, parseErr := strconv.Atoi(rawLimit)
		if parseErr != nil || limit < 1 || limit > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
		query.Set("limit", strconv.Itoa(limit))
	}
	upstream.RawQuery = query.Encode()

	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream.String(), nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "unable to build proxy stats request")
		return
	}
	request.Header.Set("Accept", "application/json")
	response, err := (&http.Client{Timeout: 12 * time.Second}).Do(request)
	if err != nil {
		writeError(w, http.StatusBadGateway, "proxy stats service is unavailable")
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxProxyStatsResponseSize+1))
	if err != nil || len(body) > maxProxyStatsResponseSize {
		writeError(w, http.StatusBadGateway, "invalid proxy stats response")
		return
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		writeError(w, http.StatusBadGateway, "proxy stats service returned "+response.Status)
		return
	}
	if !json.Valid(body) {
		writeError(w, http.StatusBadGateway, "proxy stats service returned invalid JSON")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
