package armyfamily

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
)

const DefaultNamingModel = "@cf/zai-org/glm-5.3-flash"

var familyNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9 '&-]{1,38}[A-Za-z0-9]$`)

type NamingInput struct {
	Hash          Hash
	TroopNames    []string
	SpellNames    []string
	ExistingNames []string
}

type CloudflareNamer struct {
	HTTPClient *http.Client
	APIOrigin  string
	AccountID  string
	GatewayID  string
	APIToken   string
	Model      string
}

func (n CloudflareNamer) Name(ctx context.Context, input NamingInput) (string, error) {
	if strings.TrimSpace(n.AccountID) == "" || strings.TrimSpace(n.GatewayID) == "" || n.APIToken == "" {
		return "", errors.New("Cloudflare army-family naming configuration is incomplete")
	}
	model := firstNonEmpty(strings.TrimSpace(n.Model), DefaultNamingModel)
	origin := strings.TrimRight(firstNonEmpty(strings.TrimSpace(n.APIOrigin), "https://api.cloudflare.com/client/v4"), "/")
	prompt := namingPrompt(input)
	body, err := json.Marshal(map[string]any{
		"messages": []map[string]string{
			{"role": "system", "content": "Name a Clash of Clans army. Return only a concise unique name of at most five words."},
			{"role": "user", "content": prompt},
		},
		"max_tokens":  24,
		"temperature": 0.2,
	})
	if err != nil {
		return "", err
	}
	url := fmt.Sprintf("%s/accounts/%s/ai/run/%s", origin, n.AccountID, model)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+n.APIToken)
	req.Header.Set("cf-aig-gateway-id", n.GatewayID)
	req.Header.Set("Content-Type", "application/json")
	client := n.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", fmt.Errorf("Cloudflare naming request returned %s", response.Status)
	}
	var decoded struct {
		Result struct {
			Response string `json:"response"`
		} `json:"result"`
	}
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		return "", err
	}
	name := strings.Trim(strings.TrimSpace(decoded.Result.Response), "\"'`.*")
	if !ValidUniqueName(name, input.ExistingNames) {
		return "", fmt.Errorf("Cloudflare returned invalid or duplicate army-family name %q", name)
	}
	return name, nil
}

func NameWithFallback(ctx context.Context, namer CloudflareNamer, input NamingInput) (string, string) {
	if name, err := namer.Name(ctx, input); err == nil {
		return name, "ai"
	}
	return FallbackName(input), "fallback"
}

func ValidUniqueName(name string, existing []string) bool {
	name = strings.TrimSpace(name)
	if len(strings.Fields(name)) > 5 || !familyNamePattern.MatchString(name) {
		return false
	}
	for _, other := range existing {
		if strings.EqualFold(strings.TrimSpace(other), name) {
			return false
		}
	}
	return true
}

func FallbackName(input NamingInput) string {
	parts := append([]string(nil), input.TroopNames...)
	if len(parts) == 0 {
		parts = append(parts, input.SpellNames...)
	}
	sort.Strings(parts)
	stem := "Army"
	if len(parts) > 0 {
		stem = sanitizeNamePart(parts[0])
	}
	suffix := strings.ToUpper(hex.EncodeToString(input.Hash[:3]))
	for attempt := 0; ; attempt++ {
		candidate := fmt.Sprintf("%s %s", stem, suffix)
		if attempt > 0 {
			candidate = fmt.Sprintf("%s %s%d", stem, suffix, attempt+1)
		}
		if len(candidate) > 40 {
			candidate = candidate[:40]
		}
		if ValidUniqueName(candidate, input.ExistingNames) {
			return candidate
		}
	}
}

func namingPrompt(input NamingInput) string {
	return fmt.Sprintf("Troops: %s\nSpells: %s\nNames already used: %s",
		strings.Join(input.TroopNames, ", "), strings.Join(input.SpellNames, ", "), strings.Join(input.ExistingNames, ", "))
}

func sanitizeNamePart(value string) string {
	value = strings.TrimSpace(value)
	var out strings.Builder
	for _, r := range value {
		if r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			out.WriteRune(r)
		}
	}
	if out.Len() == 0 {
		return "Army"
	}
	if out.Len() > 24 {
		return out.String()[:24]
	}
	return out.String()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
