package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const clashLinkHost = "link.clashofclans.com"

type discordLinkParseSettings struct {
	LinkParse struct {
		Army   bool `json:"army"`
		Base   bool `json:"base"`
		Clan   bool `json:"clan"`
		Player bool `json:"player"`
	} `json:"link_parse"`
}

type discordMessageCreatePayload struct {
	ChannelID string
	Content   string
	GuildID   string
}

func (w *discordDeliveryWorker) deliverLinkParse(ctx context.Context, event mobileWarEvent) error {
	payload := discordMessagePayload(event)
	if payload.GuildID == "" || payload.ChannelID == "" {
		return nil
	}
	link := firstClashLink(payload.Content)
	if link == nil {
		return nil
	}

	var settings discordLinkParseSettings
	if err := w.clashKingAPIGet(ctx, "/v2/server/"+url.PathEscape(payload.GuildID)+"/settings", &settings); err != nil {
		return fmt.Errorf("load link-parse settings: %w", err)
	}

	action := link.Query().Get("action")
	var content string
	var err error
	switch action {
	case "OpenPlayerProfile":
		if !settings.LinkParse.Player {
			return nil
		}
		content, err = w.playerLinkText(ctx, link)
	case "OpenClanProfile":
		if !settings.LinkParse.Clan {
			return nil
		}
		content, err = w.clanLinkText(ctx, link)
	case "CopyArmy", "OpenArmyLink":
		if !settings.LinkParse.Army {
			return nil
		}
		content = "**Army link**\n" + link.String()
	case "OpenLayout":
		if !settings.LinkParse.Base {
			return nil
		}
		content = "**Base layout**\n" + link.String()
	default:
		return nil
	}
	if err != nil {
		return err
	}
	return w.send(ctx, discordDeliveryTarget{
		ServerID:  payload.GuildID,
		ChannelID: payload.ChannelID,
	}, content, nil)
}

func (w *discordDeliveryWorker) playerLinkText(ctx context.Context, link *url.URL) (string, error) {
	tag := normalizeClashLinkTag(link.Query().Get("tag"))
	if tag == "" {
		return "", errors.New("player link has no valid tag")
	}
	var player map[string]any
	if err := w.clashKingAPIGet(ctx, "/proxy/v1/players/"+url.PathEscape(tag), &player); err != nil {
		return "", fmt.Errorf("load linked player: %w", err)
	}
	name := stringMapValue(player, "name")
	if name == "" {
		name = "Player"
	}
	lines := []string{fmt.Sprintf("**%s** `%s`", name, tag)}
	if townHall := intValue(player["townHallLevel"]); townHall > 0 {
		lines = append(lines, fmt.Sprintf("Town Hall %d", townHall))
	}
	if trophies := intValue(player["trophies"]); trophies > 0 {
		lines = append(lines, fmt.Sprintf("%d trophies", trophies))
	}
	if clan, ok := mapValue(player["clan"]); ok {
		if clanName := stringMapValue(clan, "name"); clanName != "" {
			lines = append(lines, clanName)
		}
	}
	lines = append(lines, link.String())
	return strings.Join(lines, " · "), nil
}

func (w *discordDeliveryWorker) clanLinkText(ctx context.Context, link *url.URL) (string, error) {
	tag := normalizeClashLinkTag(link.Query().Get("tag"))
	if tag == "" {
		return "", errors.New("clan link has no valid tag")
	}
	var clan map[string]any
	if err := w.clashKingAPIGet(ctx, "/v2/clan/"+url.PathEscape(tag)+"/cached", &clan); err != nil {
		return "", fmt.Errorf("load linked clan: %w", err)
	}
	name := stringMapValue(clan, "name")
	if name == "" {
		name = "Clan"
	}
	lines := []string{fmt.Sprintf("**%s** `%s`", name, tag)}
	if level := intValue(clan["clanLevel"]); level > 0 {
		lines = append(lines, fmt.Sprintf("Level %d", level))
	}
	if members := intValue(clan["members"]); members > 0 {
		lines = append(lines, fmt.Sprintf("%d members", members))
	}
	lines = append(lines, link.String())
	return strings.Join(lines, " · "), nil
}

func (w *discordDeliveryWorker) clashKingAPIGet(ctx context.Context, path string, out any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(w.app.Config.ClashKingAPIURL, "/")+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+w.app.Config.ClashKingAPIToken)
	request.Header.Set("Accept", "application/json")
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1024))
		return fmt.Errorf("ClashKing API returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(response.Body).Decode(out)
}

func discordMessagePayload(event mobileWarEvent) discordMessageCreatePayload {
	return discordMessageCreatePayload{
		ChannelID: stringMapValue(event.Value, "channel_id"),
		Content:   stringMapValue(event.Value, "content"),
		GuildID:   stringMapValue(event.Value, "guild_id"),
	}
}

func firstClashLink(content string) *url.URL {
	for _, field := range strings.Fields(content) {
		candidate := strings.Trim(field, "<>[](){}\"'.,")
		parsed, err := url.Parse(candidate)
		if err == nil && parsed.Scheme == "https" && strings.EqualFold(parsed.Hostname(), clashLinkHost) {
			return parsed
		}
	}
	return nil
}

func normalizeClashLinkTag(value string) string {
	value = strings.TrimPrefix(strings.TrimSpace(value), "#")
	value = strings.ReplaceAll(strings.ToUpper(value), "O", "0")
	if value == "" {
		return ""
	}
	for _, character := range value {
		if !strings.ContainsRune("0289PYLQGRJCUV", character) {
			return ""
		}
	}
	return "#" + value
}
