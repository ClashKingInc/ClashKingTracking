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

	"github.com/disgoorg/disgo/discord"
)

const clashLinkHost = "link.clashofclans.com"

type discordLinkParseSettings struct {
	LinkParse struct {
		Army   bool `json:"army"`
		Base   bool `json:"base"`
		Clan   bool `json:"clan"`
		Player bool `json:"player"`
		Show   bool `json:"show"`
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
	queries := showClanQueries(payload.Content)
	if link == nil && len(queries) == 0 {
		return nil
	}

	var settings discordLinkParseSettings
	if err := w.clashKingAPIGet(ctx, "/v2/server/"+url.PathEscape(payload.GuildID)+"/settings", &settings); err != nil {
		return fmt.Errorf("load link-parse settings: %w", err)
	}
	if link == nil {
		if !settings.LinkParse.Show {
			return nil
		}
		return w.deliverShowClans(ctx, payload, queries)
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

// Legacy -show resolves up to five comma-separated clan names from this
// server's configured family. It must never search arbitrary external clans.
func showClanQueries(content string) []string {
	if !strings.HasPrefix(content, "-show ") {
		return nil
	}
	var queries []string
	for _, value := range strings.Split(strings.TrimPrefix(content, "-show "), ",") {
		if value = strings.TrimSpace(value); value != "" {
			queries = append(queries, strings.ToLower(value))
			if len(queries) == 5 {
				break
			}
		}
	}
	return queries
}

func (w *discordDeliveryWorker) deliverShowClans(ctx context.Context, payload discordMessageCreatePayload, queries []string) error {
	var family []struct {
		Tag  string `json:"tag"`
		Name string `json:"name"`
	}
	if err := w.clashKingAPIGet(ctx, "/v2/server/"+url.PathEscape(payload.GuildID)+"/clans-basic", &family); err != nil {
		return fmt.Errorf("load -show family clans: %w", err)
	}
	inline := true
	var embeds []discord.Embed
	seen := map[string]bool{}
	for _, query := range queries {
		for _, reference := range family {
			if !strings.Contains(strings.ToLower(reference.Name), query) {
				continue
			}
			if seen[reference.Tag] {
				break
			}
			seen[reference.Tag] = true
			var clan map[string]any
			if err := w.clashKingAPIGet(ctx, "/v2/clan/"+url.PathEscape(reference.Tag)+"/cached", &clan); err != nil {
				return fmt.Errorf("load -show clan: %w", err)
			}
			embed := discord.Embed{
				Title:       discordClip(reference.Name, 256),
				URL:         "https://link.clashofclans.com/en?action=OpenClanProfile&tag=" + url.QueryEscape(reference.Tag),
				Description: discordClip(stringMapValue(clan, "description"), 600),
				Fields: []discord.EmbedField{
					{Name: "Clan", Value: reference.Tag, Inline: &inline},
					{Name: "Level", Value: fmt.Sprint(intValue(clan["clanLevel"])), Inline: &inline},
					{Name: "Members", Value: fmt.Sprintf("%d/50", intValue(clan["members"])), Inline: &inline},
					{Name: "Trophies", Value: fmt.Sprint(intValue(clan["clanPoints"])), Inline: &inline},
					{Name: "War wins", Value: fmt.Sprint(intValue(clan["warWins"])), Inline: &inline},
				},
			}
			if badge, ok := mapValue(clan["badgeUrls"]); ok {
				if image := firstDiscordString(stringMapValue(badge, "large"), stringMapValue(badge, "medium")); image != "" {
					embed.Thumbnail = &discord.EmbedResource{URL: image}
				}
			}
			embeds = append(embeds, embed)
			break
		}
	}
	if len(embeds) == 0 {
		return nil
	}
	return w.sendMessage(ctx, discordDeliveryTarget{ServerID: payload.GuildID, ChannelID: payload.ChannelID}, discord.MessageCreate{Embeds: embeds}, nil)
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
