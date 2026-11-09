package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
)

const clashLinkHost = "link.clashofclans.com"

type discordLinkParseSettings struct {
	EmbedColor string `json:"embed_color"`
	LinkParse  struct {
		Army   bool `json:"army"`
		Base   bool `json:"base"`
		Clan   bool `json:"clan"`
		Player bool `json:"player"`
		Show   bool `json:"show"`
	} `json:"link_parse"`
}

type discordMessageCreatePayload struct {
	ChannelID, Content, GuildID string
	Attachments                 []any
}

func (w *discordDeliveryWorker) deliverLinkParse(ctx context.Context, event mobileWarEvent) error {
	p := discordMessagePayload(event)
	if p.GuildID == "" || p.ChannelID == "" || !w.app.Config.AllowsDiscordGuild(p.GuildID) {
		return nil
	}
	link := firstClashLink(p.Content)
	if link == nil && len(showClanQueries(p.Content)) == 0 {
		return nil
	}
	var settings discordLinkParseSettings
	if err := w.clashKingAPIGet(ctx, "/v2/server/"+url.PathEscape(p.GuildID)+"/settings", &settings); err != nil {
		return fmt.Errorf("load link-parse settings: %w", err)
	}
	color := 0x2ecc71
	if n, err := strconv.ParseUint(strings.TrimPrefix(settings.EmbedColor, "#"), 16, 24); err == nil {
		color = int(n)
	}
	var message discord.MessageCreate
	var err error
	if link == nil {
		if !settings.LinkParse.Show {
			return nil
		}
		return w.deliverShowClans(ctx, p, showClanQueries(p.Content), color)
	} else {
		switch link.Query().Get("action") {
		case "OpenPlayerProfile":
			if !settings.LinkParse.Player {
				return nil
			}
			message, err = w.playerLinkMessage(ctx, link, color)
		case "OpenClanProfile":
			if !settings.LinkParse.Clan {
				return nil
			}
			message, err = w.clanLinkMessage(ctx, link, color)
		case "CopyArmy", "OpenArmyLink":
			if !settings.LinkParse.Army {
				return nil
			}
			message, err = w.armyLinkMessage(ctx, link, color)
		case "OpenLayout":
			if !settings.LinkParse.Base {
				return nil
			}
			message, err = baseLinkMessage(p, link, color)
		default:
			return nil
		}
	}
	if err != nil {
		return err
	}
	if len(message.Embeds) == 0 {
		return nil
	}
	return w.sendMessage(ctx, discordDeliveryTarget{ServerID: p.GuildID, ChannelID: p.ChannelID}, message, nil)
}

func (w *discordDeliveryWorker) playerLinkMessage(ctx context.Context, link *url.URL, color int) (discord.MessageCreate, error) {
	tag := normalizeClashLinkTag(link.Query().Get("tag"))
	if tag == "" {
		return discord.MessageCreate{}, errors.New("player link has no valid tag")
	}
	var player map[string]any
	if err := w.clashKingAPIGet(ctx, "/proxy/v1/players/"+url.PathEscape(tag), &player); err != nil {
		return discord.MessageCreate{}, fmt.Errorf("load linked player: %w", err)
	}
	name := firstDiscordString(stringMapValue(player, "name"), "Player")
	clan, _ := mapValue(player["clan"])
	description := fmt.Sprintf("**%s** · TH%d\n%s\n%d war stars", tag, intValue(player["townHallLevel"]), discordLabel(firstDiscordString(stringMapValue(clan, "name"), "No clan")), intValue(player["warStars"]))
	embed := discord.Embed{Title: discordClip(name, 256), Description: description, Color: color, Thumbnail: &discord.EmbedResource{URL: fmt.Sprintf("https://assets.clashk.ing/buildings/home-village/town_hall/level_%d.webp", intValue(player["townHallLevel"]))}}
	if trophies := intValue(player["trophies"]); trophies > 0 {
		embed.Fields = append(embed.Fields, discord.EmbedField{Name: "Trophies", Value: strconv.Itoa(trophies)})
	}
	return discord.MessageCreate{Embeds: []discord.Embed{embed}, Components: []discord.LayoutComponent{discord.NewActionRow(discord.NewLinkButton("Open In-Game", link.String()), discord.NewLinkButton("Clash of Stats", "https://www.clashofstats.com/players/"+strings.TrimPrefix(tag, "#")+"/summary"), discord.NewLinkButton("Clash Ninja", "https://www.clash.ninja/stats-tracker/player/"+strings.TrimPrefix(tag, "#")))}}, nil
}

func (w *discordDeliveryWorker) clanLinkMessage(ctx context.Context, link *url.URL, color int) (discord.MessageCreate, error) {
	tag := normalizeClashLinkTag(link.Query().Get("tag"))
	if tag == "" {
		return discord.MessageCreate{}, errors.New("clan link has no valid tag")
	}
	var clan map[string]any
	if err := w.clashKingAPIGet(ctx, "/v2/clan/"+url.PathEscape(tag)+"/cached", &clan); err != nil {
		return discord.MessageCreate{}, fmt.Errorf("load linked clan: %w", err)
	}
	badge, _ := mapValue(clan["badgeUrls"])
	embed := discord.Embed{Title: discordClip(firstDiscordString(stringMapValue(clan, "name"), "Clan"), 256), Color: color, Description: fmt.Sprintf("**%s** · Level %d · %d/50 members\n%s", tag, intValue(clan["clanLevel"]), intValue(clan["members"]), discordLabel(stringMapValue(clan, "description")))}
	if image := firstDiscordString(stringMapValue(badge, "large"), stringMapValue(badge, "medium")); image != "" {
		embed.Thumbnail = &discord.EmbedResource{URL: image}
	}
	if league, _ := mapValue(clan["warLeague"]); stringMapValue(league, "name") != "" {
		embed.Fields = append(embed.Fields, discord.EmbedField{Name: "CWL League", Value: discordLabel(stringMapValue(league, "name"))})
	}
	return discord.MessageCreate{Embeds: []discord.Embed{embed}, Components: []discord.LayoutComponent{discord.NewActionRow(discord.NewLinkButton("Open In-Game", link.String()), discord.NewLinkButton("Clash of Stats", "https://www.clashofstats.com/clans/"+strings.TrimPrefix(tag, "#")+"/summary"))}}, nil
}

type discordArmyPreview struct {
	ArmyLink     string `json:"armyLink"`
	Presentation struct {
		MinimumTownHall    int  `json:"minimumTownHall"`
		TroopSpace         int  `json:"troopSpace"`
		SpellSpace         int  `json:"spellSpace"`
		ClanCastleSpace    int  `json:"clanCastleSpace"`
		MetadataIncomplete bool `json:"metadataIncomplete"`
		Units              []struct {
			Name     string `json:"name"`
			Quantity int    `json:"quantity"`
			Section  string `json:"section"`
		} `json:"units"`
		Heroes []struct {
			Name      string   `json:"name"`
			Pet       string   `json:"pet"`
			Equipment []string `json:"equipment"`
		} `json:"heroes"`
	} `json:"presentation"`
}

func (w *discordDeliveryWorker) armyLinkMessage(ctx context.Context, link *url.URL, color int) (discord.MessageCreate, error) {
	var result discordArmyPreview
	if err := w.clashKingAPIRequest(ctx, http.MethodPost, "/v2/bot/armies/preview", map[string]string{"army": link.String()}, &result); err != nil {
		return discord.MessageCreate{}, err
	}
	if result.ArmyLink == "" {
		return discord.MessageCreate{}, errors.New("army preview missing copy link")
	}
	// Use the same canonical parser and metadata as /army link; never reimplement
	// compact unit/hero IDs here or save a personal army from a public message.
	_ = w.refreshLogEmojis(time.Now())
	icons := discordLogIcons(w.emojis)
	army := result.Presentation
	embed := discord.Embed{Title: "Army Link", Description: fmt.Sprintf("%sTown Hall %d+", icons.icon(fmt.Sprintf("th%d", army.MinimumTownHall), ""), army.MinimumTownHall), Color: color}
	budget := 5100
	add := func(name string, lines []string) {
		if len(lines) == 0 || budget < 100 {
			return
		}
		value := discordClip(strings.Join(lines, "\n"), min(1000, budget))
		embed.Fields = append(embed.Fields, discord.EmbedField{Name: name, Value: value})
		budget -= len([]rune(value)) + len(name)
	}
	for _, section := range []string{"Troops", "Super Troops", "Spells", "Siege Machines"} {
		var lines []string
		for _, unit := range army.Units {
			if unit.Section == section {
				lines = append(lines, fmt.Sprintf("%s`x%d` %s", icons.icon(unit.Name, ""), unit.Quantity, discordLabel(unit.Name)))
			}
		}
		add(section, lines)
	}
	var heroes []string
	for _, hero := range army.Heroes {
		line := icons.icon(hero.Name, "") + discordLabel(hero.Name)
		if hero.Pet != "" {
			line += "\n- " + icons.icon(hero.Pet, "") + discordLabel(hero.Pet)
		}
		for _, name := range hero.Equipment {
			line += "\n- " + icons.icon(name, "") + discordLabel(name)
		}
		heroes = append(heroes, line)
	}
	add("Heroes Loadout", heroes)
	var castle []string
	for _, unit := range army.Units {
		if unit.Section == "Clan Castle" {
			castle = append(castle, fmt.Sprintf("%s`x%d` %s", icons.icon(unit.Name, ""), unit.Quantity, discordLabel(unit.Name)))
		}
	}
	add("Clan Castle", castle)
	footer := fmt.Sprintf("%d Troop Space | %d Spell Space | %d CC Troop", army.TroopSpace, army.SpellSpace, army.ClanCastleSpace)
	if army.MetadataIncomplete {
		footer = "Partial army · " + footer
	}
	embed.Footer = &discord.EmbedFooter{Text: footer}
	return discord.MessageCreate{Embeds: []discord.Embed{embed}, Components: []discord.LayoutComponent{discord.NewActionRow(discord.NewLinkButton("Copy Army", result.ArmyLink))}}, nil
}

func baseLinkMessage(p discordMessageCreatePayload, link *url.URL, color int) (discord.MessageCreate, error) {
	if strings.TrimSpace(link.Query().Get("id")) == "" {
		return discord.MessageCreate{}, errors.New("base link missing layout ID")
	}
	note := strings.TrimSpace(strings.Replace(p.Content, link.String(), "", 1))
	embed := discord.Embed{Title: "Base Layout", Color: color, Description: discordClip(discordLabel(note), 2000)}
	for _, raw := range p.Attachments {
		a, _ := mapValue(raw)
		image, err := url.Parse(stringMapValue(a, "url"))
		if err == nil && image.Scheme == "https" && (image.Hostname() == "cdn.discordapp.com" || image.Hostname() == "media.discordapp.net") && strings.HasPrefix(stringMapValue(a, "content_type"), "image/") {
			embed.Image = &discord.EmbedResource{URL: image.String()}
			break
		}
	}
	return discord.MessageCreate{Embeds: []discord.Embed{embed}, Components: []discord.LayoutComponent{discord.NewActionRow(discord.NewLinkButton("Copy Base", link.String()))}}, nil
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

func (w *discordDeliveryWorker) deliverShowClans(ctx context.Context, payload discordMessageCreatePayload, queries []string, color int) error {
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
				Color:       color,
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

func (w *discordDeliveryWorker) clashKingAPIGet(ctx context.Context, path string, out any) error {
	return w.clashKingAPIRequest(ctx, http.MethodGet, path, nil, out)
}
func (w *discordDeliveryWorker) clashKingAPIRequest(ctx context.Context, method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(w.app.Config.ClashKingAPIURL, "/")+path, reader)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+w.app.Config.ClashKingAPIToken)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("ClashKing API returned %d", response.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(out)
}
func discordMessagePayload(event mobileWarEvent) discordMessageCreatePayload {
	return discordMessageCreatePayload{ChannelID: stringMapValue(event.Value, "channel_id"), Content: stringMapValue(event.Value, "content"), GuildID: stringMapValue(event.Value, "guild_id"), Attachments: anyList(event.Value["attachments"])}
}

var clashLinkPattern = regexp.MustCompile(`https://link\.clashofclans\.com/[^\s<>"\[\]]+`)

func firstClashLink(content string) *url.URL {
	for _, candidate := range clashLinkPattern.FindAllString(content, -1) {
		parsed, err := url.Parse(strings.TrimRight(candidate, ").,;}"))
		if err == nil && parsed.Scheme == "https" && parsed.User == nil && parsed.Port() == "" && strings.EqualFold(parsed.Hostname(), clashLinkHost) {
			switch parsed.Query().Get("action") {
			case "OpenPlayerProfile", "OpenClanProfile", "CopyArmy", "OpenArmyLink", "OpenLayout":
				return parsed
			}
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
