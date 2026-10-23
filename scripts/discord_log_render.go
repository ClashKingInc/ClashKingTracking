package scripts

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/disgoorg/disgo/discord"
)

// Render per destination type: one event can contain several kinds of change.
// Keep the transport independent so fake events exercise the same renderer.
func discordLogMessages(event mobileWarEvent, logType string, catalogs ...map[string]string) []discord.MessageCreate {
	icons := discordLogIcons{}
	if len(catalogs) > 0 {
		icons = catalogs[0]
	}
	clan, _ := mapValue(event.Value["clan"])
	clanName := stringMapValue(clan, "name")
	badge, _ := mapValue(clan["badgeUrls"])
	badgeURL := firstDiscordString(stringMapValue(badge, "large"), stringMapValue(badge, "medium"))
	switch logType {
	case "capital_weekly_summary":
		return discordCapitalWeeklySummaryMessages(event, icons)
	case "clan_achievement_log":
		return discordClanAchievementMessages(event, icons)
	case "legend_log_attacks", "legend_log_defenses":
		return discordLegendLogMessages(event, logType, icons)
	case "capital_attacks":
		return discordCapitalAttackMessages(event, icons)
	case "join_log", "leave_log", "clan_description_log", "clan_requirements_log":
		return discordClanLogMessages(event, logType, icons)
	case "donation_log":
		var donated, received []string
		for _, value := range anyList(event.Value["donations"]) {
			delta, _ := mapValue(value)
			member, _ := mapValue(delta["member"])
			link := discordPlayerLink(member)
			if n := intValue(delta["donated"]); n > 0 {
				donated = append(donated, fmt.Sprintf("%s`%-3d` %s", icons.icon("up_green_arrow", "⬆️"), n, link))
			}
			if n := intValue(delta["received"]); n > 0 {
				received = append(received, fmt.Sprintf("%s`%-3d` %s", icons.icon("down_red_arrow", "⬇️"), n, link))
			}
		}
		embed := discord.Embed{Description: fmt.Sprintf("[**%s**](https://link.clashofclans.com/en?action=OpenClanProfile&tag=%s)", discordLabel(clanName), url.QueryEscape(event.ClanTag))}
		if badgeURL != "" {
			embed.Thumbnail = &discord.EmbedResource{URL: badgeURL}
		}
		for _, group := range []struct {
			name  string
			lines []string
		}{{"Donated", donated}, {"Received", received}} {
			for _, chunk := range discordLineChunks(group.lines, 1000) {
				embed.Fields = append(embed.Fields, discord.EmbedField{Name: group.name, Value: chunk})
			}
		}
		return discordFieldMessages(embed)
	case "reddit_feed":
		data, _ := mapValue(event.Value["data"])
		postURL := stringMapValue(data, "url")
		commentsURL := stringMapValue(data, "comments_link")
		embed := discord.Embed{Title: discordClip(stringMapValue(data, "title"), 256), Color: 0x2ecc71,
			Description: discordClip(stringMapValue(data, "selftext"), 3500) + fmt.Sprintf("\n%d upvotes | [Link](%s) | [Comments](%s)", intValue(data["score"]), postURL, commentsURL)}
		embed.Description = discordClip(embed.Description, 4096)
		message := discord.MessageCreate{Embeds: []discord.Embed{embed}}
		if parsed, err := url.Parse(postURL); err == nil && parsed.Scheme == "https" && parsed.Host != "" && len(postURL) <= 512 {
			message.Components = []discord.LayoutComponent{discord.NewActionRow(discord.NewLinkButton("Post Link", postURL))}
		}
		return []discord.MessageCreate{message}
	case "war_log":
		if stringMapValue(event.Value, "type") == "new_attacks" {
			war, _ := mapValue(event.Value["war"])
			members := map[string]map[string]any{}
			owners := map[string]string{}
			for _, sideName := range []string{"clan", "opponent"} {
				side, _ := mapValue(war[sideName])
				for _, value := range anyList(side["members"]) {
					member, _ := mapValue(value)
					tag := stringMapValue(member, "tag")
					members[tag], owners[tag] = member, stringMapValue(side, "tag")
				}
			}
			var lines []string
			for _, value := range anyList(event.Value["attacks"]) {
				attack, _ := mapValue(value)
				attacker, defender := members[stringMapValue(attack, "attackerTag")], members[stringMapValue(attack, "defenderTag")]
				icon := icons.icon("thick_capital_sword", "⚔️")
				player, other := attacker, defender
				if owners[stringMapValue(attack, "attackerTag")] != event.ClanTag {
					icon = icons.icon("shield", "🛡️")
					player, other = defender, attacker
				}
				stars := min(3, max(0, intValue(attack["stars"])))
				lines = append(lines, fmt.Sprintf("%s %s**%s%s** %s%s **%d%%** %s%s", icon,
					icons.th(intValue(player["townhallLevel"])), discordLabel(stringMapValue(player, "name")), discordSuperscript(intValue(player["mapPosition"])),
					strings.Repeat(icons.icon("war_star", "★"), stars), strings.Repeat(icons.icon("no_star", "☆"), 3-stars), intValue(attack["destructionPercentage"]),
					icons.th(intValue(other["townhallLevel"])), discordSuperscript(intValue(other["mapPosition"]))))
			}
			var messages []discord.MessageCreate
			for _, chunk := range discordLineChunks(lines, 1900) {
				messages = append(messages, discord.MessageCreate{Content: chunk})
			}
			return messages
		}
		return discordWarStateMessages(event, icons)
	case "name_change", "role_change", "th_upgrade", "league_change", "troop_upgrade", "super_troop_boost", "spell_upgrade", "hero_upgrade", "hero_equipment_upgrade", "capital_donations":
		return discordPlayerLogMessages(event, logType, icons)
	case "cwl_lineup_change_log":
		embed := discord.Embed{Title: "CWL lineup changed", Color: 0x3498db}
		for _, group := range []struct{ key, label string }{{"added", "Added"}, {"removed", "Removed"}, {"opponent_added", "Opponent added"}, {"opponent_removed", "Opponent removed"}} {
			var lines []string
			for _, value := range anyList(event.Value[group.key]) {
				member, _ := mapValue(value)
				lines = append(lines, discordPlayerLink(member))
			}
			for _, chunk := range discordLineChunks(lines, 1000) {
				embed.Fields = append(embed.Fields, discord.EmbedField{Name: group.label, Value: chunk})
			}
		}
		return discordFieldMessages(embed)
	}
	return []discord.MessageCreate{{Content: discordEventText(event)}}
}

func discordFieldMessages(base discord.Embed) []discord.MessageCreate {
	var messages []discord.MessageCreate
	embed := base
	embed.Fields = nil
	size := len([]rune(base.Title)) + len([]rune(base.Description))
	for _, field := range base.Fields {
		n := len([]rune(field.Name)) + len([]rune(field.Value))
		if len(embed.Fields) == 25 || size+n > 5500 {
			messages = append(messages, discord.MessageCreate{Embeds: []discord.Embed{embed}})
			embed = base
			embed.Fields = nil
			size = len([]rune(base.Title)) + len([]rune(base.Description))
		}
		embed.Fields = append(embed.Fields, field)
		size += n
	}
	if len(embed.Fields) > 0 {
		messages = append(messages, discord.MessageCreate{Embeds: []discord.Embed{embed}})
	}
	return messages
}

func anyList(value any) []any { result, _ := value.([]any); return result }
func discordLabel(value string) string {
	return strings.NewReplacer("\\", "\\\\", "*", "\\*", "_", "\\_", "[", "\\[", "]", "\\]", "`", "\\`", "\n", " ").Replace(value)
}
func discordPlayerLink(member map[string]any) string {
	return fmt.Sprintf("[**%s**](<https://link.clashofclans.com/en?action=OpenPlayerProfile&tag=%s>)", discordLabel(stringMapValue(member, "name")), url.QueryEscape(stringMapValue(member, "tag")))
}
func discordClip(value string, maximum int) string {
	runes := []rune(value)
	if len(runes) <= maximum {
		return value
	}
	return string(runes[:maximum-1]) + "…"
}
func discordLineChunks(lines []string, maximum int) []string {
	var chunks []string
	current := ""
	for _, line := range lines {
		line = discordClip(line, maximum)
		if len([]rune(current))+len([]rune(line))+1 > maximum && current != "" {
			chunks = append(chunks, current)
			current = ""
		}
		if current != "" {
			current += "\n"
		}
		current += line
	}
	if current != "" {
		chunks = append(chunks, current)
	}
	return chunks
}
