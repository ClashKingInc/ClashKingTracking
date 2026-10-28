package scripts

import (
	"fmt"
	"github.com/disgoorg/disgo/discord"
	"strings"
)

func discordClanLogMessages(event mobileWarEvent, logType string, icons discordLogIcons) []discord.MessageCreate {
	clan, _ := mapValue(event.Value["clan"])
	badge, _ := mapValue(clan["badgeUrls"])
	embed := discord.Embed{Color: 0x2ecc71, Footer: &discord.EmbedFooter{Text: stringMapValue(clan, "name"), IconURL: stringMapValue(badge, "large")}}
	previous, _ := mapValue(event.Value["previous_clan"])
	switch logType {
	case "join_log", "leave_log":
		member, _ := mapValue(event.Value["member"])
		player, _ := mapValue(event.Value["player"])
		if player == nil {
			player = member
		}
		level := intValue(player["townHallLevel"])
		league, _ := mapValue(player["league"])
		embed.Description = fmt.Sprintf("[**%s** (%s)](https://link.clashofclans.com/en?action=OpenPlayerProfile&tag=%%23%s)\n", discordLabel(stringMapValue(player, "name")), stringMapValue(player, "tag"), strings.TrimPrefix(stringMapValue(player, "tag"), "#"))
		stats := fmt.Sprintf("%s%d%s%d", icons.th(level), level, icons.icon(stringMapValue(league, "name"), icons.icon("trophy", "🏆")), intValue(player["trophies"]))
		if logType == "join_log" {
			stats += fmt.Sprintf("%s%d", icons.icon("war_star", "★"), intValue(player["warStars"]))
			for _, raw := range anyList(player["heroes"]) {
				hero, _ := mapValue(raw)
				if stringMapValue(hero, "village") == "builderBase" {
					continue
				}
				stats += fmt.Sprintf("%s%d", icons.icon(stringMapValue(hero, "name"), stringMapValue(hero, "name")+" "), intValue(hero["level"]))
			}
			embed.Description += "**" + stats + "**"
			embed.Footer.Text = fmt.Sprintf("Joined %s [%d/50]", stringMapValue(clan, "name"), intValue(clan["members"]))
		} else {
			embed.Color = 0xe74c3c
			role := map[string]string{"member": "Member", "admin": "Elder", "coLeader": "Co-Leader", "leader": "Leader"}[stringMapValue(member, "role")]
			embed.Description += fmt.Sprintf("**%s(#%d)%s%d%s%d%s%s**", stats, intValue(member["clanRank"]), icons.icon("up_green_arrow", "⬆️"), intValue(member["donations"]), icons.icon("down_red_arrow", "⬇️"), intValue(member["donationsReceived"]), icons.icon("pin", "📌"), role)
			embed.Footer.Text = fmt.Sprintf("Left %s [%d/50]", stringMapValue(clan, "name"), intValue(clan["members"]))
			newClan, _ := mapValue(player["clan"])
			if tag := stringMapValue(newClan, "tag"); tag != "" && tag != event.ClanTag {
				embed.Footer.Text += " and Joined " + stringMapValue(newClan, "name")
				b, _ := mapValue(newClan["badgeUrls"])
				embed.Footer.IconURL = stringMapValue(b, "large")
			}
		}
	case "clan_description_log":
		embed.Title = "Clan description changed"
		embed.Fields = []discord.EmbedField{{Name: "Before", Value: discordNonempty(stringMapValue(previous, "description"))}, {Name: "After", Value: discordNonempty(stringMapValue(clan, "description"))}}
	case "clan_requirements_log":
		embed.Title = "Clan requirements changed"
		for _, field := range []struct{ key, label string }{{"requiredTrophies", "Trophies"}, {"requiredBuilderBaseTrophies", "Builder Base trophies"}, {"requiredTownhallLevel", "Town Hall"}, {"type", "Clan type"}} {
			a, b := previous[field.key], clan[field.key]
			if fmt.Sprint(a) != fmt.Sprint(b) {
				embed.Fields = append(embed.Fields, discord.EmbedField{Name: field.label, Value: fmt.Sprintf("%v → %v", a, b)})
			}
		}
	}
	return []discord.MessageCreate{{Embeds: []discord.Embed{embed}}}
}
func discordNonempty(value string) string {
	if value == "" {
		return "—"
	}
	return discordClip(value, 1024)
}
