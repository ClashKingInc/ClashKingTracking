package scripts

import (
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/disgoorg/disgo/discord"
	"strings"
	"unicode"
)

type discordLogIcons map[string]string

func discordEmojiKey(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, value)
}
func (i discordLogIcons) icon(name, fallback string) string {
	if v := i[discordEmojiKey(name)]; v != "" {
		return v
	}
	return fallback
}
func (i discordLogIcons) th(level int) string {
	return i.icon(fmt.Sprintf("th%d", level), fmt.Sprintf("TH%d ", level))
}
func discordSuperscript(n int) string {
	return strings.Map(func(r rune) rune { return []rune("⁰¹²³⁴⁵⁶⁷⁸⁹")[r-'0'] }, fmt.Sprint(max(0, n)))
}

func discordPlayerLogMessages(event mobileWarEvent, logType string, icons discordLogIcons) []discord.MessageCreate {
	player := map[string]any{"name": event.Value["name"], "tag": event.Value["tag"]}
	level := intValue(event.Value["town_hall_level"])
	if member, ok := mapValue(event.Value["member"]); ok {
		player = member
		level = intValue(member["townHallLevel"])
	}
	prefix := icons.th(level) + discordPlayerLink(player)
	var lines []string
	changes := anyList(event.Value["changes"])
	if logType == "role_change" && event.Topic == "clan" {
		changes = []any{map[string]any{"type": "role", "previous": event.Value["previous_role"], "current": event.Value["current_role"]}}
	}
	wanted := map[string]string{"name_change": "name", "role_change": "role", "th_upgrade": "townHallLevel", "league_change": "leagueTier", "troop_upgrade": "troops", "super_troop_boost": "troops", "spell_upgrade": "spells", "hero_upgrade": "heroes", "hero_equipment_upgrade": "heroEquipment", "capital_donations": "clanCapitalContributions"}[logType]
	for _, raw := range changes {
		change, _ := mapValue(raw)
		if stringMapValue(change, "type") != wanted {
			continue
		}
		old, now := change["previous"], change["current"]
		switch logType {
		case "name_change":
			oldPlayer := map[string]any{"name": old, "tag": player["tag"]}
			lines = append(lines, icons.th(level)+discordPlayerLink(oldPlayer)+" changed their name to "+discordLabel(fmt.Sprint(now)))
		case "role_change":
			names := map[string]string{"member": "Member", "admin": "Elder", "coLeader": "Co-Leader", "leader": "Leader"}
			ranks := map[string]int{"member": 0, "admin": 1, "coLeader": 2, "leader": 3}
			a, b := fmt.Sprint(old), fmt.Sprint(now)
			direction := "demoted"
			if ranks[b] > ranks[a] {
				direction = "promoted"
			}
			lines = append(lines, fmt.Sprintf("%s was %s from %s to %s", prefix, direction, names[a], names[b]))
		case "th_upgrade":
			lines = append(lines, fmt.Sprintf("%s upgraded to %sTownhall %d", discordPlayerLink(player), icons.th(intValue(now)), intValue(now)))
		case "league_change":
			a, _ := mapValue(old)
			b, _ := mapValue(now)
			from, to := stringMapValue(a, "name"), stringMapValue(b, "name")
			lines = append(lines, fmt.Sprintf("%s moved from %s%s to %s%s", prefix, icons.icon(from, ""), discordLabel(from), icons.icon(to, ""), discordLabel(to)))
		case "capital_donations":
			delta := intValue(now) - intValue(old)
			if delta <= 0 {
				continue
			}
			clan, _ := mapValue(event.Value["clan"])
			badge, _ := mapValue(clan["badgeUrls"])
			return []discord.MessageCreate{{Embeds: []discord.Embed{{Description: fmt.Sprintf("%s donated %s%d", discordPlayerLink(player), icons.icon("capital_gold", ""), delta), Color: 0x2ecc71, Footer: &discord.EmbedFooter{Text: stringMapValue(clan, "name"), IconURL: stringMapValue(badge, "large")}}}}}
		default:
			previous := discordTroops(old)
			current := discordTroops(now)
			// Iterate the producer's order rather than Go map order, so batched upgrades are stable.
			for _, entry := range anyList(now) {
				unit, _ := mapValue(entry)
				name := stringMapValue(unit, "name")
				key := name + "/" + stringMapValue(unit, "village")
				a, b := previous[key], current[key]
				super := clashy.Troop{Name: name}.IsSuperTroop()
				verb := ""
				if logType == "super_troop_boost" {
					if !super || b["superTroopIsActive"] != true || a["superTroopIsActive"] == true {
						continue
					}
					verb = "boosted"
				} else {
					if super {
						continue
					}
					if a == nil {
						verb = "unlocked"
					} else if intValue(b["level"]) > intValue(a["level"]) {
						verb = "leveled up"
					} else {
						continue
					}
				}
				line := fmt.Sprintf("%s %s %s%s", prefix, verb, icons.icon(name, ""), discordLabel(name))
				if verb == "leveled up" {
					line += fmt.Sprintf(" to lv%d", intValue(b["level"]))
				}
				lines = append(lines, line)
			}
		}
	}
	var out []discord.MessageCreate
	for _, chunk := range discordLineChunks(lines, 1900) {
		out = append(out, discord.MessageCreate{Content: chunk})
	}
	return out
}
