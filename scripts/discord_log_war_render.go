package scripts

import (
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
)

// discordWarIdentity is stable across every snapshot of one war. A panel is
// edited while this value matches server_logs.active_war_id.
func discordWarIdentity(event mobileWarEvent) string {
	war, _ := mapValue(event.Value["war"])
	clan, opponent := discordWarSides(war, event.ClanTag)
	prep := discordWarTime(stringMapValue(war, "preparationStartTime"))
	if prep.IsZero() || stringMapValue(clan, "tag") == "" || stringMapValue(opponent, "tag") == "" {
		return ""
	}
	return fmt.Sprintf("%sv%s-%d", stringMapValue(clan, "tag"), stringMapValue(opponent, "tag"), prep.Unix())
}

func discordWarSides(war map[string]any, trackedTag string) (map[string]any, map[string]any) {
	clan, _ := mapValue(war["clan"])
	opponent, _ := mapValue(war["opponent"])
	if stringMapValue(opponent, "tag") == trackedTag {
		return opponent, clan
	}
	return clan, opponent
}

func discordWarTime(raw string) time.Time {
	for _, layout := range []string{"20060102T150405.000Z", "20060102T150405Z", time.RFC3339Nano} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed
		}
	}
	return time.Time{}
}

func floatValue(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case json.Number:
		result, _ := typed.Float64()
		return result
	case string:
		result, _ := strconv.ParseFloat(typed, 64)
		return result
	default:
		return 0
	}
}

func discordWarBadge(side map[string]any) *discord.EmbedResource {
	badge, _ := mapValue(side["badgeUrls"])
	value := firstDiscordString(stringMapValue(badge, "large"), stringMapValue(badge, "medium"), stringMapValue(badge, "small"))
	if value == "" {
		return nil
	}
	return &discord.EmbedResource{URL: value}
}

func discordWarLink(side map[string]any) string {
	return fmt.Sprintf("[**%s**](https://link.clashofclans.com/en?action=OpenClanProfile&tag=%s)", discordLabel(stringMapValue(side, "name")), url.QueryEscape(stringMapValue(side, "tag")))
}

func discordWarType(event mobileWarEvent, war map[string]any) string {
	value := firstDiscordString(stringMapValue(event.Value, "war_type"), stringMapValue(war, "type"))
	if value == "" || value == "random" {
		return "Regular"
	}
	return strings.ToUpper(value[:1]) + value[1:]
}

func discordWarAttacksPerMember(event mobileWarEvent, war map[string]any) int {
	// CWL raw responses can omit attacksPerMember. The event still carries
	// the war type, so never apply the regular-war fallback to a CWL round.
	if strings.EqualFold(discordWarType(event, war), "cwl") || stringMapValue(event.Value, "war_tag") != "" {
		return 1
	}
	if allowed := intValue(war["attacksPerMember"]); allowed > 0 {
		return allowed
	}
	return 2
}

// discordWarPanelMessage reproduces the legacy main war page and gives every
// control a war identity. The Worker can reject stale component clicks.
func discordWarPanelMessage(event mobileWarEvent, icons discordLogIcons) (discord.MessageCreate, bool) {
	war, ok := mapValue(event.Value["war"])
	if !ok || event.Value["panel_target"] == false {
		return discord.MessageCreate{}, false
	}
	identity := discordWarIdentity(event)
	if identity == "" {
		return discord.MessageCreate{}, false
	}
	clan, opponent := discordWarSides(war, event.ClanTag)
	state := stringMapValue(war, "state")
	start, end := discordWarTime(stringMapValue(war, "startTime")), discordWarTime(stringMapValue(war, "endTime"))
	stateText, verb, at, color := "In Prep", "Starting", start, 0xf1c40f
	if state == "inWar" {
		stateText, verb, at, color = "In War", "Ending", end, 0x2ecc71
	} else if state == "warEnded" {
		stateText, verb, at, color = "War Over", "Ended", end, 0xe74c3c
	}
	clanName, opponentName := stringMapValue(clan, "name"), stringMapValue(opponent, "name")
	warKind := discordWarType(event, war)
	timeline := fmt.Sprintf("https://api.clashk.ing/timeline/%s", url.PathEscape(event.ClanTag))
	when := "Time unavailable"
	if !at.IsZero() {
		when = fmt.Sprintf("%s: <t:%d:R>", verb, at.Unix())
	}
	stateValue := fmt.Sprintf("%s (%d vs %d)\n%s\n[*View War Timeline*](%s)", stateText, intValue(war["teamSize"]), intValue(war["teamSize"]), when, timeline)
	if warKind == "Cwl" {
		if league := stringMapValue(event.Value, "war_league"); league != "" {
			stateValue = league + "\n" + stateValue
		}
	}
	maxHits := intValue(war["teamSize"]) * discordWarAttacksPerMember(event, war)
	clanHits, opponentHits := discordWarAttackCounts(clan), discordWarAttackCounts(opponent)
	stats := fmt.Sprintf("`%-7s`%s`%7s`\n`%-7d`%s`%7d`\n`%-7s`%s`%7s`",
		fmt.Sprintf("%d/%d", clanHits, maxHits), icons.icon("animated_clash_swords", "⚔️"), fmt.Sprintf("%d/%d", opponentHits, maxHits),
		intValue(clan["stars"]), icons.icon("war_star", "★"), intValue(opponent["stars"]),
		fmt.Sprintf("%.2f%%", floatValue(clan["destructionPercentage"])), icons.icon("broken_sword", "🗡️"), fmt.Sprintf("%.2f%%", floatValue(opponent["destructionPercentage"])))
	embed := discord.Embed{Description: discordWarLink(clan), Color: color, Thumbnail: discordWarBadge(clan), Footer: &discord.EmbedFooter{Text: warKind + " War"},
		Fields: []discord.EmbedField{
			{Name: "War Against", Value: discordWarLink(opponent) + " (" + discordLabel(stringMapValue(opponent, "tag")) + ")"},
			{Name: "War State", Value: stateValue},
			{Name: "War Stats", Value: stats},
			{Name: "War Composition", Value: discordClip(clanName+"\n"+discordWarComposition(clan, icons)+"\n"+opponentName+"\n"+discordWarComposition(opponent, icons), 1024)},
		}}
	if recent := discordWarRecentAttacks(clan, opponent, war, icons); recent != "" {
		embed.Fields = append(embed.Fields, discord.EmbedField{Name: "Last 5 attacks/defenses", Value: recent})
	}
	if !event.Timestamp.IsZero() {
		embed.Timestamp = &event.Timestamp
	}
	base := fmt.Sprintf("ck:war:%%s:%s:%d", strings.TrimPrefix(event.ClanTag, "#"), discordWarTime(stringMapValue(war, "preparationStartTime")).Unix())
	buttons := discord.NewActionRow(
		discord.NewSecondaryButton("Refresh", fmt.Sprintf(base, "refresh")),
		discord.NewSecondaryButton("Attacks", fmt.Sprintf(base, "attacks")),
		discord.NewSecondaryButton("Defenses", fmt.Sprintf(base, "defenses")),
		discord.NewSuccessButton("Opponent", fmt.Sprintf(base, "opponent")),
	)
	return discord.MessageCreate{Embeds: []discord.Embed{embed}, Components: []discord.LayoutComponent{buttons}}, true
}

func discordWarAttackCounts(side map[string]any) int {
	count := 0
	for _, value := range anyList(side["members"]) {
		member, _ := mapValue(value)
		count += len(anyList(member["attacks"]))
	}
	return count
}

func discordWarComposition(side map[string]any, icons discordLogIcons) string {
	counts := map[int]int{}
	for _, value := range anyList(side["members"]) {
		member, _ := mapValue(value)
		counts[intValue(member["townhallLevel"])]++
	}
	levels := make([]int, 0, len(counts))
	for level := range counts {
		levels = append(levels, level)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(levels)))
	var parts []string
	for _, level := range levels {
		parts = append(parts, fmt.Sprintf("%s`%d`", icons.th(level), counts[level]))
	}
	return strings.Join(parts, " ")
}

func discordWarRecentAttacks(clan, opponent, war map[string]any, icons discordLogIcons) string {
	type entry struct {
		order int
		line  string
	}
	var entries []entry
	for _, side := range []map[string]any{clan, opponent} {
		friendly := stringMapValue(side, "tag") == stringMapValue(clan, "tag")
		for _, value := range anyList(side["members"]) {
			member, _ := mapValue(value)
			for _, raw := range anyList(member["attacks"]) {
				attack, _ := mapValue(raw)
				icon := icons.icon("clash_sword", "⚔️")
				if !friendly {
					icon = icons.icon("shield", "🛡️")
				}
				stars := min(3, max(0, intValue(attack["stars"])))
				entries = append(entries, entry{order: intValue(attack["order"]), line: fmt.Sprintf("%s`%3d%%`%s%s`%s`", icon, intValue(attack["destructionPercentage"]), strings.Repeat(icons.icon("war_star", "★"), stars), strings.Repeat(icons.icon("no_star", "☆"), 3-stars), discordLabel(stringMapValue(member, "name")))})
			}
		}
	}
	sort.SliceStable(entries, func(i, j int) bool { return entries[i].order > entries[j].order })
	var lines []string
	for _, item := range entries[:min(5, len(entries))] {
		lines = append(lines, item.line)
	}
	return discordClip(strings.Join(lines, "\n"), 1024)
}

func discordWarStateMessages(event mobileWarEvent, icons discordLogIcons) []discord.MessageCreate {
	war, ok := mapValue(event.Value["war"])
	if !ok || (stringMapValue(event.Value, "type") != "new_war" && stringMapValue(event.Value, "type") != "war_state") {
		return nil
	}
	clan, opponent := discordWarSides(war, event.ClanTag)
	state := stringMapValue(war, "state")
	if state == "preparation" || state == "warEnded" {
		// A CWL preparation war is announced in war_log even while the panel
		// remains on the active battle. Panel targeting is a delivery choice.
		announcement := event
		announcement.Value = make(map[string]any, len(event.Value)+1)
		for key, value := range event.Value {
			announcement.Value[key] = value
		}
		announcement.Value["panel_target"] = true
		panel, valid := discordWarPanelMessage(announcement, icons)
		if !valid {
			return nil
		}
		panel.Components = nil
		return []discord.MessageCreate{panel}
	}
	if state == "inWar" {
		embed := discord.Embed{Description: discordWarLink(clan), Color: 0xf1c40f, Thumbnail: discordWarBadge(clan), Footer: &discord.EmbedFooter{Text: discordWarType(event, war) + " War"}, Fields: []discord.EmbedField{{Name: "War Started Against", Value: discordWarLink(opponent)}}}
		return []discord.MessageCreate{{Embeds: []discord.Embed{embed}}}
	}
	return nil
}

func discordWarMissedHitsMessages(event mobileWarEvent, icons discordLogIcons) []discord.MessageCreate {
	war, ok := mapValue(event.Value["war"])
	if !ok {
		return nil
	}
	clan, opponent := discordWarSides(war, event.ClanTag)
	allowed := discordWarAttacksPerMember(event, war)
	var one, multiple []string
	for _, value := range anyList(clan["members"]) {
		member, _ := mapValue(value)
		missed := allowed - len(anyList(member["attacks"]))
		if missed <= 0 {
			continue
		}
		line := icons.th(intValue(member["townhallLevel"])) + discordLabel(stringMapValue(member, "name"))
		if missed == 1 {
			one = append(one, line)
		} else {
			multiple = append(multiple, line)
		}
	}
	if len(one)+len(multiple) == 0 {
		return nil
	}
	embed := discord.Embed{Title: discordClip(stringMapValue(clan, "name")+" vs "+stringMapValue(opponent, "name"), 256), Description: "Missed Hits", Color: 0xe67e22, Thumbnail: discordWarBadge(clan)}
	for _, group := range []struct {
		name  string
		lines []string
	}{{"One Hit Missed", one}, {"Two Hits Missed", multiple}} {
		for _, chunk := range discordLineChunks(group.lines, 1024) {
			embed.Fields = append(embed.Fields, discord.EmbedField{Name: group.name, Value: chunk})
		}
	}
	return discordFieldMessages(embed)
}
