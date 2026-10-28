package scripts

import (
	"bytes"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
)

// A completed raid can arrive before Supercell fills in the medal rewards.
// Send its one weekly summary when those rewards first become available.
func discordCapitalSummaryReady(previous, current map[string]any) bool {
	if stringMapValue(current, "state") != "ended" || stringMapValue(previous, "state") == "" {
		return false
	}
	if stringMapValue(previous, "startTime") != stringMapValue(current, "startTime") {
		return false
	}
	return raidRewardsReady(current) && !raidRewardsReady(previous)
}

func raidRewardsReady(raid map[string]any) bool {
	return stringMapValue(raid, "state") == "ended" &&
		(intValue(raid["offensiveReward"]) > 0 || intValue(raid["defensiveReward"]) > 0 ||
			(intValue(raid["totalAttacks"]) == 0 && intValue(raid["capitalTotalLoot"]) == 0))
}

func discordCapitalWeeklySummaryMessages(event mobileWarEvent, icons discordLogIcons) []discord.MessageCreate {
	raid, _ := mapValue(event.Value["raid"])
	previous, _ := mapValue(event.Value["previous_raid"])
	if !discordCapitalSummaryReady(previous, raid) {
		return nil
	}
	clan, _ := mapValue(event.Value["clan"])
	badge, _ := mapValue(clan["badgeUrls"])
	name := stringMapValue(clan, "name")
	if name == "" {
		name = event.ClanTag
	}
	start, startOK := discordClashTime(stringMapValue(raid, "startTime"))
	end, endOK := discordClashTime(stringMapValue(raid, "endTime"))
	footer := "Raid Weekend"
	if startOK && endOK {
		footer = start.Format("2006/01/02") + "-" + end.Format("2006/01/02")
	}
	embed := discord.Embed{Title: discordClip(name+" Raid Weekend Overview", 256), Color: 0x2ecc71,
		Footer: &discord.EmbedFooter{Text: footer, IconURL: stringMapValue(badge, "large")}}
	var attacks int
	for _, raw := range anyList(raid["members"]) {
		member, _ := mapValue(raw)
		attacks += intValue(member["attacks"])
	}
	timeLine := ""
	if startOK && endOK {
		timeLine = fmt.Sprintf("\n- Start <t:%d:R>, End <t:%d:R>", start.Unix(), end.Unix())
	}
	add := func(label, value string, inline bool) {
		if value != "" {
			embed.Fields = append(embed.Fields, discord.EmbedField{Name: label, Value: discordClip(value, 1024), Inline: &inline})
		}
	}
	add("Overview", fmt.Sprintf("- %s%s Looted\n- %s%d Districts Destroyed\n- %s%d/300 Attacks Complete\n- %s%d/50 Participants%s",
		icons.icon("capital_gold", "🪙"), discordCommaNumber(intValue(raid["capitalTotalLoot"])), icons.icon("District_Hall5", "🏛️"), intValue(raid["enemyDistrictsDestroyed"]),
		icons.icon("thick_capital_sword", "⚔️"), attacks, icons.icon("people", "👥"), len(anyList(raid["members"])), timeLine), false)
	attackLog, defenseLog := anyList(raid["attackLog"]), anyList(raid["defenseLog"])
	loot := intValue(raid["capitalTotalLoot"])
	add("Detailed Stats (Offense)", fmt.Sprintf("- Avg Loot/Raid : %s\n- Avg Loot/Player: %s\n- Avg Hits/Clan: %.2f",
		discordCompactNumber(raidDivide(loot, len(attackLog))), discordCompactNumber(raidDivide(loot, len(anyList(raid["members"])))),
		float64(intValue(raid["totalAttacks"]))/float64(max(1, len(attackLog)))), false)
	add("(Avg Atks By District, Off)", capitalDistrictAverage(attackLog), true)
	add("(Avg Atks By District, Def)", capitalDistrictAverage(defenseLog), true)
	add("Attack Summary", capitalOpponentsSummary(attackLog, true), false)
	add("Defense Summary", capitalOpponentsSummary(defenseLog, false), false)
	add("Top 3 Raiders", capitalTopRaiders(anyList(raid["members"]), icons), false)
	message := discord.MessageCreate{Embeds: []discord.Embed{embed}}
	if png, err := renderLegacyRaidWeekImage(raid, clan); err == nil && len(png) > 0 {
		message.Files = []*discord.File{discord.NewFile("raid-weekend.png", "Raid weekend results", bytes.NewReader(png))}
		message.Embeds[0].Image = &discord.EmbedResource{URL: "attachment://raid-weekend.png"}
	} else {
		message.Embeds[0].Description = "Raid result image unavailable."
	}
	return []discord.MessageCreate{message}
}

func raidDivide(a, b int) int {
	if b == 0 {
		return 0
	}
	return a / b
}

func discordClashTime(s string) (time.Time, bool) {
	for _, layout := range []string{"20060102T150405.000Z", time.RFC3339, "20060102T150405Z"} {
		if value, err := time.Parse(layout, s); err == nil {
			return value, true
		}
	}
	return time.Time{}, false
}

func capitalDistrictAverage(logs []any) string {
	type stats struct{ sum, n int }
	order := []string{}
	counts := map[string]stats{}
	for _, raw := range logs {
		clan, _ := mapValue(raw)
		for _, dRaw := range anyList(clan["districts"]) {
			district, _ := mapValue(dRaw)
			name := stringMapValue(district, "name")
			if _, exists := counts[name]; !exists {
				order = append(order, name)
			}
			v := counts[name]
			v.sum += len(anyList(district["attacks"]))
			v.n++
			counts[name] = v
		}
	}
	var lines []string
	for _, name := range order {
		v := counts[name]
		lines = append(lines, fmt.Sprintf("%-4.2f %s", math.Round(float64(v.sum)/float64(v.n)*100)/100, name))
	}
	if len(lines) == 0 {
		return "—"
	}
	return "```\n" + discordClip(strings.Join(lines, "\n"), 1015) + "\n```"
}

func capitalOpponentsSummary(logs []any, offense bool) string {
	lines := []string{}
	if offense {
		lines = append(lines, "a=atks, d=districts, l=loot")
	}
	for _, raw := range logs {
		clan, _ := mapValue(raw)
		name := "Unknown"
		key := "attacker"
		if offense {
			key = "defender"
		}
		if other, ok := mapValue(clan[key]); ok {
			name = stringMapValue(other, "name")
		}
		if name == "" {
			name = "Unknown"
		}
		var loot int
		for _, dRaw := range anyList(clan["districts"]) {
			d, _ := mapValue(dRaw)
			loot += intValue(d["totalLooted"])
		}
		lines = append(lines, fmt.Sprintf("- %-12s %2da %dd %sl", discordClip(name, 12), intValue(clan["attackCount"]), intValue(clan["districtsDestroyed"]), discordCompactNumber(loot)))
	}
	if len(lines) == 0 {
		return "—"
	}
	return "```\n" + discordClip(strings.Join(lines, "\n"), 1015) + "\n```"
}

func capitalTopRaiders(raw []any, icons discordLogIcons) string {
	members := make([]map[string]any, 0, len(raw))
	for _, v := range raw {
		if member, ok := mapValue(v); ok {
			members = append(members, member)
		}
	}
	sort.SliceStable(members, func(i, j int) bool {
		return intValue(members[i]["capitalResourcesLooted"]) > intValue(members[j]["capitalResourcesLooted"])
	})
	var lines []string
	for i, member := range members[:min(3, len(members))] {
		ordinal := icons.icon([]string{"gold_1", "white_2", "blue_3"}[i], []string{"🥇", "🥈", "🥉"}[i])
		lines = append(lines, fmt.Sprintf("%s%s %s%d%s", ordinal, discordLabel(stringMapValue(member, "name")), icons.icon("capital_gold", "🪙"), intValue(member["capitalResourcesLooted"]), discordSuperscript(intValue(member["attacks"]))))
	}
	if len(lines) == 0 {
		return "—"
	}
	return strings.Join(lines, "\n")
}

func discordCommaNumber(value int) string {
	s := fmt.Sprint(value)
	if len(s) <= 3 {
		return s
	}
	var chunks []string
	for len(s) > 3 {
		chunks = append(chunks, s[len(s)-3:])
		s = s[:len(s)-3]
	}
	chunks = append(chunks, s)
	slices.Reverse(chunks)
	return strings.Join(chunks, ",")
}
