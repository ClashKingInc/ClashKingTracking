package scripts

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/disgoorg/snowflake/v2"
)

// Build one row per eligible account. Unlinked accounts remain visible;
// only users confirmed in this application's guild cache may be mentioned.
func discordReminderMessages(event mobileWarEvent, minutes int, custom string, linked map[string][]snowflake.ID, icons discordLogIcons) []string {
	var members []map[string]any
	if raw, err := json.Marshal(event.Value["members"]); err == nil {
		_ = json.Unmarshal(raw, &members)
	}
	kind := stringMapValue(event.Value, "type")
	war, _ := mapValue(event.Value["data"])
	preparing := kind == "war" && stringMapValue(war, "state") == "preparation"
	if len(members) == 0 && kind == "war" {
		clan, _ := discordWarSides(war, event.ClanTag)
		limit := discordWarAttacksPerMember(event, war)
		for _, raw := range anyList(clan["members"]) {
			member, ok := mapValue(raw)
			if !ok || !preparing && len(anyList(member["attacks"])) >= limit {
				continue
			}
			row := cloneAnyMap(member)
			row["townhall"] = intValue(member["townhallLevel"])
			row["remaining"] = limit - len(anyList(member["attacks"]))
			if preparing {
				row["remaining"] = limit
			}
			members = append(members, row)
		}
	}
	if len(members) == 0 {
		return nil
	}
	sort.SliceStable(members, func(i, j int) bool {
		return intValue(members[i]["townhall"]) > intValue(members[j]["townhall"])
	})
	title := map[string]string{"war": "War attacks", "raid": "Clan Capital", "clan_games": "Clan Games", "roster": "Roster"}[kind]
	if preparing {
		title = "War preparation"
	}
	// Ignore unsupported or retired reminder events still present in the stream.
	if title == "" {
		return nil
	}
	clan, _ := mapValue(event.Value["clan"])
	if kind == "war" && stringMapValue(clan, "name") == "" {
		clan, _ = discordWarSides(war, event.ClanTag)
	}
	name := firstDiscordString(stringMapValue(clan, "name"), event.ClanTag)
	lines := []string{"⏰ **" + title + " reminder — " + discordLabel(name) + "**"}
	if preparing && !discordWarTime(stringMapValue(war, "startTime")).IsZero() {
		lines = append(lines, fmt.Sprintf("War starts <t:%d:R>", discordWarTime(stringMapValue(war, "startTime")).Unix()))
	} else if minutes > 0 {
		lines = append(lines, formatReminderTime(minutes)+" remaining")
	}
	if custom != "" {
		lines = append(lines, discordClip(custom, 1000))
	}
	for _, member := range members {
		tag := stringMapValue(member, "tag")
		if tag == "" {
			continue
		}
		if stringMapValue(member, "name") == "" {
			member["name"] = tag
		}
		detail := ""
		switch kind {
		case "war":
			remaining := intValue(member["remaining"])
			if total := intValue(member["total_attacks"]); total > 0 {
				remaining = total - intValue(member["attacks"])
			}
			detail = fmt.Sprintf(" · %d attacks left", remaining)
			if preparing {
				detail = fmt.Sprintf(" · %d attacks available on battle day", remaining)
			}
		case "raid":
			remaining := intValue(member["remaining_attacks"])
			if total := intValue(member["total_attacks"]); total > 0 {
				remaining = total - intValue(member["attacks"])
			}
			if remaining == 0 {
				remaining = intValue(member["remaining"])
			}
			detail = fmt.Sprintf(" · %d attacks left", remaining)
		case "clan_games":
			detail = fmt.Sprintf(" · %s points", discordCompactNumber(intValue(member["points"])))
		}
		mentions := make([]string, 0, len(linked[tag]))
		for _, user := range linked[tag] {
			mentions = append(mentions, "<@"+user.String()+">")
		}
		if len(mentions) > 0 {
			detail += " · " + strings.Join(mentions, " ")
		}
		lines = append(lines, icons.th(intValue(member["townhall"]))+discordPlayerLink(member)+detail)
	}
	if len(lines) == 0 {
		return nil
	}
	return discordLineChunks(lines, 1900)
}
