package scripts

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

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
		a, b := intValue(members[i]["townhall"]), intValue(members[j]["townhall"])
		if a != b {
			return a > b
		}
		return strings.ToLower(stringMapValue(members[i], "name")) < strings.ToLower(stringMapValue(members[j], "name"))
	})
	// Match the current Bot /reminders layout. Keep configured recipient
	// selection upstream; presentation must not silently reset saved filters.
	clan, _ := mapValue(event.Value["clan"])
	own, opponent := discordWarSides(war, event.ClanTag)
	if kind == "war" {
		clan = own
	}
	name := firstDiscordString(stringMapValue(clan, "name"), event.ClanTag)
	var lines []string
	stamp := func(label string, t time.Time) string {
		if t.IsZero() {
			return fmt.Sprintf("⏰ %s remaining", formatReminderTime(minutes))
		}
		return fmt.Sprintf("⏰ **%s <t:%d:R>**", label, t.Unix())
	}
	switch kind {
	case "war":
		if preparing {
			lines = append(lines, stamp("War starts", discordWarTime(stringMapValue(war, "startTime"))))
		} else {
			lines = append(lines, stamp("War ends", discordWarTime(stringMapValue(war, "endTime"))))
		}
		lines = append(lines, fmt.Sprintf("📌 **%s vs %s**", discordReminderLabel(name), discordReminderLabel(stringMapValue(opponent, "name"))))
		if preparing {
			lines = append(lines, "Prepare for battle day.")
		} else {
			count := func(side map[string]any) int {
				n := 0
				for _, raw := range anyList(side["members"]) {
					m, _ := mapValue(raw)
					n += len(anyList(m["attacks"]))
				}
				return n
			}
			lines = append(lines, fmt.Sprintf("⚔️ %d/%d  ⭐ %d/%d", count(own), count(opponent), intValue(own["stars"]), intValue(opponent["stars"])))
		}
	case "raid":
		raid, _ := mapValue(event.Value["raid"])
		lines = append(lines, "**"+discordReminderLabel(name)+" Raid Weekend**")
		if end := discordWarTime(stringMapValue(raid, "endTime")); !end.IsZero() {
			lines = append(lines, fmt.Sprintf("⏰ Ends <t:%d:R>", end.Unix()))
		}
	case "clan_games":
		at := event.Timestamp.UTC()
		if at.IsZero() {
			at = time.Now().UTC()
		}
		end := time.Date(at.Year(), at.Month(), 28, 8, 0, 0, 0, time.UTC)
		lines = append(lines, "**"+discordReminderLabel(name)+" Clan Games**", fmt.Sprintf("⏰ Ends <t:%d:R>", end.Unix()))
	case "roster":
		lines = append(lines, "⏰ **Roster reminder — "+discordReminderLabel(name)+"**")
	default:
		return nil
	}
	for _, member := range members {
		tag := stringMapValue(member, "tag")
		if tag == "" {
			continue
		}
		if stringMapValue(member, "name") == "" {
			member["name"] = tag
		}
		count := ""
		switch kind {
		case "war":
			total := discordWarAttacksPerMember(event, war)
			if value := intValue(member["total_attacks"]); value > 0 {
				total = value
			}
			remaining := intValue(member["remaining"])
			if _, provided := member["total_attacks"]; provided {
				remaining = total - intValue(member["attacks"])
			}
			if preparing {
				remaining = total
			}
			if remaining <= 0 {
				continue
			}
			count = fmt.Sprintf("(%d/%d) ", remaining, total)
		case "raid":
			total := intValue(member["total_attacks"])
			remaining := total - intValue(member["attacks"])
			if total == 0 {
				remaining = intValue(member["remaining_attacks"])
				if remaining == 0 {
					remaining = intValue(member["remaining"])
				}
				count = fmt.Sprintf("(%d left) ", remaining)
			} else {
				count = fmt.Sprintf("(%d/%d) ", remaining, total)
			}
		case "clan_games":
			count = fmt.Sprintf("(%d/4,000) ", intValue(member["points"]))
		}
		mentions := make([]string, 0, len(linked[tag]))
		for _, user := range linked[tag] {
			mentions = append(mentions, "<@"+user.String()+">")
		}
		identity := tag
		if len(mentions) > 0 {
			identity = strings.Join(mentions, " ")
		}
		hall := intValue(member["townhall"])
		icon := icons.icon(fmt.Sprintf("th%d", hall), fmt.Sprintf("TH%d", hall))
		lines = append(lines, count+icon+" "+discordReminderLabel(stringMapValue(member, "name"))+" | "+identity)
	}
	if custom != "" {
		lines = append(lines, discordClip(custom, 1000))
	}

	if len(lines) == 0 {
		return nil
	}
	return discordLineChunks(lines, 1900)
}

// Same escaping as the Bot command; names are plain text beside the identity.
func discordReminderLabel(value string) string {
	return strings.NewReplacer("\\", "\\\\", "`", "\\`", "*", "\\*", "_", "\\_", "~", "\\~", "|", "\\|", ">", "\\>", "[", "\\[", "]", "\\]", "\n", " ").Replace(value)
}
