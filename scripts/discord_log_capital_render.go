package scripts

import (
	"fmt"
	"math"

	"github.com/disgoorg/disgo/discord"
)

func discordCapitalAttackMessages(event mobileWarEvent, icons discordLogIcons) []discord.MessageCreate {
	raid, _ := mapValue(event.Value["raid"])
	previous, _ := mapValue(event.Value["previous_raid"])
	clan, _ := mapValue(event.Value["clan"])
	badge, _ := mapValue(clan["badgeUrls"])
	old := map[string]map[string]any{}
	for _, raw := range anyList(previous["members"]) {
		m, _ := mapValue(raw)
		old[stringMapValue(m, "tag")] = m
	}
	var messages []discord.MessageCreate
	var embeds []discord.Embed
	for _, raw := range anyList(raid["members"]) {
		member, _ := mapValue(raw)
		before := old[stringMapValue(member, "tag")]
		if intValue(member["attacks"]) <= intValue(before["attacks"]) {
			continue
		}
		delta := intValue(member["capitalResourcesLooted"]) - intValue(before["capitalResourcesLooted"])
		if delta < 0 { // A corrected raid snapshot is not a new player's loot.
			continue
		}
		embed := discord.Embed{Description: fmt.Sprintf("%s raided %s%d", discordPlayerLink(member), icons.icon("capital_gold", ""), delta), Color: 0x2ecc71, Author: &discord.EmbedAuthor{Name: stringMapValue(clan, "name"), IconURL: stringMapValue(badge, "large")}, Footer: &discord.EmbedFooter{Text: fmt.Sprintf("%s Total CG | Calc Medals: %d", discordCompactNumber(intValue(raid["capitalTotalLoot"])), discordRaidMedals(raid))}}
		embeds = append(embeds, embed)
		if len(embeds) == 10 {
			messages = append(messages, discord.MessageCreate{Embeds: embeds})
			embeds = nil
		}
	}
	if len(embeds) > 0 {
		messages = append(messages, discord.MessageCreate{Embeds: embeds})
	}
	return messages
}
func discordCompactNumber(n int) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.2fM", float64(n)/1000000)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.2fK", float64(n)/1000)
	}
	return fmt.Sprint(n)
}

// Same per-district medal calculation as the legacy capital log.
func discordRaidMedals(raid map[string]any) int {
	districts := map[int]int{1: 135, 2: 225, 3: 350, 4: 405, 5: 460}
	capitals := map[int]int{2: 180, 3: 360, 4: 585, 5: 810, 6: 1115, 7: 1240, 8: 1260, 9: 1375, 10: 1450}
	medals, attacks := 0, 0
	for _, raw := range anyList(raid["attackLog"]) {
		clan, _ := mapValue(raw)
		attacks += intValue(clan["attackCount"])
		for _, raw := range anyList(clan["districts"]) {
			d, _ := mapValue(raw)
			if intValue(d["destructionPercent"]) != 100 {
				continue
			}
			if intValue(d["id"]) == 70000000 {
				medals += capitals[intValue(d["districtHallLevel"])]
			} else {
				medals += districts[intValue(d["districtHallLevel"])]
			}
		}
	}
	if attacks == 0 {
		return 0
	}
	return int(math.Ceil(float64(medals)/float64(attacks))) * 6
}
