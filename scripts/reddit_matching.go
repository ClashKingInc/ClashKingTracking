package scripts

import (
	"context"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/snowflake/v2"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// Only explicit title levels qualify. A '+' after TH10 never invents a TH18.
// Lists and ranges share a prefix: TH14/15/16 and Town Hall 14-16.
var redditTitleTH = regexp.MustCompile(`(?i)(?:\bt[.\s]*h[.]?|\btown\s*hall)\s*(?:level|lvl)?[\s:._-]*([0-9]{1,2})(\s*(?:[-–/,]|\band\b)\s*(?:(?:th|town\s*hall)\s*)?[0-9]{1,2})*\b`)
var redditNumbers = regexp.MustCompile(`[0-9]+`)
var redditPlayerTag = regexp.MustCompile(`(?i)(?:#|\b)[0289PYLQGRJCUV]{5,15}\b`)

func redditTitleLevel(title string) int {
	title = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, title)
	highest := 0
	for _, match := range redditTitleTH.FindAllString(title, -1) {
		for _, number := range redditNumbers.FindAllString(match, -1) {
			n, _ := strconv.Atoi(number)
			if n > highest && n <= 18 {
				highest = n
			}
		}
	}
	return highest
}

// Resolve once per event rather than once per destination. Bad/deleted tags are
// not proof of a Town Hall and must not block later valid tags or title matches.
func (w *discordDeliveryWorker) redditPlayers(ctx context.Context, data map[string]any) []map[string]any {
	if w.app.Clash == nil {
		return nil
	}
	seen := map[string]bool{}
	result := []map[string]any{}
	text := stringMapValue(data, "title") + " " + stringMapValue(data, "selftext")
	for _, raw := range redditPlayerTag.FindAllString(text, -1) {
		tag := "#" + strings.TrimPrefix(strings.ToUpper(raw), "#")
		if seen[tag] {
			continue
		}
		seen[tag] = true
		if len(seen) > 10 {
			break
		}
		player, err := w.app.Clash.GetPlayer(ctx, tag)
		if err != nil || player == nil {
			continue
		}
		result = append(result, map[string]any{"tag": player.Tag, "name": player.Name, "townHallLevel": player.TownHall})
	}
	return result
}

func redditButtonEmoji(value string) discord.ComponentEmoji {
	if strings.HasPrefix(value, "<") {
		parts := strings.Split(strings.Trim(value, "<>"), ":")
		if len(parts) == 3 {
			if id, err := snowflake.Parse(parts[2]); err == nil {
				return discord.ComponentEmoji{ID: id, Name: parts[1], Animated: parts[0] == "a"}
			}
		}
	}
	return discord.NewComponentEmoji(value)
}
