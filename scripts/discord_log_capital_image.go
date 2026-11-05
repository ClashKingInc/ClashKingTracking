package scripts

import (
	"bytes"
	_ "embed"
	"fmt"
	"image"
	"image/draw"
	"image/jpeg"
	"image/png"
	"strings"
)

// raidweek.png in the archived Python tree contains JPEG data.
//
//go:embed assets/raidweek.jpg
var discordRaidWeekTemplate []byte

func renderLegacyRaidWeekImage(raid, clan map[string]any) ([]byte, error) {
	background, err := jpeg.Decode(bytes.NewReader(discordRaidWeekTemplate))
	if err != nil {
		return nil, err
	}
	canvas := image.NewRGBA(background.Bounds())
	draw.Draw(canvas, canvas.Bounds(), background, background.Bounds().Min, draw.Src)
	badgeURLs, _ := mapValue(clan["badgeUrls"])
	if badge := discordCardBadge(firstDiscordString(stringMapValue(badgeURLs, "medium"), stringMapValue(badgeURLs, "large"))); badge != nil {
		spot := image.Rect(1125, 135, 1125+badge.Bounds().Dx(), 135+badge.Bounds().Dy())
		draw.Draw(canvas, spot, badge, badge.Bounds().Min, draw.Over)
	}
	add := func(x, y int, size float64, anchor string, stroke int, value string) error {
		return drawDiscordCardText(canvas, value, x, y, size, anchor, stroke, discordWhite)
	}
	name := stringMapValue(clan, "name")
	if err := add(1225, 117, 30, "mm", 2, name); err != nil {
		return nil, err
	}
	offense := intValue(raid["offensiveReward"]) * 6
	if offense == 0 {
		offense = discordRaidMedals(raid)
	}
	defense := intValue(raid["defensiveReward"])
	for _, label := range []struct {
		x, y   int
		size   float64
		anchor string
		stroke int
		text   string
	}{
		{750, 250, 60, "mm", 4, fmt.Sprint(offense + defense)},
		{155, 585, 30, "lm", 2, fmt.Sprint(intValue(raid["capitalTotalLoot"]))},
		{870, 585, 30, "lm", 2, fmt.Sprint(capitalCompletedRaids(anyList(raid["attackLog"])))},
		{155, 817, 30, "lm", 2, fmt.Sprint(intValue(raid["totalAttacks"]))},
		{870, 817, 30, "lm", 2, fmt.Sprint(intValue(raid["enemyDistrictsDestroyed"]))},
		{550, 370, 25, "lm", 2, fmt.Sprint(offense)},
		{1245, 370, 25, "lm", 2, fmt.Sprint(defense)},
	} {
		if err := add(label.x, label.y, label.size, label.anchor, label.stroke, label.text); err != nil {
			return nil, err
		}
	}
	date := stringMapValue(raid, "startTime")
	if parsed, ok := discordClashTime(date); ok {
		date = parsed.Format("2006-01-02")
	} else {
		date = strings.SplitN(date, "T", 2)[0]
	}
	if err := add(25, 35, 30, "lm", 2, date); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := png.Encode(&out, canvas); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func capitalCompletedRaids(logs []any) int {
	completed := 0
	for _, raw := range logs {
		clan, _ := mapValue(raw)
		if intValue(clan["districtsDestroyed"]) == intValue(clan["districtCount"]) && intValue(clan["districtCount"]) > 0 {
			completed++
		}
	}
	return completed
}
