package scripts

import (
	"bytes"
	_ "embed"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"
	"math"

	"github.com/disgoorg/disgo/discord"
	xdraw "golang.org/x/image/draw"
)

// The original archived Pillow generator uses this JPEG despite its .png name.
//
//go:embed assets/warbkpng.jpg
var discordWarResultTemplate []byte

var discordWarHallXP = map[int]int{1: 1, 2: 1, 3: 1, 4: 2, 5: 2, 6: 2, 7: 3, 8: 4, 9: 5, 10: 7, 11: 7, 12: 10, 13: 11, 14: 11, 15: 12, 16: 13, 17: 14, 18: 15}

// discordWarResultImage uses the original 2072x1346 background, typography,
// coordinates, and XP formula. It returns a 725x471 PNG like the legacy send.
func discordWarResultImage(event mobileWarEvent) (discord.MessageCreate, error) {
	war, ok := mapValue(event.Value["war"])
	if !ok {
		return discord.MessageCreate{}, fmt.Errorf("war result missing war")
	}
	clan, opponent := discordWarSides(war, event.ClanTag)
	background, err := jpeg.Decode(bytes.NewReader(discordWarResultTemplate))
	if err != nil {
		return discord.MessageCreate{}, err
	}
	canvas := image.NewRGBA(background.Bounds())
	draw.Draw(canvas, canvas.Bounds(), background, background.Bounds().Min, draw.Src)
	for index, side := range []map[string]any{clan, opponent} {
		badgeURLs, _ := mapValue(side["badgeUrls"])
		badge := discordCardBadge(firstDiscordString(stringMapValue(badgeURLs, "medium"), stringMapValue(badgeURLs, "large")))
		if badge != nil {
			x := 850
			if index == 1 {
				x = 1050
			}
			rect := badge.Bounds().Add(image.Pt(x-badge.Bounds().Min.X, 50-badge.Bounds().Min.Y))
			draw.Draw(canvas, rect, badge, badge.Bounds().Min, draw.Over)
		}
	}
	result, winXP := "Defeat", 0
	if intValue(clan["stars"]) > intValue(opponent["stars"]) || (intValue(clan["stars"]) == intValue(opponent["stars"]) && floatValue(clan["destructionPercentage"]) > floatValue(opponent["destructionPercentage"])) {
		result, winXP = "Victory", 50
	} else if intValue(clan["stars"]) == intValue(opponent["stars"]) && floatValue(clan["destructionPercentage"]) == floatValue(opponent["destructionPercentage"]) {
		result = "Draw"
	}
	possibleXP, earnedXP, availableBaseXP, earnedBaseXP := 85, winXP, 0, 0
	for _, value := range anyList(opponent["members"]) {
		member, _ := mapValue(value)
		xp := discordWarHallXP[intValue(member["townhallLevel"])]
		possibleXP += xp
		availableBaseXP += xp
		best, _ := mapValue(member["bestOpponentAttack"])
		if intValue(best["stars"]) >= 1 {
			earnedXP += xp
			earnedBaseXP += xp
		}
	}
	maxStars := intValue(war["teamSize"]) * 3
	forty, sixty := int(math.Floor(0.4*float64(maxStars))), int(math.Floor(0.6*float64(maxStars)))
	fortyXP, sixtyXP := 0, 0
	if intValue(clan["stars"]) >= forty {
		fortyXP = 10
		earnedXP += 10
	}
	if intValue(clan["stars"]) >= sixty {
		sixtyXP = 25
		earnedXP += 25
	}
	cream, yellow := color.RGBA{255, 255, 204, 255}, color.RGBA{255, 248, 113, 255}
	text := func(value string, x, y int, size float64, anchor string, stroke int, ink color.Color) error {
		return drawDiscordCardText(canvas, value, x, y, size, anchor, stroke, ink)
	}
	for _, item := range []struct {
		value  string
		x, y   int
		size   float64
		anchor string
		stroke int
		ink    color.Color
	}{
		{stringMapValue(clan, "name"), 800, 130, 45, "rm", 4, cream},
		{stringMapValue(opponent, "name"), 1250, 130, 45, "lm", 4, cream},
		{result, 1040, 290, 65, "mm", 4, yellow},
		{fmt.Sprintf("%d - %d", intValue(clan["stars"]), intValue(opponent["stars"])), 1040, 380, 70, "mm", 4, cream},
		{fmt.Sprintf("+%d / %d", earnedBaseXP, availableBaseXP), 1600, 585, 25, "rm", 4, discordWhite},
		{fmt.Sprintf("+%d / 10", fortyXP), 1600, 640, 25, "rm", 4, discordWhite},
		{fmt.Sprintf("+%d / 25", sixtyXP), 1600, 705, 25, "rm", 4, discordWhite},
		{fmt.Sprintf("+%d / 50", winXP), 1600, 762, 25, "rm", 4, discordWhite},
		{fmt.Sprint(forty), 775, 642, 25, "mm", 4, discordWhite},
		{fmt.Sprint(sixty), 775, 705, 25, "mm", 4, discordWhite},
		{fmt.Sprintf("%.2f%%", floatValue(clan["destructionPercentage"])), 850, 452, 25, "rm", 3, cream},
		{fmt.Sprintf("%.2f%%", floatValue(opponent["destructionPercentage"])), 1250, 452, 25, "lm", 3, cream},
		{fmt.Sprintf("%d / %d", earnedXP, possibleXP), 490, 775, 35, "mm", 4, discordWhite},
	} {
		if err := text(item.value, item.x, item.y, item.size, item.anchor, item.stroke, item.ink); err != nil {
			return discord.MessageCreate{}, err
		}
	}
	resized := image.NewRGBA(image.Rect(0, 0, 725, 471))
	xdraw.CatmullRom.Scale(resized, resized.Bounds(), canvas, canvas.Bounds(), draw.Over, nil)
	var data bytes.Buffer
	encoder := png.Encoder{CompressionLevel: png.BestSpeed}
	if err := encoder.Encode(&data, resized); err != nil {
		return discord.MessageCreate{}, err
	}
	return discord.MessageCreate{Files: []*discord.File{{Name: "war-result.png", Reader: bytes.NewReader(data.Bytes())}}}, nil
}
