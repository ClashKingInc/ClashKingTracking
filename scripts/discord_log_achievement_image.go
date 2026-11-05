package scripts

import (
	"bytes"
	"embed"
	"fmt"
	"image"
	"image/color"
	stdDraw "image/draw"
	"image/png"
	"slices"
	"strings"
	"time"

	"github.com/disgoorg/disgo/discord"
	xDraw "golang.org/x/image/draw"
	"golang.org/x/image/font"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/math/fixed"
)

// Artwork and font are copied from clashking_assets, the published asset source.
//
//go:embed assets/achievement/*.png assets/achievement/clashking.ttf
var achievementAssets embed.FS

var achievementRed = color.RGBA{217, 7, 9, 255}
var achievementWhite = color.RGBA{245, 242, 238, 255}

type clanAchievementCard struct {
	label, result, artwork string
	previous               string
	details                map[string]any
}

func discordClanAchievementMessages(event mobileWarEvent, _ discordLogIcons) []discord.MessageCreate {
	clan, _ := mapValue(event.Value["clan"])
	badge, _ := mapValue(clan["badgeUrls"])
	name := firstDiscordString(stringMapValue(clan, "name"), event.ClanTag)
	var card clanAchievementCard
	switch stringMapValue(event.Value, "achievement") {
	case "clan_level":
		card = clanAchievementCard{label: "Level up", result: fmt.Sprintf("Level %d", intValue(event.Value["current"]))}
	case "war_win_streak":
		card = clanAchievementCard{label: "war win streak", result: fmt.Sprint(intValue(event.Value["current"]))}
	case "cwl_promotion":
		previous, _ := mapValue(event.Value["previous"])
		current, _ := mapValue(event.Value["current"])
		league := stringMapValue(current, "name")
		if league == "" {
			return nil
		}
		details, _ := mapValue(event.Value["cwl_details"])
		card = clanAchievementCard{label: "Promoted to", result: league, artwork: cwlAchievementArtwork(league), previous: stringMapValue(previous, "name"), details: details}
	default:
		return nil
	}
	if data, err := renderClanAchievementImage(name, event.ClanTag, firstDiscordString(stringMapValue(badge, "medium"), stringMapValue(badge, "large")), card); err == nil && len(data) > 0 {
		return []discord.MessageCreate{{Files: []*discord.File{discord.NewFile("clan-achievement.png", "Clan achievement", bytes.NewReader(data))}}}
	}
	return []discord.MessageCreate{{Content: fmt.Sprintf("%s: %s · %s (achievement image unavailable)", name, card.label, card.result)}}
}

func cwlAchievementArtwork(league string) string {
	parts := strings.Fields(league)
	if len(parts) != 3 || parts[1] != "League" || !slices.Contains([]string{"Bronze", "Silver", "Gold", "Crystal", "Master", "Champion"}, parts[0]) ||
		!slices.Contains([]string{"I", "II", "III"}, parts[2]) {
		return ""
	}
	return "assets/achievement/War" + parts[0] + parts[2] + ".png"
}

func achievementArt(path string) (image.Image, error) {
	data, err := achievementAssets.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return png.Decode(bytes.NewReader(data))
}

func renderClanAchievementImage(clanName, _ string, badgeURL string, card clanAchievementCard) ([]byte, error) {
	const width = 1200
	height := 400
	if card.artwork != "" && intValue(card.details["wars"]) > 0 {
		height = 560
	}
	canvas := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			shade := uint8(3 + 8*x/width + 1*y/height)
			canvas.SetRGBA(x, y, color.RGBA{shade, shade, shade + 1, 255})
		}
	}
	// One diffuse red glow sits behind real artwork, without a separate panel.
	for y := 0; y < height; y++ {
		for x := 650; x < width; x++ {
			dx, dy := float64(x-995)/300, float64(y-height/2)/float64(height)*1.5
			strength := 1 - dx*dx - dy*dy
			if strength <= 0 {
				continue
			}
			p := canvas.RGBAAt(x, y)
			p.R += uint8(strength * 25)
			p.G += uint8(strength * 2)
			p.B += uint8(strength * 3)
			canvas.SetRGBA(x, y, p)
		}
	}
	clanBadge := discordCardBadge(badgeURL)
	art := image.Image(nil)
	if card.artwork != "" {
		var err error
		art, err = achievementArt(card.artwork)
		if err != nil {
			return nil, err
		}
	} else {
		art = clanBadge
	}
	if art != nil {
		// A quiet oversize echo gives the badge depth while the crisp badge stays in front.
		ghost := image.NewRGBA(image.Rect(0, 0, 320, 320))
		placeAchievementArt(ghost, art)
		stdDraw.DrawMask(canvas, image.Rect(825, 40, 1145, 360), ghost, image.Point{}, image.NewUniform(color.Alpha{25}), image.Point{}, stdDraw.Over)
		foreground := image.NewRGBA(image.Rect(0, 0, 230, 230))
		placeAchievementArt(foreground, art)
		stdDraw.Draw(canvas, image.Rect(870, 85, 1100, 315), foreground, image.Point{}, stdDraw.Over)
	}
	fontData, err := achievementAssets.ReadFile("assets/achievement/clashking.ttf")
	if err != nil {
		return nil, err
	}
	face, err := opentype.Parse(fontData)
	if err != nil {
		return nil, err
	}
	text := func(value string, x, baseline, maxWidth int, size float64, ink color.Color) error {
		return drawAchievementText(canvas, face, value, x, baseline, maxWidth, size, ink)
	}
	clanNameX, clanNameWidth := 72, 685
	if card.artwork != "" && clanBadge != nil {
		mini := image.NewRGBA(image.Rect(0, 0, 52, 52))
		placeAchievementArt(mini, clanBadge)
		stdDraw.Draw(canvas, image.Rect(72, 36, 124, 88), mini, image.Point{}, stdDraw.Over)
		clanNameX, clanNameWidth = 140, 615
	}
	if err := text(clanName, clanNameX, 74, clanNameWidth, 34, achievementWhite); err != nil {
		return nil, err
	}
	switch card.label {
	case "war win streak":
		if err := text(card.result, 72, 265, 655, 152, achievementWhite); err != nil {
			return nil, err
		}
		if err := text(card.label, 79, 331, 650, 32, achievementRed); err != nil {
			return nil, err
		}
	case "Promoted to":
		if err := text(card.label, 78, 149, 660, 29, achievementRed); err != nil {
			return nil, err
		}
		if err := text(card.result, 72, 244, 720, 77, achievementWhite); err != nil {
			return nil, err
		}
		if card.previous != "" {
			if err := text("From "+card.previous, 78, 293, 680, 25, color.RGBA{155, 153, 153, 255}); err != nil {
				return nil, err
			}
		}
		if height == 560 {
			if err := drawCWLDetails(text, card.details); err != nil {
				return nil, err
			}
		}
	default:
		if err := text(card.label, 78, 158, 660, 29, achievementRed); err != nil {
			return nil, err
		}
		if err := text(card.result, 72, 278, 680, 105, achievementWhite); err != nil {
			return nil, err
		}
	}
	var out bytes.Buffer
	if err := png.Encode(&out, canvas); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func drawCWLDetails(text func(string, int, int, int, float64, color.Color) error, details map[string]any) error {
	muted := color.RGBA{165, 162, 161, 255}
	season := stringMapValue(details, "season")
	if season != "" {
		if date, err := time.Parse("2006-01", season); err == nil {
			season = date.Format("January 2006")
		}
		if err := text(season, 78, 352, 695, 25, muted); err != nil {
			return err
		}
	}
	wars := intValue(details["wars"])
	wins, hasWins := details["wins"]
	stars, hasStars := details["stars"]
	if wars > 0 && hasWins {
		if err := text(fmt.Sprintf("%d/%d", intValue(wins), wars), 78, 430, 170, 42, achievementWhite); err != nil {
			return err
		}
		if err := text("Wars won", 80, 467, 170, 22, muted); err != nil {
			return err
		}
	}
	if hasStars {
		if err := text(fmt.Sprint(intValue(stars)), 315, 430, 170, 42, achievementWhite); err != nil {
			return err
		}
		if err := text("Stars", 317, 467, 170, 22, muted); err != nil {
			return err
		}
	}
	if destruction, ok := achievementPercent(details["destruction_percentage"]); ok {
		if err := text(fmt.Sprintf("%.1f%%", destruction), 535, 430, 240, 42, achievementWhite); err != nil {
			return err
		}
		if err := text("Avg destruction", 537, 467, 245, 22, muted); err != nil {
			return err
		}
	}
	return nil
}

func achievementPercent(value any) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	default:
		return 0, false
	}
}

func placeAchievementArt(dst *image.RGBA, src image.Image) {
	width, height := dst.Bounds().Dx(), dst.Bounds().Dy()
	srcWidth, srcHeight := src.Bounds().Dx(), src.Bounds().Dy()
	if srcWidth == 0 || srcHeight == 0 {
		return
	}
	if width*srcHeight > height*srcWidth {
		width = height * srcWidth / srcHeight
	} else {
		height = width * srcHeight / srcWidth
	}
	left := (dst.Bounds().Dx() - width) / 2
	top := (dst.Bounds().Dy() - height) / 2
	xDraw.CatmullRom.Scale(dst, image.Rect(left, top, left+width, top+height), src, src.Bounds(), stdDraw.Over, nil)
}

func drawAchievementText(dst stdDraw.Image, parsed *opentype.Font, value string, x, baseline, maxWidth int, initialSize float64, ink color.Color) error {
	minimumSize := min(initialSize, 24)
	for size := initialSize; ; size = max(minimumSize, size-2) {
		face, err := opentype.NewFace(parsed, &opentype.FaceOptions{Size: size, DPI: 72, Hinting: font.HintingFull})
		if err != nil {
			return err
		}
		if font.MeasureString(face, value).Ceil() <= maxWidth || size == minimumSize {
			if font.MeasureString(face, value).Ceil() > maxWidth {
				runes := []rune(value)
				for len(runes) > 0 && font.MeasureString(face, string(runes)+"…").Ceil() > maxWidth {
					runes = runes[:len(runes)-1]
				}
				value = string(runes) + "…"
			}
			(&font.Drawer{Dst: dst, Src: image.NewUniform(ink), Face: face, Dot: fixed.P(x, baseline)}).DrawString(value)
			face.Close()
			return nil
		}
		face.Close()
	}
}
