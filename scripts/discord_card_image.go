package scripts

import (
	"bytes"
	"context"
	_ "embed"
	"image"
	"image/color"
	"image/draw"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/image/font"
	"golang.org/x/image/font/opentype"
	"golang.org/x/image/math/fixed"
)

//go:embed assets/SCmagic.ttf
var discordSCMagicFont []byte

var discordWhite = color.RGBA{255, 255, 255, 255}
var discordBlack = color.RGBA{0, 0, 0, 255}

// drawDiscordCardText uses the original Supercell font and center/left anchors
// used by the archived Pillow graphic generator. Stroke matches its dark outline.
func drawDiscordCardText(dst draw.Image, text string, x, y int, size float64, anchor string, stroke int, ink color.Color) error {
	parsed, err := opentype.Parse(discordSCMagicFont)
	if err != nil {
		return err
	}
	face, err := opentype.NewFace(parsed, &opentype.FaceOptions{Size: size, DPI: 72, Hinting: font.HintingFull})
	if err != nil {
		return err
	}
	defer face.Close()
	width := font.MeasureString(face, text).Ceil()
	metrics := face.Metrics()
	if strings.HasPrefix(anchor, "m") {
		x -= width / 2
	}
	if strings.HasPrefix(anchor, "r") {
		x -= width
	}
	if strings.HasSuffix(anchor, "m") {
		y += (metrics.Ascent.Ceil() - metrics.Descent.Ceil()) / 2
	}
	baseline := fixed.P(x, y)
	if stroke > 0 {
		for dy := -stroke; dy <= stroke; dy++ {
			for dx := -stroke; dx <= stroke; dx++ {
				if dx*dx+dy*dy > stroke*stroke {
					continue
				}
				(&font.Drawer{Dst: dst, Src: image.NewUniform(discordBlack), Face: face, Dot: baseline.Add(fixed.P(dx, dy))}).DrawString(text)
			}
		}
	}
	(&font.Drawer{Dst: dst, Src: image.NewUniform(ink), Face: face, Dot: baseline}).DrawString(text)
	return nil
}

// The URL is supplied by the Clash clan API. Keep fetching bounded so a badge
// host outage cannot delay weekly Discord delivery for long.
func discordCardBadge(rawURL string) image.Image {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "https" || !strings.HasSuffix(u.Hostname(), ".clashofclans.com") {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil
	}
	badge, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil
	}
	return badge
}
