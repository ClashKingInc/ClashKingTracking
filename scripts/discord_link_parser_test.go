package scripts

import "testing"

func TestFirstClashLink(t *testing.T) {
	link := firstClashLink("check this <https://link.clashofclans.com/en?action=OpenPlayerProfile&tag=%23P0Y>")
	if link == nil || link.Query().Get("tag") != "#P0Y" {
		t.Fatalf("firstClashLink() = %#v", link)
	}
	if firstClashLink("https://example.com/?action=OpenPlayerProfile") != nil {
		t.Fatal("accepted a non-Clash link")
	}
}

func TestNormalizeClashLinkTag(t *testing.T) {
	if got := normalizeClashLinkTag(" #poy "); got != "#P0Y" {
		t.Fatalf("normalizeClashLinkTag() = %q", got)
	}
	if got := normalizeClashLinkTag("#BAD1"); got != "" {
		t.Fatalf("invalid tag normalized to %q", got)
	}
}

func TestDiscordMessagePayload(t *testing.T) {
	event := mobileWarEvent{Value: map[string]any{
		"channel_id": "channel",
		"content":    "content",
		"guild_id":   "guild",
	}}
	if got := discordMessagePayload(event); got.ChannelID != "channel" || got.Content != "content" || got.GuildID != "guild" {
		t.Fatalf("discordMessagePayload() = %#v", got)
	}
}
