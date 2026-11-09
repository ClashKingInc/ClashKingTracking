// Exercise queued link events through the actual delivery service and REST client.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/valkey-io/valkey-go"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable services required")
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback required")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(err)
	defer cache.Close()
	const guild = "999999999999995801"
	const appID = "999999999999995802"
	const channel = "999999999999995803"
	var mu sync.Mutex
	var messages []map[string]any
	disabled := false
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/settings"):
			mu.Lock()
			enabled := !disabled
			mu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"embed_color": "#123456", "link_parse": map[string]bool{"army": enabled, "base": enabled, "player": enabled, "clan": enabled, "show": enabled}})
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[]}`)
		case r.URL.Path == "/v2/bot/armies/preview":
			var input map[string]string
			must(json.NewDecoder(r.Body).Decode(&input))
			check(strings.Contains(input["army"], "CopyArmy"), "army input missing")
			fmt.Fprint(w, `{"armyLink":"https://link.clashofclans.com/en?action=CopyArmy&army=u2x0s1x0","presentation":{"minimumTownHall":3,"troopSpace":2,"spellSpace":1,"clanCastleSpace":0,"units":[{"name":"Barbarian","quantity":2,"section":"Troops"},{"name":"Lightning Spell","quantity":1,"section":"Spells"}],"heroes":[]}}`)
		case strings.HasSuffix(r.URL.Path, "/clans-basic"):
			fmt.Fprint(w, `[{"name":"Parser Clan","tag":"#PYL"}]`)
		case strings.Contains(r.URL.Path, "/cached"):
			fmt.Fprint(w, `{"tag":"#PYL","name":"Parser Clan","clanLevel":20,"members":45,"description":"Welcome","warLeague":{"name":"Champion League I"}}`)
		case strings.HasPrefix(r.URL.Path, "/proxy/v1/players/"):
			fmt.Fprint(w, `{"tag":"#PYP","name":"Parser Player","townHallLevel":18,"warStars":400,"clan":{"name":"Parser Clan"}}`)
		case strings.Contains(r.URL.Path, "/channels/"+channel+"/messages") && r.Method == "POST":
			var body map[string]any
			must(json.NewDecoder(r.Body).Decode(&body))
			mu.Lock()
			messages = append(messages, body)
			mu.Unlock()
			fmt.Fprint(w, `{"id":"999999999999995899","channel_id":"`+channel+`","type":0}`)
		default:
			http.Error(w, "unexpected local route", 404)
		}
	}))
	defer provider.Close()
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", DiscordDeliveryBatchSize: 10, EventStreamName: fmt.Sprintf("link-preview:%d", os.Getpid()), EventStreamReclaimIdleSeconds: 1}, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	contents := []string{"See [army](https://link.clashofclans.com/en?action=CopyArmy&army=u2x0s1x0)", "https://link.clashofclans.com/en?action=OpenPlayerProfile&tag=%23PYP", "https://link.clashofclans.com/en?action=OpenClanProfile&tag=%23PYL", "My base https://link.clashofclans.com/en?action=OpenLayout&id=TH18%3AHV%3ATEST", "-show Parser"}
	for _, content := range contents {
		must(app.PublishEvent(ctx, platform.Event{Topic: "discord_message_create", Timestamp: time.Now(), Value: map[string]any{"guild_id": guild, "channel_id": channel, "content": content, "attachments": []map[string]string{{"url": "https://cdn.discordapp.com/attachments/1/2/base.png", "content_type": "image/png"}}}}))
	}
	until := time.Now().Add(15 * time.Second)
	for time.Now().Before(until) {
		mu.Lock()
		n := len(messages)
		mu.Unlock()
		if n == 5 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	mu.Lock()
	check(len(messages) == 5, "not every parser delivered a preview")
	titles := map[string]map[string]any{}
	for _, message := range messages {
		check(message["content"] == nil || message["content"] == "", "parser echoed raw content")
		mentions := message["allowed_mentions"].(map[string]any)
		check(len(mentions["parse"].([]any)) == 0, "mentions enabled")
		embeds := message["embeds"].([]any)
		embed := embeds[0].(map[string]any)
		check(embed["color"].(float64) == 0x123456, "server color lost")
		titles[embed["title"].(string)] = message
	}
	check(len(titles["Army Link"]["embeds"].([]any)[0].(map[string]any)["fields"].([]any)) == 2, "army sections missing")
	check(titles["Base Layout"]["embeds"].([]any)[0].(map[string]any)["image"] != nil, "base image lost")
	check(titles["Parser Player"] != nil && titles["Parser Clan"] != nil, "player/clan cards missing")
	disabled = true
	mu.Unlock()
	must(app.PublishEvent(ctx, platform.Event{Topic: "discord_message_create", Timestamp: time.Now(), Value: map[string]any{"guild_id": guild, "channel_id": channel, "content": contents[0]}}))
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	check(len(messages) == 5, "disabled parser sent a message")
	mu.Unlock()
	stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		panic("delivery did not stop")
	}
	fmt.Println("PASS actual delivery service: army/player/clan/base/-show previews, copy buttons, base image, settings, no mentions, disabled parser; no live Discord messages.")
}
