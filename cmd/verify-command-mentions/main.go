// Exercise real Gateway -> Valkey -> Discord Delivery against loopback Discord.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"github.com/disgoorg/disgo/rest"
	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	valkey "github.com/valkey-io/valkey-go"
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
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback services required")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(err)
	defer cache.Close()
	const guild = "999999999999993401"
	const appID = "999999999999993402"
	const channel = "999999999999993403"
	const author = "999999999999993404"
	var requests, unexpected atomic.Int32
	var mu sync.Mutex
	var messages []string
	var origin string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v10/gateway/bot":
			json.NewEncoder(w).Encode(map[string]any{"url": "ws" + strings.TrimPrefix(origin, "http") + "/gateway", "shards": 1, "session_start_limit": map[string]any{"total": 100, "remaining": 100, "reset_after": 60000, "max_concurrency": 1}})
		case strings.HasSuffix(r.URL.Path, "/applications/"+appID+"/commands") && r.Method == "GET":
			requests.Add(1)
			fmt.Fprintf(w, `[{"id":"101","application_id":"%s","type":1,"name":"setup","description":"Setup","version":"1"},{"id":"102","application_id":"%s","type":1,"name":"link","description":"Links","version":"1","options":[{"type":1,"name":"add","description":"Add","options":[{"type":3,"name":"player","description":"Player","required":true}]},{"type":1,"name":"remove","description":"Remove"}]},{"id":"103","application_id":"%s","type":1,"name":"admin","description":"Admin","version":"1","options":[{"type":2,"name":"access","description":"Access","options":[{"type":1,"name":"view","description":"View"}]}]},{"id":"104","application_id":"%s","type":1,"name":"new-command","description":"Newly registered","version":"1"},{"id":"105","application_id":"%s","type":2,"name":"User info","version":"1"}]`, appID, appID, appID, appID, appID)
		case strings.HasSuffix(r.URL.Path, "/channels/"+channel+"/messages") && r.Method == "POST":
			var body map[string]any
			must(json.NewDecoder(r.Body).Decode(&body))
			mentions := body["allowed_mentions"].(map[string]any)
			check(len(mentions["parse"].([]any)) == 0, "mentions enabled")
			check(body["message_reference"] == nil, "should send a standalone message")
			check(body["embeds"] == nil, "command links should not add an embed")
			mu.Lock()
			messages = append(messages, body["content"].(string))
			mu.Unlock()
			fmt.Fprintf(w, `{"id":"999999999999993499","channel_id":"%s","type":0}`, channel)
		case r.URL.Path == "/gateway":
			conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			var compressed bytes.Buffer
			encoder, err := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1))
			must(err)
			defer encoder.Close()
			send := func(v any) error {
				b, err := json.Marshal(v)
				if err != nil {
					return err
				}
				if _, err = encoder.Write(b); err != nil {
					return err
				}
				if err = encoder.Flush(); err != nil {
					return err
				}
				err = conn.WriteMessage(websocket.BinaryMessage, append([]byte(nil), compressed.Bytes()...))
				compressed.Reset()
				return err
			}
			if send(map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": 1000}}) != nil {
				return
			}
			seq := 0
			dispatch := func(t string, v any) error { seq++; return send(map[string]any{"op": 0, "s": seq, "t": t, "d": v}) }
			for {
				var incoming struct {
					Op int `json:"op"`
				}
				if conn.ReadJSON(&incoming) != nil {
					return
				}
				switch incoming.Op {
				case 1:
					if send(map[string]any{"op": 11, "d": nil}) != nil {
						return
					}
				case 2:
					if dispatch("READY", map[string]any{"v": 10, "user": map[string]any{"id": appID, "username": "fixture", "discriminator": "0", "bot": true}, "guilds": []any{map[string]any{"id": guild, "unavailable": true}}, "session_id": "local-session", "resume_gateway_url": "ws" + strings.TrimPrefix(origin, "http") + "/gateway", "shard": []int{0, 1}, "application": map[string]any{"id": appID, "flags": 0}}) != nil {
						return
					}
					if dispatch("GUILD_CREATE", map[string]any{"id": guild, "name": "Fixture", "owner_id": author, "unavailable": false, "large": false, "member_count": 0, "roles": []any{map[string]any{"id": guild, "name": "@everyone", "permissions": "0", "color": 0, "position": 0}}, "channels": []any{map[string]any{"id": channel, "type": 0, "name": "commands", "permission_overwrites": []any{}}}, "threads": []any{}, "members": []any{}, "emojis": []any{}, "features": []any{}, "voice_states": []any{}, "presences": []any{}}) != nil {
						return
					}
					contents := []string{"-/setup", "-/link add", "-/link remove", "-/admin access view", "-/LINK   ADD", "-/new-command", "-/missing", "-/link missing", "-/setup option", "-/admin access missing", "text -/setup", "`-/setup`", "-/link add extra argument", "-/User info", "-/setup", "-/setup"}
					for i, content := range contents {
						m := map[string]any{"id": fmt.Sprint(999999999999993500 + i), "channel_id": channel, "guild_id": guild, "author": map[string]any{"id": author, "username": "tester", "discriminator": "0", "bot": i == 14}, "content": content, "timestamp": time.Now().UTC().Format(time.RFC3339), "tts": false, "mention_everyone": false, "mentions": []any{}, "mention_roles": []any{}, "attachments": []any{}, "embeds": []any{}, "pinned": false, "type": 0}
						if i == 15 {
							m["webhook_id"] = "999999999999993499"
						}
						if dispatch("MESSAGE_CREATE", m) != nil {
							return
						}
					}
				}
			}
		default:
			unexpected.Add(1)
			http.Error(w, "unexpected local request", 404)
		}
	}))
	defer provider.Close()
	origin = provider.URL
	oldAPI := rest.API
	rest.API = origin + "/"
	defer func() { rest.API = oldAPI }()
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: origin, ClashKingAPIURL: origin, ClashKingAPIToken: "fixture", DiscordMessageCreateEnabled: true, DiscordGatewayQueueSize: 1024, DiscordGatewayMemberChunkConcurrency: 1, DiscordDeliveryBatchSize: 20, EventStreamName: "command-mention-fixture", EventStreamReclaimIdleSeconds: 1}, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 2)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	go func() { done <- scripts.NewDiscordGatewayDomain().Run(runCtx, app) }()
	expected := map[string]int{"</setup:101>": 1, "</link add:102>": 2, "</link remove:102>": 1, "</admin access view:103>": 1, "</new-command:104>": 1}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(messages)
		mu.Unlock()
		if n >= 6 {
			break
		}
		select {
		case err := <-done:
			panic(fmt.Sprintf("service exited early: %v", err))
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	actual := map[string]int{}
	for _, m := range messages {
		actual[m]++
	}
	mu.Unlock()
	check(reflect.DeepEqual(actual, expected), fmt.Sprintf("wrong command responses: %v", actual))
	check(requests.Load() == 1, fmt.Sprintf("expected one cached command registry read, got %d", requests.Load()))
	check(unexpected.Load() == 0, "unexpected external operation (including message edit/delete, settings lookup, or command execution)")
	stop()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			panic("service did not stop")
		}
	}
	fmt.Println("PASS real Gateway -> Valkey -> Delivery: root, subcommand, nested group, normalized whitespace, dynamic registered command, cached IDs, invalid/context commands ignored, bots/webhooks ignored, standalone messages, no pings or command execution")
}
