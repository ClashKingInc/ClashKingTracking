// Exercise the actual delivery domain with disposable stores and loopback Discord.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func check(b bool, m string) {
	if !b {
		panic(m)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable fixtures required")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dsn := os.Getenv("TEST_DATABASE_URL")
	addr := os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback required")
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999992901"
	const appID = "999999999999992902"
	const channel = "999999999999992903"
	const thread = "999999999999992904"
	const hook = "999999999999992905"
	const badHook = "999999999999992906"
	var sent, botMessages, badReads atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[]}`)
		case strings.HasSuffix(r.URL.Path, "/webhooks/"+hook) || strings.HasSuffix(r.URL.Path, "/webhooks/"+badHook):
			owner := appID
			if strings.HasSuffix(r.URL.Path, badHook) {
				owner = "999999999999992999"
				badReads.Add(1)
			}
			json.NewEncoder(w).Encode(map[string]any{"id": hook, "type": 1, "guild_id": guild, "channel_id": channel, "application_id": owner, "token": "fixture"})
		case strings.HasSuffix(r.URL.Path, "/channels/"+thread):
			json.NewEncoder(w).Encode(map[string]any{"id": thread, "type": 11, "guild_id": guild, "parent_id": channel, "name": "fixture"})
		case strings.Contains(r.URL.Path, "/webhooks/"+hook+"/fixture") && r.Method == "POST":
			check(r.URL.Query().Get("thread_id") == thread, "thread not forwarded")
			sent.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"id": "999999999999992907", "channel_id": thread, "type": 0})
		case strings.Contains(r.URL.Path, "/messages") && r.Method == "POST":
			botMessages.Add(1)
			http.Error(w, "bot fallback forbidden", 500)
		default:
			http.Error(w, "unexpected fixture request", 404)
		}
	}))
	defer provider.Close()
	cc := clashy.DefaultClientConfig()
	cc.BaseURL = provider.URL + "/v1"
	cc.LookupCache = false
	cc.UpdateCache = false
	clash, e := clashy.NewClient(cc)
	must(e)
	defer clash.Close()
	_, e = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'Webhook delivery fixture')", guild)
	must(e)
	ids := []string{}
	for _, h := range []string{hook, badHook, ""} {
		var id string
		must(db.QueryRow(ctx, `INSERT INTO reminders(server_id,type,type_name,clan_tag,webhook_token,channel_id,thread_id,webhook_id,minutes_remaining) VALUES($1,1,'War','#PYLQ','',$2,$3,NULLIF($4,''),60) RETURNING id::text`, guild, channel, thread, h).Scan(&id))
		ids = append(ids, id)
	}
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", DiscordDeliveryBatchSize: 20, EventStreamName: "reminder-webhook-fixture", EventStreamReclaimIdleSeconds: 1}, Clash: clash, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	time.Sleep(300 * time.Millisecond)
	for _, id := range ids {
		must(app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: "#PYLQ", Value: map[string]any{"type": "war", "members": []any{map[string]any{"tag": "#PYL", "name": "Fixture", "townhall": 16, "remaining": 2}}, "reminder": map[string]any{"id": id, "server_id": guild, "channel_id": channel, "thread_id": thread, "minutes_remaining": 60}}}))
	}
	until := time.Now().Add(15 * time.Second)
	for time.Now().Before(until) && (sent.Load() != 1 || badReads.Load() == 0) {
		time.Sleep(50 * time.Millisecond)
	}
	check(sent.Load() == 1, "valid reminder did not use webhook")
	check(badReads.Load() > 0, "foreign webhook case not exercised")
	check(botMessages.Load() == 0, "reminder fell back to bot message")
	stop()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		panic("delivery did not stop")
	}
	fmt.Println("PASS actual reminder delivery: owned webhook, correct thread, foreign/missing webhook rejected, no bot-message fallback")
}
