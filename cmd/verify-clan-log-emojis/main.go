// Exercise the actual Discord delivery service with local SQL, Valkey and HTTP.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
func check(b bool, s string) {
	if !b {
		panic(s)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable services required")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback required")
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999992801"
	const appID = "999999999999992802"
	const channel = "999999999999992803"
	const thread = "999999999999992804"
	const hook = "999999999999992805"
	var sent atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[{"id":"999999999999992811","name":"legendleagueiii"},{"id":"999999999999992812","name":"trophy"}]}`)
		case strings.HasSuffix(r.URL.Path, "/webhooks/"+hook):
			json.NewEncoder(w).Encode(map[string]any{"id": hook, "type": 1, "guild_id": guild, "channel_id": channel, "application_id": appID, "token": "fixture"})
		case strings.Contains(r.URL.Path, "/webhooks/"+hook+"/fixture") && r.Method == "POST":
			check(r.URL.Query().Get("thread_id") == thread, "thread lost")
			var body map[string]any
			must(json.NewDecoder(r.Body).Decode(&body))
			encoded, err := json.Marshal(body)
			must(err)
			content := string(encoded)
			if strings.Contains(content, "Unknown League") {
				check(strings.Contains(content, "trophy:999999999999992812"), "unknown league lost trophy fallback")
			} else {
				check(strings.Contains(content, "legendleagueiii:999999999999992811"), "league badge missing")
				check(!strings.Contains(content, "trophy:999999999999992812"), "known league rendered as trophy")
			}
			sent.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"id": "999999999999992806", "channel_id": thread, "type": 0})
		default:
			http.Error(w, "unexpected fixture request", 404)
		}
	}))
	defer provider.Close()
	_, e = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'War log fixture')", guild)
	must(e)
	_, e = db.Exec(ctx, "INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received) VALUES('#PYL','War log clan',true,0,1,'',0,0)")
	must(e)
	_, e = db.Exec(ctx, "INSERT INTO server_clans(server_id,tag) VALUES($1,'#PYL')", guild)
	must(e)
	_, e = db.Exec(ctx, "INSERT INTO server_logs(server_id,clan_tag,type,webhook_id,thread_id) VALUES($1,'#PYL','join_log',$2,$3),($1,'#PYL','leave_log',$2,$3)", guild, hook, thread)
	must(e)
	stream := fmt.Sprintf("war-log-fixture:%d", os.Getpid())
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", DiscordDeliveryBatchSize: 10, EventStreamName: stream, EventStreamReclaimIdleSeconds: 1}, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	for _, fixture := range []struct{ kind, field, league, name string }{
		{"member_join", "leagueTier", "Legend III", "Current Join"},
		{"member_leave", "leagueTier", "Legend III", "Current Leave"},
		{"member_join", "league", "Legend League III", "Legacy Join"},
		{"member_leave", "leagueTier", "", "Unknown League"},
	} {
		player := map[string]any{"tag": "#PYP", "name": fixture.name, "townHallLevel": 18, "trophies": 721, "warStars": 2775, "heroes": []any{}, fixture.field: map[string]any{"name": fixture.league}}
		must(app.PublishEvent(ctx, platform.Event{Topic: "clan", ClanTag: "#PYL", Timestamp: time.Now(), Value: map[string]any{"type": fixture.kind, "member": player, "player": player, "clan": map[string]any{"tag": "#PYL", "name": "Fixture Clan", "members": 38}}}))
	}
	until := time.Now().Add(15 * time.Second)
	for sent.Load() < 4 && time.Now().Before(until) {
		time.Sleep(30 * time.Millisecond)
	}
	check(sent.Load() == 4, "join/leave fixtures did not all reach the configured thread")
	stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		panic("delivery did not stop")
	}
	fmt.Println("PASS actual clan log delivery: modern join/leave league badges, legacy badge and unknown-league fallback; no production writes")
}
