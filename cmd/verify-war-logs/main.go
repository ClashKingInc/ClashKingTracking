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
			fmt.Fprint(w, `{"items":[]}`)
		case strings.HasSuffix(r.URL.Path, "/webhooks/"+hook):
			json.NewEncoder(w).Encode(map[string]any{"id": hook, "type": 1, "guild_id": guild, "channel_id": channel, "application_id": appID, "token": "fixture"})
		case strings.Contains(r.URL.Path, "/webhooks/"+hook+"/fixture") && r.Method == "POST":
			check(r.URL.Query().Get("thread_id") == thread, "thread lost")
			var body map[string]any
			must(json.NewDecoder(r.Body).Decode(&body))
			content, _ := body["content"].(string)
			check(strings.Contains(content, "Attacker") && strings.Contains(content, "100%"), "attack content missing")
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
	_, e = db.Exec(ctx, "INSERT INTO server_logs(server_id,clan_tag,type,webhook_id,thread_id) VALUES($1,'#PYL','war_log',$2,$3)", guild, hook, thread)
	must(e)
	stream := fmt.Sprintf("war-log-fixture:%d", os.Getpid())
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", DiscordDeliveryBatchSize: 10, EventStreamName: stream, EventStreamReclaimIdleSeconds: 1}, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	for _, kind := range []string{"random", "cwl", "friendly"} {
		war := map[string]any{"type": kind, "clan": map[string]any{"tag": "#PYL", "members": []any{map[string]any{"tag": "#PYP", "name": "Attacker", "townhallLevel": 16, "mapPosition": 1}}}, "opponent": map[string]any{"tag": "#PYQ", "members": []any{map[string]any{"tag": "#PYG", "name": "Defender", "townhallLevel": 16, "mapPosition": 1}}}}
		must(app.PublishEvent(ctx, platform.Event{Topic: "war", ClanTag: "#PYL", Timestamp: time.Now(), Value: map[string]any{"type": "new_attacks", "war_type": kind, "war": war, "attacks": []any{map[string]any{"attackerTag": "#PYP", "defenderTag": "#PYG", "stars": 3, "destructionPercentage": 100}}}}))
	}
	until := time.Now().Add(15 * time.Second)
	for sent.Load() < 3 && time.Now().Before(until) {
		time.Sleep(30 * time.Millisecond)
	}
	check(sent.Load() == 3, "regular/CWL/friendly attacks did not all reach the log-only thread")
	stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		panic("delivery did not stop")
	}
	fmt.Println("PASS actual war log delivery: regular, CWL and friendly attacks; log-only configuration; existing forum thread; no production writes")
}
