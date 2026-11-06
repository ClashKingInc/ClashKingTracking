// Run the real battlelogs and Discord-delivery services against local SQL,
// Valkey and loopback HTTP. No public upstream or Discord destination is used.
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
	"sync"
	"sync/atomic"
	"time"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func check(ok bool, msg string) {
	if !ok {
		panic(msg)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable services required")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback services required")
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999993701"
	const appID = "999999999999993702"
	const channel = "999999999999993703"
	const attackThread = "999999999999993704"
	const defenseThread = "999999999999993705"
	const hook = "999999999999993706"
	_, e = db.Exec(ctx, `INSERT INTO servers(id,name,last_command_at) VALUES($1,'Legend fixture',now());`, guild)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received) VALUES('#PYLC','Legend fixture',true,0,1,'',0,0);
 INSERT INTO basic_player(tag,name,league_id,townhall_level,clan_tag,trophies) VALUES('#PYLQ','Legend player',105000036,18,'#PYLC',5200)`)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO server_clans(server_id,tag) VALUES($1,'#PYLC');`, guild)
	must(e)
	for kind, thread := range map[string]string{"legend_log_attacks": attackThread, "legend_log_defenses": defenseThread} {
		_, e = db.Exec(ctx, `INSERT INTO server_logs(server_id,clan_tag,type,webhook_id,thread_id) VALUES($1,'#PYLC',$2,$3,$4)`, guild, kind, hook, thread)
		must(e)
	}
	var auxiliary, rawCalls atomic.Int32
	var mu sync.Mutex
	var entries []clashy.BattleLogEntry
	sent := map[string]int{}
	origin := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	battle := func(sec, stars, destruction int, attack bool) clashy.BattleLogEntry {
		return clashy.BattleLogEntry{BattleType: clashy.BattleTypeLegend, Attack: attack, OpponentPlayerTag: "#PYLR", OpponentName: "Opponent", OpponentTownHallLevel: 17, Stars: stars, DestructionPercentage: destruction, Duration: 90, Timestamp: origin.Add(time.Duration(sec) * time.Second).Format("20060102T150405.000Z")}
	}
	entries = []clashy.BattleLogEntry{battle(1, 3, 100, true), battle(2, 2, 80, false)}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/battlelog") {
			rawCalls.Add(1)
			mu.Lock()
			defer mu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"items": entries})
			return
		}
		auxiliary.Add(1)
		// This is the real Legend I shape. A regression that fetches this profile
		// still fails the zero-auxiliary assertion, even though the response is valid.
		json.NewEncoder(w).Encode(map[string]any{"tag": "#PYLQ", "leagueTier": map[string]any{"id": 105000036, "name": "Legend I"}, "currentLeagueGroupTag": "#0", "currentLeagueSeasonId": 1793595600})
	}))
	defer upstream.Close()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[]}`)
		case strings.HasSuffix(r.URL.Path, "/webhooks/"+hook):
			json.NewEncoder(w).Encode(map[string]any{"id": hook, "type": 1, "guild_id": guild, "channel_id": channel, "application_id": appID, "token": "fixture"})
		case strings.Contains(r.URL.Path, "/webhooks/"+hook+"/fixture") && r.Method == "POST":
			var body struct {
				Embeds []struct {
					Description string `json:"description"`
					Author      struct {
						Name string `json:"name"`
					} `json:"author"`
				} `json:"embeds"`
			}
			must(json.NewDecoder(r.Body).Decode(&body))
			check(len(body.Embeds) == 1, "missing embed")
			description := body.Embeds[0].Description
			check(body.Embeds[0].Author.Name == "Legend player | Legend fixture", "identity lost")
			thread := r.URL.Query().Get("thread_id")
			if strings.Contains(description, "-26 trophies") {
				check(thread == defenseThread, "defense routing lost")
			} else {
				check(thread == attackThread, "attack routing lost")
			}
			var rows int
			must(db.QueryRow(ctx, `SELECT count(*) FROM battles_ranked WHERE player_tag='#PYLQ'`).Scan(&rows))
			check(rows == 5, "Discord event preceded durable raw SQL")
			mu.Lock()
			sent[description]++
			mu.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"id": "999999999999993707", "channel_id": thread, "type": 0})
		default:
			http.Error(w, "unexpected fixture request", 404)
		}
	}))
	defer provider.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = upstream.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	client, e := clashy.NewClient(cfg)
	must(e)
	defer client.Close()
	stream := fmt.Sprintf("legend-raw-fixture:%d", os.Getpid())
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, BattlelogRequestsPerSecond: 10, BattlelogPriorityRequestsPerSecond: 10, BattlelogCheckpointTTLDays: 15, BattlelogFirstSeenLookbackDays: 14, TrackedClanSnapshotPrefix: "fixture:", EventStreamName: stream, EventStreamRetentionSeconds: 300, EventStreamReclaimIdleSeconds: 1, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", DiscordDeliveryBatchSize: 10}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 2)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	go func() { done <- scripts.NewBattlelogsDomain().Run(runCtx, app) }()
	wait := func(checker func() bool) {
		until := time.Now().Add(12 * time.Second)
		for !checker() {
			if time.Now().After(until) {
				panic("timed out waiting for actual service")
			}
			select {
			case err := <-done:
				panic(fmt.Sprintf("service exited: %v", err))
			default:
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	checkpoint := func() string { v, _ := cache.Do(ctx, cache.B().Get().Key("bl:#PYLQ").Build()).ToString(); return v }
	wait(func() bool { return checkpoint() == origin.Add(2*time.Second).Format(time.RFC3339Nano) })
	mu.Lock()
	check(len(sent) == 0, "first-seen history sent Discord messages")
	entries = append(entries, battle(3, 3, 100, true), battle(4, 2, 80, false), battle(5, 0, 0, true))
	mu.Unlock()
	total := func() int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for _, v := range sent {
			n += v
		}
		return n
	}
	wait(func() bool { return total() == 3 && checkpoint() == origin.Add(5*time.Second).Format(time.RFC3339Nano) })
	// Replay the same raw rows from a still-present older watermark. Existing
	// stream dedupe must suppress repeats while SQL identity preserves rows.
	must(cache.Do(ctx, cache.B().Set().Key("bl:#PYLQ").Value(origin.Add(2*time.Second).Format(time.RFC3339Nano)).Build()).Error())
	wait(func() bool { return checkpoint() == origin.Add(5*time.Second).Format(time.RFC3339Nano) })
	time.Sleep(500 * time.Millisecond)
	check(total() == 3, "replayed Discord events duplicated")
	check(auxiliary.Load() == 0, "Legend I made profile/group requests")
	check(rawCalls.Load() > 1, "raw poll did not run")
	mu.Lock()
	joined := ""
	for d := range sent {
		joined += d
	}
	mu.Unlock()
	check(strings.Contains(joined, "+40 trophies") && strings.Contains(joined, "-26 trophies") && strings.Contains(joined, "+0 trophies"), "trophy values or zero-result attack lost")
	values, e := cache.Do(ctx, cache.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	must(e)
	mobile, discord := 0, 0
	for _, v := range values {
		if v.FieldValues["topic"] == "legend" {
			mobile++
		}
		if v.FieldValues["topic"] == "discord_legend" {
			discord++
		}
	}
	check(mobile == 1 && discord == 3, "mobile/Discord event separation changed")
	stop()
	for range 2 {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			panic("service shutdown timed out")
		}
	}
	fmt.Printf("PASS raw Legend delivery: profiles/groups=0 raw_polls=%d Discord=3 mobile=1 SQL-before-send=true first-seen-silent=true replay-suppressed=true thread-routing=true trophies=+40,-26,+0\n", rawCalls.Load())
}
