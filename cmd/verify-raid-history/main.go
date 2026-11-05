// Exercises the actual one-pass capital domain with disposable stores and a
// loopback Clash provider. No external services or Discord writes are used.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
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
func check(b bool, s string) {
	if !b {
		panic(s)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable stores required")
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback stores required")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999994901"
	const clan = "#PYLQGRJJ"
	_, e = db.Exec(ctx, "INSERT INTO servers(id,name,last_command_at) VALUES($1,'Raid fixture',now())", guild)
	must(e)
	_, e = db.Exec(ctx, "INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received) VALUES($1,'Raid clan',true,0,1,'',0,0)", clan)
	must(e)
	_, e = db.Exec(ctx, "INSERT INTO server_clans(server_id,tag) VALUES($1,$2)", guild, clan)
	must(e)
	var stage atomic.Int32
	var requests atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		check(r.URL.Query().Get("limit") == "10", "raid history not requested")
		w.Header().Set("Content-Type", "application/json")
		state, loot := "ended", 1000
		if stage.Load() == 1 {
			state, loot = "ongoing", 100
		}
		if stage.Load() == 2 {
			loot = 1100
		}
		json.NewEncoder(w).Encode(map[string]any{"items": []any{
			map[string]any{"state": state, "startTime": "20261016T070000.000Z", "endTime": "20261019T070000.000Z", "capitalTotalLoot": loot, "members": []any{map[string]any{"tag": "#PYLQGRJP", "name": "One", "attacks": 6, "capitalResourcesLooted": loot}}},
			map[string]any{"state": "ended", "startTime": "20261009T070000.000Z", "endTime": "20261012T070000.000Z", "capitalTotalLoot": 500, "members": []any{}},
		}})
	}))
	defer provider.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = provider.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	clash, e := clashy.NewClient(cfg)
	must(e)
	defer clash.Close()
	app := &platform.App{Config: platform.Config{RunOnce: true, TimescaleURL: dsn, CapitalRequestsPerSecond: 10, CapitalTargetRefreshSeconds: 60, CapitalSnapshotPrefix: "raid-history-fixture:", EventStreamName: "raid-history-events"}, Clash: clash, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	run := func() { must(scripts.NewCapitalDomain().Run(ctx, app)) }
	run()
	var count int
	must(db.QueryRow(ctx, "SELECT count(*) FROM capital_raid_history WHERE clan_tag=$1", clan).Scan(&count))
	check(count == 2, "available raid history not seeded")
	stage.Store(1)
	run()
	var state string
	var loot int
	must(db.QueryRow(ctx, "SELECT state,(data->>'capitalTotalLoot')::int FROM capital_raid_history WHERE clan_tag=$1 ORDER BY start_time DESC LIMIT 1", clan).Scan(&state, &loot))
	check(state == "ended" && loot == 1000, "final snapshot regressed")
	_, e = db.Exec(ctx, "DELETE FROM capital_raid_history WHERE clan_tag=$1 AND start_time='2026-10-09 07:00:00Z'", clan)
	must(e)
	run()
	must(db.QueryRow(ctx, "SELECT count(*) FROM capital_raid_history WHERE clan_tag=$1", clan).Scan(&count))
	check(count == 2, "cache fast path prevented history restore")
	stage.Store(2)
	run()
	must(db.QueryRow(ctx, "SELECT (data->>'capitalTotalLoot')::int FROM capital_raid_history WHERE clan_tag=$1 ORDER BY start_time DESC LIMIT 1", clan).Scan(&loot))
	check(loot == 1100, "final correction not retained")
	events, e := cache.Do(ctx, cache.B().Xlen().Key("raid-history-events").Build()).AsInt64()
	must(e)
	check(events == 0, "history-only clan emitted notifications")
	check(requests.Load() == 4, "unexpected provider request count")
	fmt.Println("PASS actual capital domain: linked clan history, available older weekends, terminal-state protection, cache-hit backfill, final correction, one-pass mode, no extra notifications")
}
