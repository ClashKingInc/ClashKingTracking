// Exercise the actual battlelogs domain with loopback Clash responses and the
// canonical disposable database/Valkey fixture. No public service is contacted.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func main() {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" || os.Getenv("CLASHKING_DISPOSABLE_VALKEY") != "1" {
		panic("Disposable fixture required")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	if !strings.Contains(dsn, "@127.0.0.1:") {
		panic("Loopback database required")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{os.Getenv("TEST_VALKEY_ADDR")}})
	must(err)
	defer cache.Close()
	_, err = db.Exec(ctx, "INSERT INTO basic_player(tag,name,league_id,townhall_level) VALUES('#PYLQ','Streak fixture',105000036,18)")
	must(err)
	var mu sync.RWMutex
	var entries []clashy.BattleLogEntry
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/players/#PYLQ/battlelog") {
			http.Error(w, "Unexpected request", 500)
			return
		}
		mu.RLock()
		defer mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		must(json.NewEncoder(w).Encode(map[string]any{"items": entries}))
	}))
	defer server.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = server.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	client, err := clashy.NewClient(cfg)
	must(err)
	defer client.Close()
	origin := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	hit := func(minute, stars int, attack bool, mode clashy.BattleType) clashy.BattleLogEntry {
		return clashy.BattleLogEntry{BattleType: mode, Attack: attack, OpponentPlayerTag: "#PYLR", OpponentTownHallLevel: 17, Stars: stars, DestructionPercentage: 100, Duration: 90, Timestamp: origin.Add(time.Duration(minute) * time.Minute).Format("20060102T150405.000Z")}
	}
	triple := func(minute int) clashy.BattleLogEntry { return hit(minute, 3, true, clashy.BattleTypeLegend) }
	run := func(values []clashy.BattleLogEntry, want int64, lastMinute int, concurrent bool) {
		mu.Lock()
		entries = values
		mu.Unlock()
		// Simulate a lost checkpoint to replay the same API page. SQL idempotence
		// must still hold; checkpoint persistence alone is not the protection.
		must(cache.Do(ctx, cache.B().Del().Key("bl:#PYLQ").Build()).Error())
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 2)
		start := func() {
			app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: os.Getenv("TEST_VALKEY_ADDR"), BattlelogRequestsPerSecond: 5, BattlelogPriorityRequestsPerSecond: 5, BattlelogCheckpointTTLDays: 7, BattlelogFirstSeenLookbackDays: 14, TrackedClanSnapshotPrefix: "fixture:", EventStreamName: "fixture-events", EventStreamRetentionSeconds: 3600}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
			go func() { done <- scripts.NewBattlelogsDomain().Run(runCtx, app) }()
		}
		start()
		workers := 1
		if concurrent {
			start()
			workers = 2
		}
		matched := false
		for i := 0; i < 200; i++ {
			var count int64
			var latest time.Time
			err := db.QueryRow(ctx, "SELECT current_streak,last_attack_at FROM legend_attack_streaks WHERE player_tag='#PYLQ'").Scan(&count, &latest)
			if err == nil && count == want && latest.Equal(origin.Add(time.Duration(lastMinute)*time.Minute)) {
				matched = true
				break
			}
			select {
			case err := <-done:
				cancel()
				panic(fmt.Sprintf("Domain exited: %v", err))
			default:
			}
			time.Sleep(100 * time.Millisecond)
		}
		// Let a second poll replay data before checking the stable result.
		if matched {
			time.Sleep(1500 * time.Millisecond)
		}
		cancel()
		for i := 0; i < workers; i++ {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				panic("Domain shutdown timeout")
			}
		}
		var count int64
		must(db.QueryRow(ctx, "SELECT current_streak FROM legend_attack_streaks WHERE player_tag='#PYLQ'").Scan(&count))
		if !matched || count != want {
			panic(fmt.Sprintf("streak=%d want=%d matched=%v", count, want, matched))
		}
	}
	run([]clashy.BattleLogEntry{triple(1), hit(2, 2, true, clashy.BattleTypeLegend), triple(3)}, 1, 3, false)
	batch := []clashy.BattleLogEntry{triple(1), hit(2, 2, true, clashy.BattleTypeLegend), triple(3), triple(5), hit(6, 1, false, clashy.BattleTypeLegend), hit(7, 1, true, clashy.BattleTypeRanked)}
	run(batch, 2, 5, true)
	run(append(batch, hit(4, 1, true, clashy.BattleTypeLegend)), 1, 5, false)
	// Raw-retention simulation must not erase the durable ongoing counter.
	_, err = db.Exec(ctx, "DELETE FROM battles_ranked WHERE player_tag='#PYLQ'")
	must(err)
	run([]clashy.BattleLogEntry{triple(8)}, 2, 8, false)
	fmt.Println("Legend streak E2E passed: actual domain, duplicate/concurrent polls, non-triple reset, defense/Ranked excluded, delayed reset, raw-retention continuation")
}
