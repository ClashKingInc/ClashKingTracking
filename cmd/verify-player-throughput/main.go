// This check runs the real tracked-player domain with disposable SQL/Valkey.
// A transaction-local SQL delay reproduces the observed storage flush latency.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/json"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
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
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable fixtures required")
	dsn := os.Getenv("TEST_DATABASE_URL")
	check(strings.Contains(dsn, "@127.0.0.1:"), "loopback SQL required")
	ctx := context.Background()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{os.Getenv("TEST_VALKEY_ADDR")}})
	must(e)
	defer cache.Close()
	_, e = db.Exec(ctx, `INSERT INTO player_links(tag,source,is_verified,last_login) SELECT '#RATE'||lpad(i::text,5,'0'),'discord',true,now() FROM generate_series(1,3000)i`)
	must(e)
	_, e = db.Exec(ctx, `CREATE TABLE player_rate_transactions (id bigint PRIMARY KEY);
 CREATE FUNCTION player_rate_delay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 IF current_setting('fixture.player_rate_tx',true) IS DISTINCT FROM 'done' THEN
 PERFORM set_config('fixture.player_rate_tx','done',true);
 INSERT INTO player_rate_transactions VALUES(txid_current());
 PERFORM pg_sleep(0.020);
 END IF; RETURN NEW; END $$;
 CREATE TRIGGER player_rate_delay BEFORE INSERT OR UPDATE ON basic_player FOR EACH ROW EXECUTE FUNCTION player_rate_delay()`)
	must(e)
	var mu sync.Mutex
	var starts []time.Time
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		check(strings.HasPrefix(r.URL.Path, "/v1/players/"), "unexpected upstream path")
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		tag := strings.TrimPrefix(r.URL.Path, "/v1/players/")
		w.Header().Set("Content-Type", "application/json")
		must(json.NewEncoder(w).Encode(map[string]any{"tag": tag, "name": "Rate fixture", "townHallLevel": 16, "trophies": 5000, "leagueTier": map[string]int{"id": 105000036}, "heroes": []any{}, "troops": []any{}, "spells": []any{}, "heroEquipment": []any{}, "achievements": []any{}}))
	}))
	defer upstream.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = upstream.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	cfg.MaxBaseURLConns = 1000
	cfg.ThrottleLimit = 1000
	client, e := clashy.NewClient(cfg)
	must(e)
	defer client.Close()
	u, e := url.Parse(dsn)
	must(e)
	q := u.Query()
	q.Set("pool_max_conns", "2")
	u.RawQuery = q.Encode()
	app := &platform.App{Config: platform.Config{TimescaleURL: u.String(), ValkeyAddr: os.Getenv("TEST_VALKEY_ADDR"), TrackedPlayerRequestsPerSecond: 250, TrackedPlayerTargetRefreshSeconds: 60, TrackedClanSnapshotPrefix: "rate:", TargetPageMultiplier: 5, EventStreamName: "rate-events", EventStreamRetentionSeconds: 3600}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	run, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	started := time.Now()
	go func() { done <- scripts.NewTrackedPlayersDomain().Run(run, app) }()
	time.Sleep(8 * time.Second)
	var stored, txs int
	must(db.QueryRow(ctx, `SELECT count(*) FROM basic_player WHERE tag LIKE '#RATE%'`).Scan(&stored))
	must(db.QueryRow(ctx, `SELECT count(*) FROM player_rate_transactions`).Scan(&txs))
	elapsed := time.Since(started).Seconds()
	cancel()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		panic("domain did not stop")
	}
	mu.Lock()
	recent := 0
	for _, t := range starts {
		if t.After(started.Add(4*time.Second)) && t.Before(started.Add(8*time.Second)) {
			recent++
		}
	}
	requests := len(starts)
	mu.Unlock()
	var final int
	must(db.QueryRow(ctx, `SELECT count(*) FROM basic_player WHERE tag LIKE '#RATE%'`).Scan(&final))
	// Every published comparison snapshot must have a committed SQL profile.
	keys, e := cache.Do(ctx, cache.B().Keys().Pattern("ps:#RATE*").Build()).AsStrSlice()
	must(e)
	for _, key := range keys {
		var exists bool
		must(db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM basic_player WHERE tag=$1)`, strings.TrimPrefix(key, "ps:")).Scan(&exists))
		check(exists, "snapshot advanced before SQL")
	}
	fmt.Printf("PLAYER_THROUGHPUT configured=250 recent_http_rps=%.2f committed_rows=%d elapsed=%.2f committed_rps=%.2f transactions=%d final_rows=%d snapshots=%d requests=%d sql_delay_ms=20 pool=2\n", float64(recent)/4, stored, elapsed, float64(stored)/elapsed, txs, final, len(keys), requests)
	if os.Getenv("CLASHKING_THROUGHPUT_BASELINE") != "1" {
		check(float64(recent)/4 >= 237.5, "player HTTP rate below configured target")
		check(float64(stored)/elapsed >= 225, "committed SQL rate below target")
		check(txs < stored/5, "per-player SQL transactions remain")
	}
	fmt.Println("PASS actual trackedplayers: bounded writes, configured rate under SQL latency, SQL-before-snapshot")
}
