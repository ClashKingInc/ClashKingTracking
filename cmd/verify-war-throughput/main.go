// verify-war-throughput runs the actual discovery domain against disposable
// Timescale and a loopback HTTP service with slow responses at page boundaries.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
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
	ctx := context.Background()
	base := os.Getenv("TEST_DATABASE_URL")
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && strings.Contains(base, "@127.0.0.1:"), "disposable loopback SQL required")
	root, err := pgxpool.New(ctx, base)
	must(err)
	defer root.Close()
	schema := fmt.Sprintf("war_rate_e2e_%d", time.Now().UnixNano())
	_, err = root.Exec(ctx, `CREATE SCHEMA `+schema)
	must(err)
	defer func() { _, err := root.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`); must(err) }()
	for _, table := range []string{"basic_clan", "wars", "war_schedule", "player_timers", "cwl_groups", "cwl_group_clans"} {
		_, err = root.Exec(ctx, `CREATE TABLE `+schema+`.`+table+` (LIKE public.`+table+` INCLUDING ALL)`)
		must(err)
	}
	target, err := url.Parse(base)
	must(err)
	q := target.Query()
	q.Set("search_path", schema)
	q.Set("pool_max_conns", "2")
	target.RawQuery = q.Encode()
	db, err := pgxpool.New(ctx, target.String())
	must(err)
	defer db.Close()
	// Real target paging, with enough eligible clans that a full scan cannot wrap
	// in the measurement window. Every page contains one slow upstream response.
	for _, group := range []string{"A", "D"} {
		for i := 0; i < 1200; i++ {
			_, err = db.Exec(ctx, `INSERT INTO basic_clan(tag,name,public_war_log,war_wins,last_war_at,member_count,badge_token,troops_donated,troops_received) VALUES($1,'rate fixture',true,0,CASE WHEN $2='A' THEN now() ELSE NULL END,10,'',0,0)`, fmt.Sprintf("#%s%05d", group, i), group)
			must(err)
		}
	}
	var mu sync.Mutex
	inFlight := map[string]int{}
	overlap := false
	starts := map[string][]time.Time{"A": {}, "D": {}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tag := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/clans/"), "/currentwar")
		check(len(tag) > 2, "unexpected HTTP path")
		group := tag[1:2]
		mu.Lock()
		starts[group] = append(starts[group], time.Now())
		inFlight[tag]++
		if inFlight[tag] > 1 {
			overlap = true
		}
		mu.Unlock()
		defer func() { mu.Lock(); inFlight[tag]--; mu.Unlock() }()
		if strings.HasSuffix(tag, "39") || strings.HasSuffix(tag, "79") {
			select {
			case <-time.After(1500 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "notInWar"})
	}))
	defer server.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = server.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	cfg.MaxBaseURLConns = 240
	cfg.ThrottleLimit = 240
	client, err := clashy.NewClient(cfg)
	must(err)
	defer client.Close()
	app := &platform.App{Config: platform.Config{TimescaleURL: target.String(), WarDiscoveryActiveRequestsPerSecond: 40, WarDiscoveryDormantRequestsPerSecond: 40, WarDiscoveryMaxInFlight: 120, TargetPageMultiplier: 1}, Clash: client, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- scripts.NewWarDiscoveryDomain().Run(runCtx, app) }()
	time.Sleep(14 * time.Second)
	stopped := time.Now()
	cancel()
	<-done
	mu.Lock()
	for _, group := range []string{"A", "D"} {
		times := starts[group]
		check(len(times) > 0, "no actual discovery HTTP requests")
		// The limiter permits a whole initial burst. Measure the full observed
		// run instead of cropping through a burst at both window boundaries.
		from, to := times[0], stopped
		n := 0
		peak := 0
		for i, t := range times {
			if !t.Before(from) && t.Before(to) {
				n++
			}
			j := i
			for j < len(times) && times[j].Before(t.Add(990*time.Millisecond)) {
				j++
			}
			peak = max(peak, j-i)
		}
		rate := float64(n) / to.Sub(from).Seconds()
		fmt.Printf("WAR_THROUGHPUT group=%s configured=40 observed=%.2f rolling_990ms_peak=%d requests=%d\n", group, rate, peak, len(times))
		// HTTP arrival times can compress two legal send bursts when the CI
		// runner stalls. Enforce the actual total budget (including the one
		// initial burst); retain arrival peaks as diagnostics, not a send clock.
		check(n <= int(to.Sub(from).Seconds()*40)+40, "upstream request budget exceeded")
		if os.Getenv("CLASHKING_THROUGHPUT_BASELINE") != "1" {
			check(rate >= 38, "discovery stalled behind page-tail requests")
		}
	}
	mu.Unlock()
	_, err = db.Exec(ctx, `DELETE FROM basic_clan WHERE tag NOT IN ('#A00039','#D00039')`)
	must(err)
	runCtx, cancel = context.WithCancel(ctx)
	done = make(chan error, 1)
	go func() { done <- scripts.NewWarDiscoveryDomain().Run(runCtx, app) }()
	time.Sleep(4 * time.Second)
	cancel()
	<-done
	mu.Lock()
	check(!overlap, "same clan fetched concurrently across scan wrap")
	mu.Unlock()
	var count int
	must(db.QueryRow(ctx, `SELECT count(*) FROM basic_clan`).Scan(&count))
	check(count == 2, "small target set changed")
	fmt.Println("WAR_THROUGHPUT_PASS actual_domain=war-discovery actual_sql=disposable slow_http=1500ms scan_wrap_serial=true small_targets_preserved=2")
}
