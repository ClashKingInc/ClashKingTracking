// Exercise repeated CWL snapshots through the running service and real SQL.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/json"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, s string) {
	if !ok {
		panic(s)
	}
}
func main() {
	dsn := os.Getenv("TEST_DATABASE_URL")
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && strings.Contains(dsn, "@127.0.0.1:"), "disposable loopback SQL required")
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	season := time.Now().UTC().Format("2006-01")
	clans := make([]map[string]any, 8)
	for c := range clans {
		members := make([]map[string]any, 15)
		for m := range members {
			members[m] = map[string]any{"tag": fmt.Sprintf("#M%02d%02d", c, m), "name": "Member", "townHallLevel": 16}
		}
		clans[c] = map[string]any{"tag": fmt.Sprintf("#CWL%02d", c), "name": "Clan", "clanLevel": 20, "members": members, "badgeUrls": map[string]string{}}
	}
	snapshot := map[string]any{"season": season, "state": "preparation", "clans": clans, "rounds": []any{map[string]any{"warTags": []string{"#0"}}}}
	var payload atomic.Value
	setPayload := func() { b, e := json.Marshal(snapshot); must(e); payload.Store(b) }
	setPayload()
	var requests atomic.Int64
	var responseStatus atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		check(strings.HasSuffix(r.URL.Path, "/currentwar/leaguegroup"), "unexpected auxiliary HTTP request")
		requests.Add(1)
		if status := int(responseStatus.Load()); status > 0 && status != 200 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			reason := "accessDenied"
			if status == 404 {
				reason = "notFound"
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"reason": reason})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload.Load().([]byte))
	}))
	defer upstream.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = upstream.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	client, err := clashy.NewClient(cfg)
	must(err)
	defer client.Close()
	_, err = db.Exec(ctx, `INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received,cwl_league_id) VALUES('#CWL00','Fixture',true,0,15,'',0,0,48000001)`)
	must(err)
	run := func() {
		// Make the real startup discovery eligible on every calendar date. The API
		// still returns this month's group with the same stable identity and roster.
		_, e := db.Exec(ctx, `UPDATE cwl_groups SET season='2000-01'`)
		must(e)
		app := &platform.App{Config: platform.Config{TimescaleURL: dsn, CWLRequestsPerSecond: 10, CWLWarRequestsPerSecond: 10, CWLSyncSeconds: 1800, TargetPageMultiplier: 1}, Clash: client, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
		runctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- scripts.NewCWLDomain().Run(runctx, app) }()
		committed := false
		for runctx.Err() == nil {
			var n int
			must(db.QueryRow(ctx, `SELECT count(*) FROM cwl_groups WHERE season=$1`, season).Scan(&n))
			if n == 1 {
				committed = true
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			panic("CWL service did not stop")
		}
		check(committed, "CWL snapshot did not commit")
	}
	rows := func(table, key string) map[string]string {
		out := map[string]string{}
		r, e := db.Query(ctx, "SELECT "+key+",xmin::text || ':' || xmax::text FROM "+table)
		must(e)
		defer r.Close()
		for r.Next() {
			var k, v string
			must(r.Scan(&k, &v))
			out[k] = v
		}
		must(r.Err())
		return out
	}
	run()
	beforeClans := rows("cwl_group_clans", "clan_tag")
	beforeMembers := rows("cwl_group_members", "tag")
	check(len(beforeClans) == 8 && len(beforeMembers) == 120, "initial snapshot incomplete")
	var walStart string
	must(db.QueryRow(ctx, `SELECT pg_current_wal_insert_lsn()::text`).Scan(&walStart))
	run()
	var repeatedWALBytes int64
	must(db.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), $1::pg_lsn)::bigint`, walStart).Scan(&repeatedWALBytes))
	sameClans := rows("cwl_group_clans", "clan_tag")
	sameMembers := rows("cwl_group_members", "tag")
	var rewritten int
	for k, v := range beforeClans {
		if sameClans[k] != v {
			rewritten++
		}
	}
	for k, v := range beforeMembers {
		if sameMembers[k] != v {
			rewritten++
		}
	}
	if os.Getenv("CWL_SNAPSHOT_BASELINE") != "1" {
		check(rewritten == 0, fmt.Sprintf("unchanged row metadata modified=%d", rewritten))
	}
	clans[0]["name"] = "Renamed clan"
	members := clans[0]["members"].([]map[string]any)
	members[0]["name"] = "Renamed member"
	members[0]["townHallLevel"] = 17
	clans[0]["members"] = members[:14]
	setPayload()
	run()
	var cn, mn string
	var th, count int
	must(db.QueryRow(ctx, `SELECT name FROM cwl_group_clans WHERE clan_tag='#CWL00'`).Scan(&cn))
	must(db.QueryRow(ctx, `SELECT name,town_hall FROM cwl_group_members WHERE tag='#M0000'`).Scan(&mn, &th))
	must(db.QueryRow(ctx, `SELECT count(*) FROM cwl_group_members`).Scan(&count))
	check(cn == "Renamed clan" && mn == "Renamed member" && th == 17 && count == 119, "changed values or stale-member removal lost")

	// Run the actual refresh service through an upstream outage, an unchanged
	// successful snapshot, and expected no-group responses. No fixture calls
	// the health helper directly: HTTP, SQL and normal polling drive recovery.
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, CWLRequestsPerSecond: 10, CWLWarRequestsPerSecond: 10, CWLSyncSeconds: 1, TargetPageMultiplier: 1}, Clash: client, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	healthCtx, stopHealth := context.WithTimeout(ctx, 25*time.Second)
	defer stopHealth()
	responseStatus.Store(403)
	healthDone := make(chan error, 1)
	go func() { healthDone <- scripts.NewCWLDomain().Run(healthCtx, app) }()
	state := func() platform.DomainStats {
		for _, row := range app.Stats.Snapshot().Domains {
			if row.Name == "cwl.groups" {
				return row
			}
		}
		return platform.DomainStats{}
	}
	await := func(test func() bool, message string) {
		for healthCtx.Err() == nil {
			if test() {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		panic(message)
	}
	await(func() bool { v := state(); return v.Errors > 0 && !v.Healthy }, "upstream failure did not remain visible")
	responseStatus.Store(200)
	await(func() bool { v := state(); return v.Healthy && v.LastError == "" && !v.LastSuccess.IsZero() }, "unchanged successful snapshot left failed health sticky")
	beforeNegative := requests.Load()
	responseStatus.Store(404)
	await(func() bool { return requests.Load() >= beforeNegative+3 && state().Healthy }, "expected no-group discovery did not stay healthy")
	errorCount := state().Errors
	beforeMoreNegative := requests.Load()
	await(func() bool { return requests.Load() >= beforeMoreNegative+3 && state().Healthy }, "no-group discovery stopped making progress")
	check(state().Errors == errorCount, "expected no-group responses counted as errors")
	stopHealth()
	select {
	case <-healthDone:
	case <-time.After(5 * time.Second):
		panic("health probe service did not stop")
	}
	fmt.Println("CWL_HEALTH_PASS upstream_failure_visible=true unchanged_snapshot_recovers=true no_group_is_not_outage=true actual_http_sql_service=true")
	fmt.Printf("CWL_SNAPSHOTS_PASS unchanged_rows=128 tuple_metadata_changes=%d repeated_wal_bytes=%d changed_values=true stale_member_removed=true upstream_calls=%d actual_service=true\n", rewritten, repeatedWALBytes, requests.Load())
}
