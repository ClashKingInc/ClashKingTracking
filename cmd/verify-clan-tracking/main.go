// Command verify-clan-tracking exercises the real global-clan and war-archiver
// domains against a loopback HTTP fault server and a disposable schema in the
// canonical local development database. It never uses public as a SQL fallback.
// Run with: go run ./cmd/verify-clan-tracking
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"
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
	base := "postgres://clashking_local:clashking_local@127.0.0.1:54330/clashking_dev"
	pool, err := pgxpool.New(ctx, base)
	must(err)
	defer pool.Close()
	schema := fmt.Sprintf("clan_refine_e2e_%d", time.Now().UnixNano())
	_, err = pool.Exec(ctx, `CREATE SCHEMA `+schema)
	must(err)
	defer func() {
		_, err := pool.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
		must(err)
		fmt.Println("DISPOSABLE_SCHEMA_REMOVED", schema)
	}()
	for _, table := range []string{"basic_clan", "basic_player", "clan_records", "clan_change_history", "join_leave_history", "wars", "war_schedule", "player_timers", "war_archive_pending", "war_archive_packs", "player_war_history", "cwl_groups", "cwl_group_clans", "cwl_group_members", "war_triple_streak_results", "war_triple_streaks"} {
		_, err = pool.Exec(ctx, `CREATE TABLE `+schema+`.`+table+` (LIKE public.`+table+` INCLUDING ALL)`)
		must(err)
	}
	dsn := base + "?search_path=" + schema // No public fallback: every runtime SQL write stays isolated.
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	for _, tag := range []string{"#PYY", "#PYP", "#PYQ", "#PYG", "#PYL", "#PYC", "#PYJ", "#PYR"} {
		_, err = db.Exec(ctx, `INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received,last_active) VALUES($1,'seed',true,0,10,'',0,0,CASE WHEN $1 IN ('#PYY','#PYP') THEN now() ELSE NULL END)`, tag)
		must(err)
	}
	var mode atomic.Int32
	var calls atomic.Int64
	warResponses := map[string]any{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if mode.Load() == 3 && (strings.HasSuffix(r.URL.Path, "/currentwar") || strings.Contains(r.URL.Path, "/clanwarleagues/wars/")) {
			key := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/v1/clans/"), "/currentwar")
			if strings.Contains(r.URL.Path, "/clanwarleagues/wars/") {
				key = r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
			}
			_ = json.NewEncoder(w).Encode(warResponses[key])
			return
		}
		if strings.HasSuffix(r.URL.Path, "/currentwar") {
			w.WriteHeader(403)
			fmt.Fprint(w, `{"reason":"accessDenied"}`)
			return
		}
		if mode.Load() == 1 {
			w.WriteHeader(503)
			fmt.Fprint(w, `{"reason":"inMaintenance"}`)
			return
		}
		if mode.Load() == 2 {
			w.WriteHeader(503)
			fmt.Fprint(w, `{"reason":"unknown"}`)
			return
		}
		if strings.HasSuffix(r.URL.Path, "#PYY") {
			w.WriteHeader(502)
			fmt.Fprint(w, `{"reason":"badGateway","message":"upstream deadline"}`)
			return
		}
		tag := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		_ = json.NewEncoder(w).Encode(map[string]any{"tag": tag, "name": "refreshed", "members": 10, "isWarLogPublic": true, "clanLevel": 10, "warWins": 5, "warWinStreak": 0, "clanPoints": 1000, "clanBuilderBasePoints": 100, "badgeUrls": map[string]string{"large": "https://example.invalid/badge.png"}, "warLeague": map[string]any{"id": 48000001, "name": "Bronze"}, "memberList": []any{}})
	})}
	go server.Serve(listener)
	defer server.Close()
	clientConfig := clashy.DefaultClientConfig()
	clientConfig.BaseURL = "http://" + listener.Addr().String() + "/v1"
	clientConfig.LookupCache = false
	clientConfig.UpdateCache = false
	client, err := clashy.NewClient(clientConfig)
	must(err)
	defer client.Close()
	newApp := func() *platform.App {
		return &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: "isolated-validation", GlobalClanPriorityRequestsPerSecond: 20, GlobalClanNonPriorityRequestsPerSecond: 20, GlobalClanWriteWorkers: 1, TargetPageMultiplier: 1, WarArchiveRequestsPerSecond: 20, WarArchiveScanSeconds: 1, WarArchivePackSize: 10000, WarArchiveEndpoint: "http://127.0.0.1:1", WarArchiveBucket: "disabled", WarArchiveAccessKeyID: "local", WarArchiveSecretAccessKey: "local", CWLRequestsPerSecond: 1, CWLWarRequestsPerSecond: 1, CWLSyncSeconds: 3600}, Logger: slog.New(slog.NewJSONHandler(os.Stdout, nil)), Clash: client, Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	}
	app := newApp()
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- scripts.NewGlobalClansDomain().Run(runCtx, app) }()
	time.Sleep(7 * time.Second)
	stats := app.Stats.Snapshot()
	encoded, _ := json.Marshal(stats)
	fmt.Println("GLOBAL_STATS", string(encoded))
	var success, deferred, wraps int64
	for _, s := range stats.Domains {
		if strings.HasSuffix(s.Name, "fetch-success") {
			success += s.ProcessingCount
		}
		if strings.HasSuffix(s.Name, "fetch-deferred") {
			deferred += s.Requests
		}
		if strings.HasSuffix(s.Name, "scan-wrap") {
			wraps += s.ProcessingCount
		}
	}
	check(success > 30 && deferred > 0 && wraps > 0, "502 isolation did not preserve progress/defer/wrap")
	check(app.Availability.PausedDuration() == 0, "isolated 502 paused gate")
	var stored int
	must(db.QueryRow(ctx, `SELECT count(*) FROM basic_clan WHERE name='refreshed'`).Scan(&stored))
	check(stored > 0, "no actual SQL refresh")
	mode.Store(1)
	time.Sleep(2 * time.Second)
	countBefore := calls.Load()
	time.Sleep(2 * time.Second)
	check(calls.Load() == countBefore, "maintenance did not stop HTTP requests")
	check(app.Availability.PausedDuration() > time.Second, "maintenance did not pause gate")
	cancel()
	<-done
	fmt.Printf("GLOBAL_PASS successful=%d deferred=%d wraps=%d stored=%d maintenance_paused_seconds=%.2f\n", success, deferred, wraps, stored, app.Availability.PausedDuration().Seconds())
	mode.Store(2)
	app = newApp()
	runCtx, cancel = context.WithCancel(ctx)
	done = make(chan error, 1)
	go func() { done <- scripts.NewGlobalClansDomain().Run(runCtx, app) }()
	time.Sleep(2 * time.Second)
	check(app.Availability.PausedDuration() > time.Second, "generic 503 did not pause gate")
	cancel()
	<-done
	fmt.Println("GENERIC_503_PASS")
	mode.Store(0)
	prep := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Second)
	end := prep.Add(24 * time.Hour)
	_, err = db.Exec(ctx, `INSERT INTO war_schedule(schedule_key,war_id,source_clan_tag,opponent_tag,prep_time,end_time,next_run_at,war_type) VALUES ('terminal',123,'#PYQ','#PYG',$1,$2,$2,'regular'),('clock-fallback',124,'#PYL','#PYC',$3,$2,$2,'regular')`, prep, end, time.Time{})
	must(err)
	app = newApp()
	runCtx, cancel = context.WithCancel(ctx)
	done = make(chan error, 1)
	go func() { done <- scripts.NewWarArchiverDomain().Run(runCtx, app) }()
	time.Sleep(3 * time.Second)
	cancel()
	<-done
	var remaining int
	must(db.QueryRow(ctx, `SELECT count(*) FROM war_schedule`).Scan(&remaining))
	check(remaining == 0, "terminal schedules remained")
	var prepCount, clockCount int
	must(db.QueryRow(ctx, `SELECT count(*) FROM basic_clan WHERE tag IN ('#PYQ','#PYG') AND last_active=$1`, prep).Scan(&prepCount))
	check(prepCount == 2, "both sides did not use stored preparation time")
	must(db.QueryRow(ctx, `SELECT count(*) FROM basic_clan WHERE tag IN ('#PYL','#PYC') AND last_active>now()-interval '1 minute'`).Scan(&clockCount))
	check(clockCount == 2, "invalid prep did not use one-time clock fallback")
	fmt.Println("TERMINAL_PASS both_sides_prep=2 both_sides_clock=2 schedules_deleted=2")
	_, err = db.Exec(ctx, `UPDATE basic_clan SET last_active=NULL`)
	must(err)
	for i, pair := range [][2]string{{"#PYY", "#PYP"}, {"#PYQ", "#PYG"}, {"#PYL", "#PYC"}, {"#PYJ", "#PYR"}} {
		warTag := ""
		warType := "random"
		state := "warEnded"
		start := end.Add(-24 * time.Hour)
		if i == 1 {
			start = end.Add(-15 * time.Minute)
		}
		if i == 2 {
			warTag = "#CCL"
			warType = "cwl"
		}
		if i == 3 {
			state = "inWar"
		}
		stamp := func(t time.Time) string { return t.Format("20060102T150405.000Z") }
		war := map[string]any{"state": state, "teamSize": 5, "attacksPerMember": 2, "preparationStartTime": stamp(prep), "startTime": stamp(start), "endTime": stamp(end), "clan": map[string]any{"tag": pair[0], "name": "A", "clanLevel": 10, "attacks": 0, "stars": 0, "destructionPercentage": 0, "members": []any{}}, "opponent": map[string]any{"tag": pair[1], "name": "B", "clanLevel": 10, "attacks": 0, "stars": 0, "destructionPercentage": 0, "members": []any{}}}
		warResponses[pair[0]] = war
		// Feed complete real endpoint-shaped rosters through the archiver, not
		// a helper-only test. One player qualifies; every other member misses.
		left, right := []any{}, []any{}
		for j := 0; j < 5; j++ {
			member := map[string]any{"tag": fmt.Sprintf("#LEFT%d", j), "name": "Left", "townhallLevel": 18, "mapPosition": j + 1}
			if j == 0 {
				member["attacks"] = []any{map[string]any{"defenderTag": "#RIGHT0", "stars": 3, "order": 1}, map[string]any{"defenderTag": "#RIGHT1", "stars": 3, "order": 2}}
			}
			left = append(left, member)
			right = append(right, map[string]any{"tag": fmt.Sprintf("#RIGHT%d", j), "name": "Right", "townhallLevel": 18, "mapPosition": j + 1})
		}
		war["clan"].(map[string]any)["members"] = left
		war["opponent"].(map[string]any)["members"] = right
		warResponses[pair[1]] = war
		if warTag != "" {
			warResponses[warTag] = war
		}
		_, err = db.Exec(ctx, `INSERT INTO war_schedule(schedule_key,war_id,source_clan_tag,opponent_tag,prep_time,end_time,next_run_at,war_type,war_tag) VALUES($1,$2,$3,$4,$5,$6,$6,$7,NULLIF($8,''))`, models.ComputeWarKey(pair[0], pair[1], prep), 200+i, pair[0], pair[1], prep, end, warType, warTag)
		must(err)
	}
	_, err = db.Exec(ctx, `UPDATE basic_clan SET last_active=$1 WHERE tag='#PYP'`, end.Add(time.Hour))
	must(err)
	mode.Store(3)
	app = newApp()
	runCtx, cancel = context.WithCancel(ctx)
	done = make(chan error, 2)
	for range 2 {
		go func() { done <- scripts.NewWarArchiverDomain().Run(runCtx, app) }()
	}
	time.Sleep(4 * time.Second)
	cancel()
	<-done
	<-done
	var finished, correct, pending int
	must(db.QueryRow(ctx, `SELECT count(*) FROM wars`).Scan(&finished))
	check(finished == 3, "not all completed wars stored")
	must(db.QueryRow(ctx, `SELECT count(*) FROM basic_clan WHERE tag IN ('#PYY','#PYQ','#PYG','#PYL','#PYC') AND last_active=$1`, end).Scan(&correct))
	check(correct == 5, "stored war end times wrong")
	var newer bool
	must(db.QueryRow(ctx, `SELECT last_active=$1 FROM basic_clan WHERE tag='#PYP'`, end.Add(time.Hour)).Scan(&newer))
	check(newer, "newer activity regressed")
	must(db.QueryRow(ctx, `SELECT count(*) FROM basic_clan WHERE tag IN ('#PYJ','#PYR') AND last_active IS NULL`).Scan(&pending))
	check(pending == 2, "pending retry advanced activity")
	must(db.QueryRow(ctx, `SELECT count(*) FROM war_schedule WHERE next_run_at>$1`, end).Scan(&remaining))
	check(remaining == 1, "pending schedule not rescheduled")
	fmt.Println("STORED_WARS_PASS regular_short_friendly_cwl=3 end_time_clans=5 newer_preserved=1 pending_clans_unchanged=2 concurrent_finalizers=2")
	var streak, results int
	must(db.QueryRow(ctx, `SELECT streak FROM war_triple_streaks WHERE player_tag='#LEFT0' AND townhall_level=18`).Scan(&streak))
	check(streak == 2, "qualifying player did not receive exactly +2")
	must(db.QueryRow(ctx, `SELECT count(*) FROM war_triple_streak_results`).Scan(&results))
	check(results == 10, "CWL/friendly war counted or duplicate finalization scored twice")
	must(db.QueryRow(ctx, `SELECT count(*) FROM war_triple_streaks WHERE streak=0`).Scan(&results))
	check(results == 9, "missed attacks did not reset every other roster member")
	fmt.Println("WAR_TRIPLE_STREAK_PASS complete_rosters=2 qualified=1 resets=9 excluded_cwl_friendly=2 concurrent_finalizers=2")
	replay := func(id int, offset time.Duration, qualify, upgrade bool) {
		raw, _ := json.Marshal(warResponses["#PYY"])
		var war map[string]any
		must(json.Unmarshal(raw, &war))
		warEnd := end.Add(offset)
		warStart := warEnd.Add(-24 * time.Hour)
		warPrep := warStart.Add(-23 * time.Hour)
		stamp := func(t time.Time) string { return t.Format("20060102T150405.000Z") }
		war["preparationStartTime"] = stamp(warPrep)
		war["startTime"] = stamp(warStart)
		war["endTime"] = stamp(warEnd)
		left := war["clan"].(map[string]any)["members"].([]any)
		if !qualify {
			left[0].(map[string]any)["attacks"] = []any{}
		}
		if upgrade {
			left[0].(map[string]any)["townhallLevel"] = 19
			right := war["opponent"].(map[string]any)["members"].([]any)
			right[0].(map[string]any)["townhallLevel"] = 19
			right[1].(map[string]any)["townhallLevel"] = 19
		}
		warResponses["#PYY"] = war
		_, err = db.Exec(ctx, `INSERT INTO war_schedule(schedule_key,war_id,source_clan_tag,opponent_tag,prep_time,end_time,next_run_at,war_type) VALUES($1,$2,'#PYY','#PYP',$3,$4,now(),'random')`, models.ComputeWarKey("#PYY", "#PYP", warPrep), id, warPrep, warEnd)
		must(err)
		app = newApp()
		runCtx, cancel = context.WithCancel(ctx)
		done = make(chan error, 1)
		go func() { done <- scripts.NewWarArchiverDomain().Run(runCtx, app) }()
		time.Sleep(2 * time.Second)
		cancel()
		<-done
	}
	replay(301, 4*time.Hour, true, false)
	must(db.QueryRow(ctx, `SELECT streak FROM war_triple_streaks WHERE player_tag='#LEFT0' AND townhall_level=18`).Scan(&streak))
	check(streak == 4, "second qualifying war did not add 2")
	replay(302, 2*time.Hour, false, false)
	must(db.QueryRow(ctx, `SELECT streak FROM war_triple_streaks WHERE player_tag='#LEFT0' AND townhall_level=18`).Scan(&streak))
	check(streak == 2, "out-of-order older reset did not preserve the later suffix")
	replay(303, 6*time.Hour, false, false)
	must(db.QueryRow(ctx, `SELECT streak FROM war_triple_streaks WHERE player_tag='#LEFT0' AND townhall_level=18`).Scan(&streak))
	check(streak == 0, "latest missed war did not reset streak")
	// Restore the two attacks after the deliberate missed-war fixture.
	warResponses["#PYY"].(map[string]any)["clan"].(map[string]any)["members"].([]any)[0].(map[string]any)["attacks"] = []any{map[string]any{"defenderTag": "#RIGHT0", "stars": 3, "order": 1}, map[string]any{"defenderTag": "#RIGHT1", "stars": 3, "order": 2}}
	replay(304, 8*time.Hour, true, true)
	must(db.QueryRow(ctx, `SELECT streak FROM war_triple_streaks WHERE player_tag='#LEFT0' AND townhall_level=19`).Scan(&streak))
	check(streak == 2, "Town Hall upgrade did not use a separate key")
	var last time.Time
	must(db.QueryRow(ctx, `SELECT last_attack_time FROM war_triple_streaks WHERE player_tag='#LEFT0' AND townhall_level=19`).Scan(&last))
	check(last.Equal(end.Add(8*time.Hour)), "last attack time is not war end time")
	fmt.Println("WAR_TRIPLE_REPLAY_PASS increments=2 out_of_order_reset=1 latest_reset=1 townhall_keys=2 last_time=war_end")
}
