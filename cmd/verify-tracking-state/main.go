// verify-tracking-state drives real Tracking domains against the canonical
// disposable SQL/Valkey fixture and a loopback upstream API. It never reads .env.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, msg string) {
	if !ok {
		panic(msg)
	}
}
func await(msg string, f func() bool) {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	panic("timeout: " + msg)
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "canonical disposable fixtures required")
	dsn := os.Getenv("TEST_DATABASE_URL")
	check(strings.Contains(dsn, "@127.0.0.1:"), "loopback database required")
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{os.Getenv("TEST_VALKEY_ADDR")}})
	must(err)
	defer cache.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var probeStatus atomic.Int32
	probeStatus.Store(200)
	var oldKind atomic.Int32
	var released atomic.Bool
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	var oldResponses atomic.Int32
	var oldStarted atomic.Bool
	sendPlayer := func(w http.ResponseWriter, name string, level int) {
		must(json.NewEncoder(w).Encode(map[string]any{"tag": "#PYY", "name": name, "townHallLevel": level, "trophies": 5000, "leagueTier": map[string]any{"id": 105000036}, "clan": map[string]any{"tag": "#PYL", "name": "Fixture"}, "heroes": []any{map[string]any{"name": "Barbarian King", "level": level, "maxLevel": 100, "village": "home"}}, "heroEquipment": []any{}, "achievements": []any{}}))
	}
	sendClan := func(w http.ResponseWriter, name string, level int) {
		must(json.NewEncoder(w).Encode(map[string]any{"tag": "#PYL", "name": "Fixture", "members": 6, "clanLevel": 10, "warWins": 0, "isWarLogPublic": true, "badgeUrls": map[string]any{}, "memberList": []any{map[string]any{"tag": "#PYY", "name": name, "townHallLevel": level, "trophies": 5000, "leagueTier": map[string]any{"id": 105000036}}}}))
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/locations") {
			code := int(probeStatus.Load())
			w.WriteHeader(code)
			if code == 200 {
				fmt.Fprint(w, `{"items":[]}`)
			} else {
				if code == 503 {
					fmt.Fprint(w, `{"reason":"inMaintenance","message":"maintenance"}`)
				} else {
					fmt.Fprint(w, `{"reason":"badGateway"}`)
				}
			}
			return
		}
		isOld := strings.HasPrefix(r.URL.Path, "/old/")
		oldRequest := isOld && ((oldKind.Load() == 1 && strings.Contains(r.URL.Path, "/players/")) || (oldKind.Load() == 2 && strings.Contains(r.URL.Path, "/clans/")))
		if isOld && !oldRequest {
			<-r.Context().Done()
			return
		}
		if oldRequest && !oldStarted.CompareAndSwap(false, true) {
			<-r.Context().Done()
			return
		}
		if oldRequest && !released.Load() {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			oldResponses.Add(1)
			if strings.Contains(r.URL.Path, "/players/") {
				sendPlayer(w, "Older", 15)
			} else {
				sendClan(w, "Older", 15)
			}
			return
		}
		if strings.Contains(r.URL.Path, "/players/") {
			sendPlayer(w, "Newer", 16)
			return
		}
		if strings.Contains(r.URL.Path, "/clans/") {
			sendClan(w, "Newer", 16)
			return
		}
		http.Error(w, "unexpected fixture route", 500)
	}))
	defer server.Close()
	newApp := func(prefix string, once bool) *platform.App {
		c := clashy.DefaultClientConfig()
		c.BaseURL = server.URL + prefix + "/v1"
		c.LookupCache = false
		c.UpdateCache = false
		client, err := clashy.NewClient(c)
		must(err)
		return &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: os.Getenv("TEST_VALKEY_ADDR"), RunOnce: once,
			BasicPlayerRequestsPerSecond: 2, TargetPageMultiplier: 1, GlobalClanPriorityRequestsPerSecond: 2, GlobalClanNonPriorityRequestsPerSecond: 1, GlobalClanWriteWorkers: 1,
			EventStreamName: "tracking-state-e2e", EventStreamRetentionSeconds: 3600}, Logger: logger, Clash: client, Valkey: cache, Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	}
	runProbe := func(expectError bool) {
		app := newApp("", true)
		defer app.Clash.Close()
		err := scripts.NewAvailabilityDomain().Run(ctx, app)
		if expectError {
			check(err != nil, "expected heartbeat publication failure")
		} else {
			must(err)
		}
	}
	probeStatus.Store(503)
	runProbe(false)
	var detected bool
	must(db.QueryRow(ctx, `SELECT official_started_at IS NOT NULL AND NOT available FROM tracking_availability`).Scan(&detected))
	check(detected, "official response did not establish durable maintenance state")
	fmt.Println("PASS official maintenance detection is persisted")
	// An official outage has outlived its prior SQL/Valkey deadline. A later proxy
	// probe must retain it. The war's old end is already in the past at recovery.
	_, err = db.Exec(ctx, `INSERT INTO war_schedule(schedule_key,source_clan_tag,opponent_tag,prep_time,end_time,next_run_at)
 VALUES('recovery','#PYL','#PYC',now()-interval '1 day',now()-interval '10 minutes',now()-interval '10 minutes');
 INSERT INTO player_timers(player_tag,event_type,event_key,expires_at) VALUES('#PYY','war','recovery',now()-interval '10 minutes');
 INSERT INTO war_reminder_jobs(schedule_key,minutes_remaining,run_at) VALUES('recovery',30,now()-interval '40 minutes');
 UPDATE tracking_availability SET available=false, official_started_at=now()-interval '1 hour',changed_at=now()-interval '1 hour',observed_at=now()-interval '5 seconds',revision=1`)
	must(err)
	probeStatus.Store(502)
	runProbe(false)
	var retained bool
	must(db.QueryRow(ctx, `SELECT official_started_at IS NOT NULL AND NOT available FROM tracking_availability`).Scan(&retained))
	check(retained, "proxy erased official maintenance")
	ttl, err := cache.Do(ctx, cache.B().Ttl().Key(platform.AvailabilityStateKey).Build()).AsInt64()
	must(err)
	check(ttl > 35, "unavailable heartbeat was not refreshed")
	probeStatus.Store(200)
	// Wrong-type heartbeat forces the post-commit Valkey publication to fail.
	must(cache.Do(ctx, cache.B().Del().Key(platform.AvailabilityStateKey).Build()).Error())
	must(cache.Do(ctx, cache.B().Lpush().Key(platform.AvailabilityStateKey).Element("injected-failure").Build()).Error())
	runProbe(true)
	var end, timer, job time.Time
	must(db.QueryRow(ctx, `SELECT s.end_time,t.expires_at,j.run_at FROM war_schedule s JOIN player_timers t ON t.event_key=s.schedule_key JOIN war_reminder_jobs j ON j.schedule_key=s.schedule_key WHERE s.schedule_key='recovery'`).Scan(&end, &timer, &job))
	check(end.After(time.Now().Add(45*time.Minute)), "expired war was not extended")
	check(end.Sub(timer).Abs() < time.Second && end.Sub(job) == 30*time.Minute, "clocks did not shift together")
	must(cache.Do(ctx, cache.B().Del().Key(platform.AvailabilityStateKey).Build()).Error())
	runProbe(false)
	var retried time.Time
	must(db.QueryRow(ctx, `SELECT end_time FROM war_schedule WHERE schedule_key='recovery'`).Scan(&retried))
	check(end.Equal(retried), "recovery replay shifted clocks twice")
	fmt.Println("PASS maintenance: expired deadlines, retained official classification, refreshed heartbeat, SQL-before-Valkey failure/restart replay")
	// A proxy-only outage must not shift any game clock.
	probeStatus.Store(502)
	runProbe(false)
	probeStatus.Store(200)
	runProbe(false)
	must(db.QueryRow(ctx, `SELECT end_time FROM war_schedule WHERE schedule_key='recovery'`).Scan(&retried))
	check(end.Equal(retried), "proxy-only outage shifted war clocks")
	fmt.Println("PASS proxy-only recovery leaves game clocks unchanged")
	_, err = db.Exec(ctx, `INSERT INTO basic_player(tag,name,townhall_level,trophies) VALUES('#PYY','Seed',14,5000);
 INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received,last_active)
 VALUES('#PYL','Fixture',true,0,6,'',0,0,now())`)
	must(err)
	startDomain := func(domain platform.Domain, app *platform.App) (context.CancelFunc, <-chan error) {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- platform.Run(runCtx, app, []platform.Domain{domain}) }()
		return cancel, done
	}
	stopDomain := func(cancel context.CancelFunc, done <-chan error, app *platform.App) {
		cancel()
		select {
		case err := <-done:
			check(err == nil || errors.Is(err, context.Canceled), fmt.Sprint(err))
		case <-time.After(15 * time.Second):
			panic("domain shutdown did not finish")
		}
		must(app.Clash.Close())
	}
	// Two actual profile pollers: delay an older response until the newer one is durable.
	oldKind.Store(1)
	oldApp := newApp("/old", false)
	cancelOld, doneOld := startDomain(scripts.NewBasicPlayersDomain(), oldApp)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		panic("older profile request did not start")
	}
	newerApp := newApp("/new", false)
	cancelNew, doneNew := startDomain(scripts.NewBasicPlayersDomain(), newerApp)
	await("newer profile durable", func() bool {
		var name string
		return db.QueryRow(ctx, `SELECT name FROM basic_player WHERE tag='#PYY'`).Scan(&name) == nil && name == "Newer"
	})
	stopDomain(cancelNew, doneNew, newerApp)
	released.Store(true)
	releaseOnce.Do(func() { close(release) })
	await("older response released", func() bool { return oldResponses.Load() > 0 })
	time.Sleep(600 * time.Millisecond)
	stopDomain(cancelOld, doneOld, oldApp)
	var name string
	var hall int
	must(db.QueryRow(ctx, `SELECT p.name,p.townhall_level FROM basic_player p WHERE p.tag='#PYY'`).Scan(&name, &hall))
	check(name == "Newer" && hall == 16, "older profile overwrote newer player")
	must(db.QueryRow(ctx, `SELECT townhall_level FROM player_profile_details WHERE player_tag='#PYY'`).Scan(&hall))
	check(hall == 16, "older profile details overwrote newer details")
	fmt.Println("PASS two real basicplayers processes: delayed older profile cannot replace newer facts or details; both join on shutdown")
	// An older global-clan member summary is also fenced against a newer profile.
	release = make(chan struct{})
	released.Store(false)
	releaseOnce = sync.Once{}
	oldResponses.Store(0)
	oldStarted.Store(false)
	oldKind.Store(2)
	globalApp := newApp("/old", false)
	cancelGlobal, doneGlobal := startDomain(scripts.NewGlobalClansDomain(), globalApp)
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		panic("older clan request did not start")
	}
	newerApp = newApp("/new", false)
	cancelNew, doneNew = startDomain(scripts.NewBasicPlayersDomain(), newerApp)
	var before time.Time
	must(db.QueryRow(ctx, `SELECT observed_at FROM basic_player WHERE tag='#PYY'`).Scan(&before))
	await("newer observation fence", func() bool {
		var at time.Time
		return db.QueryRow(ctx, `SELECT observed_at FROM basic_player WHERE tag='#PYY'`).Scan(&at) == nil && at.After(before)
	})
	stopDomain(cancelNew, doneNew, newerApp)
	released.Store(true)
	releaseOnce.Do(func() { close(release) })
	await("older clan released", func() bool { return oldResponses.Load() > 0 })
	time.Sleep(700 * time.Millisecond)
	stopDomain(cancelGlobal, doneGlobal, globalApp)
	must(db.QueryRow(ctx, `SELECT name,townhall_level FROM basic_player WHERE tag='#PYY'`).Scan(&name, &hall))
	check(name == "Newer" && hall == 16, "older clan summary overwrote newer profile")
	fmt.Println("PASS globalclans writer: delayed member summary cannot replace newer player facts; shutdown joins writer")
	fmt.Println("TRACKING_STATE_E2E_PASS")
}
