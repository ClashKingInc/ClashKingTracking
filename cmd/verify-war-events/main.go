// Exercises the actual delivery service and canonical database, with Discord
// and Clash requests terminating at a loopback provider.
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
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback only")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999998501"
	const foreign = "999999999999998599"
	const appID = "999999999999998502"
	const clan = "#PYLQGRJJ"
	for _, id := range []string{guild, foreign} {
		_, e = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'War event fixture')", id)
		must(e)
		defer db.Exec(context.Background(), "DELETE FROM servers WHERE id=$1", id)
	}
	_, e = db.Exec(ctx, "INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received) VALUES($1,'War fixture',true,0,1,'',0,0)", clan)
	must(e)
	defer db.Exec(context.Background(), "DELETE FROM basic_clan WHERE tag=$1", clan)
	for _, id := range []string{guild, foreign} {
		_, e = db.Exec(ctx, "INSERT INTO server_clans(server_id,tag,war_events_enabled) VALUES($1,$2,true)", id, clan)
		must(e)
	}
	var mu sync.Mutex
	now := time.Now().UTC().Truncate(time.Second)
	prep := now.Add(-time.Hour)
	finish := now.Add(24 * time.Hour)
	state := "preparation"
	cwl := false
	creates, updates, reads := 0, 0, 0
	events := []map[string]any{}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			reads++
			if strings.HasSuffix(r.URL.Path, "/currentwar") && cwl {
				json.NewEncoder(w).Encode(map[string]any{"state": "notInWar"})
				return
			}
			if strings.HasSuffix(r.URL.Path, "/leaguegroup") {
				json.NewEncoder(w).Encode(map[string]any{"state": "inWar", "season": "2026-10", "rounds": []any{map[string]any{"warTags": []string{"#PYLQ"}}}})
				return
			}
			body := map[string]any{"state": state, "teamSize": 15, "preparationStartTime": prep.Format("20060102T150405.000Z"), "startTime": prep.Add(24 * time.Hour).Format("20060102T150405.000Z"), "endTime": finish.Format("20060102T150405.000Z"), "clan": map[string]any{"tag": clan, "name": "Fixture clan"}, "opponent": map[string]any{"tag": "#PYLQGRJP", "name": "Opponent"}}
			if cwl {
				body["tag"] = "#PYLQ"
			}
			json.NewEncoder(w).Encode(body)
			return
		}
		if r.URL.Path == "/applications/"+appID+"/emojis" {
			fmt.Fprint(w, `{"items":[]}`)
			return
		}
		expected := "/guilds/" + guild + "/scheduled-events"
		if r.URL.Path != expected && !strings.HasPrefix(r.URL.Path, expected+"/") {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"code":10003}`)
			return
		}
		if r.Method == "GET" {
			json.NewEncoder(w).Encode(events)
			return
		}
		var body map[string]any
		must(json.NewDecoder(r.Body).Decode(&body))
		if r.Method == "POST" {
			check(body["entity_type"] == float64(3) && body["privacy_level"] == float64(2), "wrong event type/privacy")
			start, e := time.Parse(time.RFC3339, body["scheduled_start_time"].(string))
			must(e)
			check(start.After(time.Now()) && start.Before(time.Now().Add(time.Minute)), "not immediate future")
			end, e := time.Parse(time.RFC3339, body["scheduled_end_time"].(string))
			must(e)
			check(end.Equal(finish), "incorrect event end")
			check(!strings.Contains(body["description"].(string), "<@"), "mention in event")
			creates++
			body["id"] = fmt.Sprintf("999999999999997%03d", creates)
			body["guild_id"] = guild
			body["creator_id"] = appID
			body["status"] = 1
			events = append(events, body)
			json.NewEncoder(w).Encode(body)
			return
		}
		if r.Method == "PATCH" {
			updates++
			for _, event := range events {
				if strings.HasSuffix(r.URL.Path, event["id"].(string)) {
					event["scheduled_end_time"] = body["scheduled_end_time"]
					json.NewEncoder(w).Encode(event)
					return
				}
			}
		}
		w.WriteHeader(400)
	}))
	defer provider.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = provider.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	clash, e := clashy.NewClient(cfg)
	must(e)
	defer clash.Close()
	for phase := 0; phase < 7; phase++ {
		mu.Lock()
		beforeC, beforeU := creates, updates
		reads = 0
		switch phase {
		case 2:
			state = "inWar"
			finish = finish.Add(time.Hour)
		case 3:
			state = "warEnded"
		case 4:
			state = "preparation"
			prep = prep.Add(48 * time.Hour)
			finish = finish.Add(48 * time.Hour)
		case 5:
			state = "inWar"
			cwl = true
		case 6:
			state = "notInWar"
			cwl = false
		}
		mu.Unlock()
		if phase == 4 {
			_, e = db.Exec(ctx, "UPDATE server_clans SET war_events_enabled=false WHERE server_id=$1", guild)
			must(e)
		}
		if phase == 5 {
			_, e = db.Exec(ctx, "UPDATE server_clans SET war_events_enabled=true WHERE server_id=$1", guild)
			must(e)
		}
		// Put the next CWL preparation in the past so startup can recover its event.
		if phase == 5 {
			mu.Lock()
			prep = now.Add(-30 * time.Minute)
			mu.Unlock()
		}
		stream := fmt.Sprintf("war-events:%d:%d", os.Getpid(), phase)
		app := &platform.App{Config: platform.Config{TimescaleURL: dsn, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, DiscordGuildAllowlist: map[string]struct{}{guild: {}}, DiscordDeliveryBatchSize: 10, EventStreamName: stream, EventStreamReclaimIdleSeconds: 1}, Clash: clash, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
		topic := "war_events_marker"
		value := map[string]any{}
		if phase == 1 {
			topic = "war"
			value["type"] = "new_war"
		}
		must(app.PublishEvent(ctx, platform.Event{Topic: topic, ClanTag: clan, Timestamp: time.Now(), Value: value}))
		for {
			groups, e := cache.Do(ctx, cache.B().Arbitrary("XINFO", "GROUPS", stream).Build()).ToArray()
			complete := false
			if e == nil {
				for _, g := range groups {
					v, _ := g.AsMap()
					read, pending := v["entries-read"], v["pending"]
					n, _ := read.AsInt64()
					p, _ := pending.AsInt64()
					if n >= 1 && p == 0 {
						complete = true
					}
				}
			}
			if complete {
				break
			}
			select {
			case <-ctx.Done():
				panic("worker did not complete")
			case <-time.After(20 * time.Millisecond):
			}
		}
		stop()
		<-done
		must(cache.Do(ctx, cache.B().Del().Key(stream).Build()).Error())
		mu.Lock()
		deltaC, deltaU := creates-beforeC, updates-beforeU
		expectedC, expectedU := 0, 0
		if phase == 0 || phase == 5 {
			expectedC = 1
		}
		if phase == 2 {
			expectedU = 1
		}
		check(deltaC == expectedC && deltaU == expectedU, fmt.Sprintf("phase %d: created %d updated %d", phase, deltaC, deltaU))
		if phase == 4 {
			check(reads == 0, "disabled config made provider request")
		}
		if phase == 5 {
			check(strings.HasPrefix(events[len(events)-1]["name"].(string), "CWL"), "CWL event mislabeled")
		}
		mu.Unlock()
	}
	fmt.Println("PASS real war-event delivery: startup recovery, replay suppression, changed end time, ended/disabled suppression, CWL lookup, guild allowlist; zero external writes")
}
