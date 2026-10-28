// Runs the real delivery service against canonical disposable stores and
// loopback Clash/Discord providers; it never connects to a real Discord guild.
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
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable fixtures required")
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
	const guild = "999999999999998501"
	const appID = "999999999999998502"
	const clan = "#PYLQGRJJ"
	_, e = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'Countdown fixture')", guild)
	must(e)
	defer db.Exec(context.Background(), "DELETE FROM servers WHERE id=$1", guild)
	_, e = db.Exec(ctx, "INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received) VALUES($1,'Clock clan',true,0,1,'',0,0)", clan)
	must(e)
	defer db.Exec(context.Background(), "DELETE FROM basic_clan WHERE tag=$1", clan)
	_, e = db.Exec(ctx, "INSERT INTO server_clans(server_id,tag) VALUES($1,$2)", guild, clan)
	must(e)
	kinds := []string{"clan_games_timer", "cwl_timer", "raid_weekend_timer", "season_end_timer", "season_day_timer", "war_score", "war_timer"}
	var mu sync.Mutex
	names := map[string]string{}
	types := map[string]int{}
	owners := map[string]string{}
	writes := 0
	warReads := 0
	state := "inWar"
	for i, kind := range kinds {
		id := fmt.Sprintf("9999999999999986%02d", i)
		names[id] = "Clock | Loading..."
		if i >= 5 {
			names[id] = "My clan: Loading..."
		}
		types[id] = 2
		owners[id] = guild
		var tag any
		if i >= 5 {
			tag = clan
		}
		_, e = db.Exec(ctx, "INSERT INTO server_countdowns(server_id,clan_tag,type,channel_id) VALUES($1,$2,$3,$4)", guild, tag, kind, id)
		must(e)
	}
	// Two invalid destinations must remain untouched, even though their IDs are configured.
	for i, kind := range []string{"raid_weekend_timer", "season_end_timer"} {
		id := fmt.Sprintf("9999999999999987%02d", i)
		names[id] = "Do not change"
		types[id] = 2
		owners[id] = guild
		if i == 0 {
			types[id] = 0
		} else {
			owners[id] = "999999999999998599"
		}
		server := fmt.Sprintf("9999999999999988%02d", i)
		_, e = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'Invalid destination fixture')", server)
		must(e)
		defer db.Exec(context.Background(), "DELETE FROM servers WHERE id=$1", server)
		_, e = db.Exec(ctx, "INSERT INTO server_countdowns(server_id,clan_tag,type,channel_id) VALUES($1,NULL,$2,$3)", server, kind, id)
		must(e)
	}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		defer mu.Unlock()
		if strings.HasPrefix(r.URL.Path, "/v1/") {
			warReads++
			if state == "notInWar" {
				json.NewEncoder(w).Encode(map[string]any{"state": "notInWar"})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"state": state, "teamSize": 15, "preparationStartTime": time.Now().UTC().Add(-time.Hour).Format("20060102T150405.000Z"), "startTime": time.Now().UTC().Add(time.Hour).Format("20060102T150405.000Z"), "endTime": time.Now().UTC().Add(12 * time.Hour).Format("20060102T150405.000Z"), "clan": map[string]any{"tag": clan, "stars": 32}, "opponent": map[string]any{"tag": "#PYLQGRJP", "stars": 27}})
			return
		}
		parts := strings.Split(r.URL.Path, "/")
		id := parts[len(parts)-1]
		name, ok := names[id]
		if !ok {
			w.WriteHeader(404)
			fmt.Fprint(w, `{"code":10003}`)
			return
		}
		if r.Method == "PATCH" {
			var body struct {
				Name string `json:"name"`
			}
			must(json.NewDecoder(r.Body).Decode(&body))
			names[id] = body.Name
			name = body.Name
			writes++
		}
		json.NewEncoder(w).Encode(map[string]any{"id": id, "guild_id": owners[id], "name": name, "type": types[id]})
	}))
	defer provider.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = provider.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	clash, e := clashy.NewClient(cfg)
	must(e)
	defer clash.Close()
	for phase := 0; phase < 4; phase++ {
		mu.Lock()
		before := writes
		warReads = 0
		if phase == 2 {
			state = "preparation"
		}
		if phase == 3 {
			state = "notInWar"
		}
		mu.Unlock()
		stream := fmt.Sprintf("countdown-e2e:%d:%d", os.Getpid(), phase)
		app := &platform.App{Config: platform.Config{TimescaleURL: dsn, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, DiscordGuildAllowlist: map[string]struct{}{guild: {}, "999999999999998800": {}, "999999999999998801": {}}, DiscordDeliveryBatchSize: 10, EventStreamName: stream, EventStreamReclaimIdleSeconds: 1}, Clash: clash, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
		must(app.PublishEvent(ctx, platform.Event{Topic: "countdown_e2e_marker", Value: map[string]any{}}))
		// The marker is consumed only after the startup countdown scan finishes.
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
				panic("countdown scan timed out")
			case <-time.After(20 * time.Millisecond):
			}
		}
		stop()
		<-done
		must(cache.Do(ctx, cache.B().Del().Key(stream).Build()).Error())
		mu.Lock()
		delta := writes - before
		reads := warReads
		if phase == 0 {
			check(delta == 7, fmt.Sprintf("expected seven countdown updates, got %d", delta))
			check(names["999999999999998601"] != "Clock | Loading...", "signup timer not replaced")
			check(strings.Contains(names["999999999999998605"], "32 ⭐ | 27 ⭐"), "war score wrong")
			check(strings.Contains(names["999999999999998606"], "Ends in"), "war timer wrong")
		}
		if phase == 1 {
			check(delta == 0, "unchanged channel names were renamed")
		}
		if phase == 2 {
			check(delta == 2, "preparation did not update both war channels")
			check(strings.Contains(names["999999999999998605"], "Starts in"), "preparation score misleading")
		}
		expectedReads := 1
		if phase == 3 {
			check(delta == 2, "missing war did not update both channels")
			check(strings.Contains(names["999999999999998605"], "No visible war"), "missing war was asserted idle")
			expectedReads = 2
		}
		check(reads == expectedReads, fmt.Sprintf("expected shared current-war lookup (%d requests), got %d", expectedReads, reads))
		check(names["999999999999998700"] == "Do not change" && names["999999999999998701"] == "Do not change", "invalid destination modified")
		mu.Unlock()
	}
	fmt.Println("PASS actual countdown delivery: seven timer types, live war score/time, preparation transition, shared war fetch, preserved custom prefixes, unchanged-name suppression, foreign guild and non-voice rejection; no external Discord writes")
}
