// Drive the real tracked-clan and Discord delivery services against disposable
// SQL/Valkey and loopback providers. No production requests or messages occur.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/scripts"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
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
func wait(ctx context.Context, msg string, f func() bool) {
	for !f() {
		select {
		case <-ctx.Done():
			panic("timeout: " + msg)
		case <-time.After(25 * time.Millisecond):
		}
	}
}
func main() {
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1" && strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "disposable loopback services required")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999992901"
	const appID = "999999999999992902"
	const hook = "999999999999992903"
	const channel = "999999999999992904"
	var attack atomic.Bool
	var failClan atomic.Bool
	var panels, feeds, groups atomic.Int32
	now := time.Now().UTC()
	stamp := func(t time.Time) string { return t.Format("20060102T150405.000Z") }
	war := func() map[string]any {
		old := map[string]any{"tag": "#PYC", "name": "Already Hit", "townhallLevel": 16, "mapPosition": 2, "attacks": []any{map[string]any{"attackerTag": "#PYC", "defenderTag": "#PYG", "stars": 2, "destructionPercentage": 80, "order": 1, "duration": 90}}}
		newMember := map[string]any{"tag": "#PYP", "name": "Attacker", "townhallLevel": 16, "mapPosition": 1}
		own := map[string]any{"tag": "#PYL", "name": "Active CWL", "clanLevel": 10, "badgeUrls": map[string]string{}, "attacks": 1, "stars": 2, "destructionPercentage": 40, "members": []any{old, newMember}}
		if attack.Load() {
			newMember["attacks"] = []any{map[string]any{"attackerTag": "#PYP", "defenderTag": "#PYG", "stars": 3, "destructionPercentage": 100, "order": 2, "duration": 90}}
			own["attacks"] = 2
			own["stars"] = 3
			own["destructionPercentage"] = 50
		}
		return map[string]any{"tag": "#CWLP", "state": "inWar", "teamSize": 2, "attacksPerMember": 1, "preparationStartTime": stamp(now.Add(-25 * time.Hour)), "startTime": stamp(now.Add(-time.Hour)), "endTime": stamp(now.Add(23 * time.Hour)), "clan": own, "opponent": map[string]any{"tag": "#PYQ", "name": "Opponent", "clanLevel": 10, "badgeUrls": map[string]string{}, "attacks": 0, "stars": 0, "destructionPercentage": 0, "members": []any{map[string]any{"tag": "#PYG", "name": "Defender", "townhallLevel": 16, "mapPosition": 1}}}}
	}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[]}`)
		case strings.HasSuffix(r.URL.Path, "/webhooks/"+hook):
			json.NewEncoder(w).Encode(map[string]any{"id": hook, "type": 1, "guild_id": guild, "channel_id": channel, "application_id": appID, "token": "fixture"})
		case strings.Contains(r.URL.Path, "/webhooks/"+hook+"/fixture"):
			var body map[string]any
			must(json.NewDecoder(r.Body).Decode(&body))
			if content, _ := body["content"].(string); strings.Contains(content, "Already Hit") {
				panic("replayed a historical attack")
			} else if strings.Contains(content, "Attacker") {
				feeds.Add(1)
			} else if body["embeds"] != nil {
				panels.Add(1)
			}
			json.NewEncoder(w).Encode(map[string]any{"id": "999999999999992905", "channel_id": channel, "type": 0})
		case strings.HasSuffix(r.URL.Path, "/currentwar/leaguegroup"):
			groups.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"season": now.Format("2006-01"), "state": "inWar", "clans": []any{}, "rounds": []any{map[string]any{"warTags": []string{"#CWLP"}}}})
		case strings.Contains(r.URL.Path, "/clanwarleagues/wars/"):
			json.NewEncoder(w).Encode(war())
		case strings.HasSuffix(r.URL.Path, "/currentwar"):
			fmt.Fprint(w, `{"state":"notInWar"}`)
		case strings.Contains(r.URL.Path, "/clans/"):
			if failClan.Load() {
				// A 5xx closes the shared maintenance gate; this fixture has no
				// maintenance controller. Use a real non-outage error to isolate
				// clan health recovery without blocking later CWL discovery.
				w.WriteHeader(http.StatusForbidden)
				fmt.Fprint(w, `{"reason":"gatewayTimeout"}`)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"tag": "#PYL", "name": "Active CWL", "clanLevel": 10, "members": 1, "memberList": []any{}, "badgeUrls": map[string]string{}})
		default:
			http.Error(w, "unexpected route "+r.URL.Path, 404)
		}
	}))
	defer provider.Close()
	_, e = db.Exec(ctx, `INSERT INTO servers(id,name,last_command_at) VALUES($1,'CWL discovery fixture',now());
 `, guild)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received) VALUES('#PYL','CWL fixture',true,0,1,'',0,0)`)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO server_clans(server_id,tag) VALUES($1,'#PYL')`, guild)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO server_logs(server_id,clan_tag,type,webhook_id) VALUES($1,'#PYL','war_panel',$2),($1,'#PYL','war_log',$2)`, guild, hook)
	must(e)
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = provider.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	client, e := clashy.NewClient(cfg)
	must(e)
	defer client.Close()
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, TrackedClanRequestsPerSecond: 50, TrackedClanTargetRefreshSeconds: 1, TrackedClanSnapshotPrefix: "cwl-discovery:", TrackedClanCWLStateSnapshot: "cwlstate", CWLSyncSeconds: 1, EventStreamName: "cwl-discovery-events", EventStreamRetentionSeconds: 300, EventStreamReclaimIdleSeconds: 1, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", DiscordDeliveryBatchSize: 20}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	deliveryCtx, stopDelivery := context.WithCancel(ctx)
	defer stopDelivery()
	deliveryDone := make(chan error, 1)
	go func() {
		err := scripts.NewDiscordDeliveryDomain().Run(deliveryCtx, app)
		if deliveryCtx.Err() == nil {
			panic(fmt.Sprintf("delivery exited early: %v", err))
		}
		deliveryDone <- err
	}()
	run := func() context.CancelFunc {
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() {
			err := scripts.NewTrackedClansDomain().Run(runCtx, app)
			if runCtx.Err() == nil {
				panic(fmt.Sprintf("tracking exited early: %v", err))
			}
			done <- err
		}()
		return func() {
			stop()
			select {
			case <-done:
			case <-ctx.Done():
				panic("tracker did not stop")
			}
		}
	}
	stop := run()
	wait(ctx, "cold-start CWL panel", func() bool { return panels.Load() > 0 })
	check(feeds.Load() == 0, "cold start replayed old attacks")
	clanHealth := func() (bool, int64) {
		for _, domain := range app.Stats.Snapshot().Domains {
			if domain.Name == "trackedclans.clans" {
				return domain.Healthy, domain.Requests
			}
		}
		return true, 0
	}
	failClan.Store(true)
	wait(ctx, "tracked clan upstream error", func() bool { healthy, _ := clanHealth(); return !healthy })
	_, failedRequests := clanHealth()
	failClan.Store(false)
	wait(ctx, "unchanged tracked clan recovers health", func() bool { healthy, requests := clanHealth(); return healthy && requests > failedRequests })
	fmt.Println("PASS tracked-clan health recovers after upstream 403 and unchanged successful snapshot")
	attack.Store(true)
	wait(ctx, "new CWL attack delivered", func() bool { return feeds.Load() > 0 })
	stop()
	check(groups.Load() == 1, "unchanged active group repeatedly rediscovered")
	fmt.Println("PASS cold-start CWL discovery, initial panel and subsequent attack feed through real services")
	// A terminal snapshot from last month must not suppress this month's group.
	stale, _ := json.Marshal(map[string]any{"season": now.AddDate(0, -1, 0).Format("2006-01"), "group_state": "ended", "ended": true})
	must(cache.Do(ctx, cache.B().Set().Key("cwl-discovery:cwlstate:#PYL").Value(valkey.BinaryString(utils.Compress(stale))).Build()).Error())
	stop = run()
	wait(ctx, "previous-season rediscovery", func() bool { return groups.Load() >= 2 })
	stop()
	fmt.Println("PASS previous-season terminal state rediscovers current CWL")
	// Known nonparticipants stay suppressed after signup instead of being fetched every cycle.
	terminal, _ := json.Marshal(map[string]any{"season": now.Format("2006-01"), "group_state": "notInWar", "no_spin": true})
	must(cache.Do(ctx, cache.B().Set().Key("cwl-discovery:cwlstate:#PYL").Value(valkey.BinaryString(utils.Compress(terminal))).Build()).Error())
	before := groups.Load()
	stop = run()
	time.Sleep(1200 * time.Millisecond)
	stop()
	check(groups.Load() == before, "known nonparticipant was polled again")
	fmt.Println("PASS current-season nonparticipant remains suppressed; no production calls")
	stopDelivery()
	select {
	case <-deliveryDone:
	case <-ctx.Done():
		panic("delivery did not stop")
	}
}
