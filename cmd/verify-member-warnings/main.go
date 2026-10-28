// Exercises threshold delivery through the real stream consumer and canonical
// SQL settings. The only Discord destination is a loopback HTTP provider.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
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
func check(ok bool, s string) {
	if !ok {
		panic(s)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable stores required")
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback stores required")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999997901"
	const channel = "999999999999997902"
	const role = "999999999999997903"
	const appID = "999999999999997904"
	const clan = "#PYLQGRJJ"
	_, e = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'Warning fixture')", guild)
	must(e)
	defer db.Exec(context.Background(), "DELETE FROM servers WHERE id=$1", guild)
	_, e = db.Exec(ctx, "INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received) VALUES($1,'Warning clan',true,0,0,'',0,0)", clan)
	must(e)
	defer db.Exec(context.Background(), "DELETE FROM basic_clan WHERE tag=$1", clan)
	_, e = db.Exec(ctx, "INSERT INTO server_clans(server_id,tag) VALUES($1,$2)", guild, clan)
	must(e)
	_, e = db.Exec(ctx, "INSERT INTO server_clan_member_warnings(server_id,clan_tag,channel_id,below_count,above_count,role_id) VALUES($1,$2,$3,10,45,$4)", guild, clan, channel, role)
	must(e)
	var mu sync.Mutex
	var messages []map[string]any
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" {
			json.NewEncoder(w).Encode(map[string]any{"id": channel, "guild_id": guild, "type": 0})
			return
		}
		var body map[string]any
		must(json.NewDecoder(r.Body).Decode(&body))
		mu.Lock()
		messages = append(messages, body)
		mu.Unlock()
		body["id"] = "999999999999997905"
		body["channel_id"] = channel
		json.NewEncoder(w).Encode(body)
	}))
	defer provider.Close()
	stream := fmt.Sprintf("member-warning-e2e:%d", os.Getpid())
	defer cache.Do(context.Background(), cache.B().Del().Key(stream).Build())
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, DiscordGuildAllowlist: map[string]struct{}{guild: {}}, EventStreamName: stream, EventStreamReclaimIdleSeconds: 1, DiscordDeliveryBatchSize: 10}, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(ctx, app) }()
	defer func() { cancel(); <-done }()
	total := int64(0)
	publish := func(old, next int, at ...time.Time) {
		timestamp := time.Now().UTC()
		if len(at) > 0 {
			timestamp = at[0]
		}
		must(app.PublishEvent(ctx, platform.Event{Topic: "clan", Timestamp: timestamp, ClanTag: clan, Value: map[string]any{"type": "member_count", "previous_count": old, "current_count": next, "clan": map[string]any{"name": "Warning clan"}}}))
		total++
		for {
			groups, e := cache.Do(ctx, cache.B().Arbitrary("XINFO", "GROUPS", stream).Build()).ToArray()
			complete := false
			if e == nil {
				for _, g := range groups {
					v, _ := g.AsMap()
					rv, pv := v["entries-read"], v["pending"]
					n, _ := rv.AsInt64()
					p, _ := pv.AsInt64()
					if n >= total && p == 0 {
						complete = true
					}
				}
			}
			if complete {
				return
			}
			select {
			case <-ctx.Done():
				panic("warning acknowledgement timed out")
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	count := func() int { mu.Lock(); defer mu.Unlock(); return len(messages) }
	publish(44, 45)
	check(count() == 0, "equality sent warning")
	publish(45, 46)
	check(count() == 1, "above crossing missing")
	publish(46, 47)
	check(count() == 1, "staying above repeated warning")
	publish(11, 10)
	check(count() == 1, "below equality sent warning")
	publish(10, 9)
	check(count() == 2, "below crossing missing")
	publish(9, 8)
	check(count() == 2, "staying below repeated warning")
	publish(8, 20)
	check(count() == 2, "recovery sent warning")
	publish(20, 50)
	check(count() == 3, "later independent crossing missing")
	publish(10, 9, time.Now().Add(-time.Hour))
	check(count() == 3, "pre-configuration event delivered")
	mu.Lock()
	for _, m := range messages {
		mentions := m["allowed_mentions"].(map[string]any)
		roles := mentions["roles"].([]any)
		check(len(roles) == 1 && roles[0] == role, "role scope wrong")
		check(len(mentions["parse"].([]any)) == 0, "broad mentions enabled")
	}
	mu.Unlock()
	_, e = db.Exec(ctx, "DELETE FROM server_clan_member_warnings WHERE server_id=$1", guild)
	must(e)
	publish(10, 9)
	check(count() == 3, "disabled warning delivered")
	fmt.Println("PASS real member warning delivery: strict above/below crossings, equality, no repeat outside threshold, recovery, later crossing, role-only mentions and disabled settings; no external Discord writes")
}
