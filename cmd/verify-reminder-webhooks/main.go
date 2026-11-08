// Exercise the actual delivery domain with disposable stores and loopback Discord.
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
	"sync/atomic"
	"time"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func check(b bool, m string) {
	if !b {
		panic(m)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable fixtures required")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dsn := os.Getenv("TEST_DATABASE_URL")
	addr := os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback required")
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999992901"
	const appID = "999999999999992902"
	const channel = "999999999999992903"
	const thread = "999999999999992904"
	const hook = "999999999999992905"
	const badHook = "999999999999992906"
	var sent, botMessages, badReads, raidReads atomic.Int32
	var currentRaid atomic.Bool
	var contents []string
	globalAvatar := os.Getenv("CLASHKING_E2E_GLOBAL_AVATAR") == "1"
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/capitalraidseasons"):
			raidReads.Add(1)
			now := time.Now().UTC()
			days := (int(time.Monday) - int(now.Weekday()) + 7) % 7
			end := time.Date(now.Year(), now.Month(), now.Day(), 7, 0, 0, 0, time.UTC).AddDate(0, 0, days)
			if !end.After(now) {
				end = end.AddDate(0, 0, 7)
			}
			start := now.Add(-time.Hour)
			state := "ongoing"
			if !currentRaid.Load() {
				start = now.Add(-7 * 24 * time.Hour)
				end = now.Add(-4 * 24 * time.Hour)
				state = "ended"
			}
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"state": state, "startTime": start.Format("20060102T150405.000Z"), "endTime": end.Format("20060102T150405.000Z"), "members": []any{}}}})
		case strings.HasSuffix(r.URL.Path, "/clans/#PYLQ"):
			json.NewEncoder(w).Encode(map[string]any{"tag": "#PYLQ", "name": "Fixture Clan", "memberList": []any{map[string]any{"tag": "#PYQ", "name": "Fresh Raid Player", "townHallLevel": 16, "role": "member"}}})
		case strings.HasSuffix(r.URL.Path, "/guilds/"+guild+"/members/"+appID):
			profile := map[string]any{"user": map[string]any{"id": appID, "username": "ClashKing", "avatar": "globalhash", "discriminator": "0"}}
			if !globalAvatar {
				profile["nick"] = "Guild Bot"
				profile["avatar"] = "guildhash"
			}
			json.NewEncoder(w).Encode(profile)
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[]}`)
		case strings.HasSuffix(r.URL.Path, "/webhooks/"+hook) || strings.HasSuffix(r.URL.Path, "/webhooks/"+badHook):
			owner := appID
			if strings.HasSuffix(r.URL.Path, badHook) {
				owner = "999999999999992999"
				badReads.Add(1)
			}
			json.NewEncoder(w).Encode(map[string]any{"id": hook, "type": 1, "guild_id": guild, "channel_id": channel, "application_id": owner, "token": "fixture"})
		case strings.HasSuffix(r.URL.Path, "/channels/"+thread):
			json.NewEncoder(w).Encode(map[string]any{"id": thread, "type": 11, "guild_id": guild, "parent_id": channel, "name": "fixture"})
		case strings.Contains(r.URL.Path, "/webhooks/"+hook+"/fixture") && r.Method == "POST":
			check(r.URL.Query().Get("thread_id") == thread, "thread not forwarded")
			var payload struct {
				Content  string `json:"content"`
				Username string `json:"username"`
				Avatar   string `json:"avatar_url"`
			}
			must(json.NewDecoder(r.Body).Decode(&payload))
			if globalAvatar {
				check(payload.Username == "ClashKing" && strings.Contains(payload.Avatar, "/avatars/"+appID+"/globalhash"), "global bot profile fallback missing")
			} else {
				check(payload.Username == "Guild Bot", "guild bot name missing")
				check(strings.Contains(payload.Avatar, "/guilds/"+guild+"/users/"+appID+"/avatars/guildhash"), "guild bot avatar missing")
			}
			contents = append(contents, payload.Content)
			sent.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"id": "999999999999992907", "channel_id": thread, "type": 0})
		case strings.Contains(r.URL.Path, "/messages") && r.Method == "POST":
			botMessages.Add(1)
			http.Error(w, "bot fallback forbidden", 500)
		default:
			http.Error(w, "unexpected fixture request", 404)
		}
	}))
	defer provider.Close()
	cc := clashy.DefaultClientConfig()
	cc.BaseURL = provider.URL + "/v1"
	cc.LookupCache = false
	cc.UpdateCache = false
	clash, e := clashy.NewClient(cc)
	must(e)
	defer clash.Close()
	_, e = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'Webhook delivery fixture')", guild)
	must(e)
	ids := []string{}
	for _, h := range []string{hook, badHook, ""} {
		var id string
		must(db.QueryRow(ctx, `INSERT INTO reminders(server_id,type,type_name,clan_tag,webhook_token,channel_id,thread_id,webhook_id,minutes_remaining) VALUES($1,1,'War','#PYLQ','',$2,$3,NULLIF($4,''),60) RETURNING id::text`, guild, channel, thread, h).Scan(&id))
		ids = append(ids, id)
	}
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ReminderRequestsPerSecond: 10, ValkeyAddr: addr, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", DiscordDeliveryBatchSize: 20, EventStreamName: "reminder-webhook-fixture", EventStreamReclaimIdleSeconds: 1}, Clash: clash, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	time.Sleep(300 * time.Millisecond)
	for _, id := range ids {
		must(app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: "#PYLQ", Value: map[string]any{"type": "war", "members": []any{map[string]any{"tag": "#PYL", "name": "Fixture", "townhall": 16, "remaining": 2}}, "reminder": map[string]any{"id": id, "server_id": guild, "channel_id": channel, "thread_id": thread, "minutes_remaining": 60}}}))
	}
	until := time.Now().Add(15 * time.Second)
	for time.Now().Before(until) && (sent.Load() != 1 || badReads.Load() == 0) {
		time.Sleep(50 * time.Millisecond)
	}
	check(sent.Load() == 1, "valid reminder did not use webhook")
	check(badReads.Load() > 0, "foreign webhook case not exercised")
	check(botMessages.Load() == 0, "reminder fell back to bot message")
	// Feed the actual Valkey -> delivery -> webhook path with raw CWL data.
	// A completed one-hit member must disappear even without event war_type.
	publishWar := func(league bool, allDone bool) {
		one := []any{map[string]any{"stars": 3}}
		zero := []any{}
		if allDone {
			zero = one
		}
		war := map[string]any{"state": "inWar", "clan": map[string]any{"tag": "#PYLQ", "name": "Fixture Clan", "members": []any{
			map[string]any{"tag": "#PYL", "name": "CompletedOne", "townhallLevel": 16, "attacks": one},
			map[string]any{"tag": "#PYQ", "name": "MissingZero", "townhallLevel": 16, "attacks": zero},
			map[string]any{"tag": "#PYY", "name": "CompletedTwo", "townhallLevel": 16, "attacks": []any{one[0], one[0]}},
		}}, "opponent": map[string]any{"tag": "#QYL", "members": []any{}}}
		if league {
			war["tag"] = "#PYQL"
		} else {
			// Regular/friendly payloads report their allowance. Only CWL omits
			// it; a fixture must not rely on the removed two-hit guess.
			war["attacksPerMember"] = 2
		}
		must(app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: "#PYLQ", Value: map[string]any{"type": "war", "data": war, "reminder": map[string]any{"id": ids[0], "server_id": guild, "channel_id": channel, "thread_id": thread, "minutes_remaining": 60}}}))
	}
	waitSent := func(want int32) {
		end := time.Now().Add(10 * time.Second)
		for sent.Load() < want && time.Now().Before(end) {
			time.Sleep(25 * time.Millisecond)
		}
		check(sent.Load() == want, "war reminder delivery count")
	}
	publishWar(true, false)
	waitSent(2)
	check(!strings.Contains(contents[1], "CompletedOne") && !strings.Contains(contents[1], "CompletedTwo") && strings.Contains(contents[1], "MissingZero") && strings.Contains(contents[1], "(1/1) TH16 MissingZero | #PYQ"), "CWL did not require exactly one hit")
	publishWar(false, false)
	waitSent(3)
	check(strings.Contains(contents[2], "CompletedOne") && strings.Contains(contents[2], "MissingZero") && !strings.Contains(contents[2], "CompletedTwo") && strings.Contains(contents[2], "(2/2) TH16 MissingZero | #PYQ"), "regular war allowance changed")
	publishWar(true, true)
	time.Sleep(400 * time.Millisecond)
	check(sent.Load() == 3, "completed CWL emitted reminder")
	publishOther := func(kind string, data map[string]any) {
		value := map[string]any{"type": kind, "clan": map[string]any{"name": "Fixture Clan"}, "members": []any{data}, "raid": map[string]any{"state": "ongoing", "startTime": time.Now().Add(-time.Hour).UTC().Format("20060102T150405.000Z"), "endTime": time.Now().Add(time.Hour).UTC().Format("20060102T150405.000Z")}, "reminder": map[string]any{"id": ids[0], "server_id": guild, "channel_id": channel, "thread_id": thread, "minutes_remaining": 60, "custom_text": "Follow the plan"}}
		must(app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: "#PYLQ", Value: value}))
	}
	publishOther("raid", map[string]any{"tag": "#PYQ", "name": "Raid Player", "townhall": 16, "attacks": 3, "total_attacks": 6})
	waitSent(4)
	check(strings.HasPrefix(contents[3], "**Fixture Clan Raid Weekend**\n⏰ Ends <t:") && strings.Contains(contents[3], "(3/6) TH16 Raid Player | #PYQ") && strings.HasSuffix(contents[3], "Follow the plan"), "raid command layout mismatch")
	publishOther("clan_games", map[string]any{"tag": "#PYQ", "name": "Games Player", "townhall": 16, "points": 1500})
	waitSent(5)
	check(strings.HasPrefix(contents[4], "**Fixture Clan Clan Games**\n⏰ Ends <t:") && strings.Contains(contents[4], "(1500/4,000) TH16 Games Player | #PYQ") && strings.HasSuffix(contents[4], "Follow the plan"), "clan games command layout mismatch")

	// Stale queued raids and future raids must be consumed without a webhook.
	for _, window := range [][2]time.Time{{time.Now().Add(-7 * 24 * time.Hour), time.Now().Add(-4 * 24 * time.Hour)}, {time.Now().Add(time.Hour), time.Now().Add(2 * time.Hour)}} {
		must(app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: "#PYLQ", Value: map[string]any{"type": "raid", "raid": map[string]any{"state": "ongoing", "startTime": window[0].UTC().Format("20060102T150405.000Z"), "endTime": window[1].UTC().Format("20060102T150405.000Z")}, "members": []any{map[string]any{"tag": "#PYQ", "name": "Must not ping", "townhall": 16, "attacks": 0, "total_attacks": 5}}, "reminder": map[string]any{"id": ids[0], "server_id": guild, "channel_id": channel, "thread_id": thread}}}))
	}
	time.Sleep(500 * time.Millisecond)
	check(sent.Load() == 5, "expired or not-yet-started raid emitted a reminder")
	// Exercise the real producer's API fallback as well as queued delivery.
	_, e = db.Exec(ctx, "UPDATE reminders SET type_name='Clan Capital',type=2 WHERE id=$1::uuid", ids[0])
	must(e)
	producerDone := make(chan error, 1)
	go func() { producerDone <- scripts.NewRemindersDomain().Run(runCtx, app) }()
	time.Sleep(300 * time.Millisecond)
	wake := func() {
		payload, _ := json.Marshal(map[string]any{"kind": "reminder_send", "serverId": guild, "reminderId": ids[0]})
		_, err := db.Exec(ctx, "SELECT pg_notify('clashking_tracking_wake_v1',$1)", string(payload))
		must(err)
	}
	wake()
	until = time.Now().Add(5 * time.Second)
	for raidReads.Load() == 0 && time.Now().Before(until) {
		time.Sleep(25 * time.Millisecond)
	}
	check(raidReads.Load() > 0, "raid fallback not exercised")
	time.Sleep(300 * time.Millisecond)
	check(sent.Load() == 5, "last weekend from API fallback emitted reminder")
	currentRaid.Store(true)
	wake()
	waitSent(6)
	check(strings.Contains(contents[5], "Fresh Raid Player"), "current raid did not reach webhook")
	stop()
	select {
	case <-producerDone:
	case <-time.After(10 * time.Second):
		panic("reminder producer did not stop")
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		panic("delivery did not stop")
	}
	fmt.Println("PASS actual reminder delivery: CWL one hit, regular two hits, completed members excluded, stale API raids and queued raids suppressed, current raid delivered, command layouts for war/raid/games, bot avatar/name, owned webhook, thread, foreign/missing rejected, no bot fallback")
}
