// Exercise real reminder producers and independent delivery consumers in disposable stores.
// Only the external Clash, Discord and FCM providers are loopback fixtures.
package main

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"
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
func check(ok bool, why string) {
	if !ok {
		panic(why)
	}
}
func until(ctx context.Context, why string, fn func() bool) {
	for !fn() {
		select {
		case <-ctx.Done():
			panic("timeout: " + why)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func encrypt(value, key string) string {
	digest := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(digest[:])
	must(err)
	var gcm cipher.AEAD
	gcm, err = cipher.NewGCM(block)
	must(err)
	nonce := make([]byte, gcm.NonceSize())
	_, err = rand.Read(nonce)
	must(err)
	return "v1." + base64.RawURLEncoding.EncodeToString(append(nonce, gcm.Seal(nil, nonce, []byte(value), nil)...))
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "owned fixtures required")
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback required")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(err)
	defer cache.Close()
	const guild = "999999999999991501"
	const appID = "999999999999991502"
	const channel = "999999999999991503"
	const hook = "999999999999991504"
	const user = "999999999999991505"
	const clan = "#PYLQRRJC"
	const enemy = "#PYLQRRJY"
	const player = "#PYLQRRJP"
	const key = "disposable-fixture-key"
	const currentKey = "disposable-current-push-key"
	const stream = "investigate:mobile:reminders"
	now := time.Now().UTC().Truncate(time.Second)
	prep := now.Add(-25 * time.Hour)
	end := now.Add(15 * time.Minute)
	stamp := func(t time.Time) string { return t.Format("20060102T150405.000Z") }
	war := map[string]any{"state": "inWar", "teamSize": 1, "attacksPerMember": 2, "preparationStartTime": stamp(prep), "startTime": stamp(now.Add(-time.Hour)), "endTime": stamp(end),
		"clan":     map[string]any{"tag": clan, "name": "Fixture Clan", "clanLevel": 10, "members": []any{map[string]any{"tag": player, "name": "Fixture Player", "townhallLevel": 16, "mapPosition": 1}}},
		"opponent": map[string]any{"tag": enemy, "name": "Fixture Opponent", "clanLevel": 10, "members": []any{}}}
	var discordSends, fcmSends, warReads atomic.Int64
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/token":
			fmt.Fprint(w, `{"access_token":"local-fixture","token_type":"Bearer","expires_in":3600}`)
		case r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/messages:send"):
			fcmSends.Add(1)
			fmt.Fprint(w, `{"name":"projects/fixture/messages/local"}`)
		case r.URL.Path == "/v1/clans/"+clan+"/currentwar":
			warReads.Add(1)
			json.NewEncoder(w).Encode(war)
		case r.URL.Path == "/v1/clans/"+enemy+"/currentwar":
			warReads.Add(1)
			json.NewEncoder(w).Encode(war)
		case r.URL.Path == "/v1/clans/"+clan:
			json.NewEncoder(w).Encode(map[string]any{"tag": clan, "name": "Fixture Clan", "type": "inviteOnly", "clanLevel": 10, "members": 1, "memberList": []any{map[string]any{"tag": player, "name": "Fixture Player", "townHallLevel": 16, "role": "member"}}})
		case r.URL.Path == "/webhooks/"+hook:
			json.NewEncoder(w).Encode(map[string]any{"id": hook, "type": 1, "guild_id": guild, "channel_id": channel, "application_id": appID, "token": "fixture"})
		case r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/webhooks/"+hook+"/fixture"):
			discordSends.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"id": "999999999999991506", "channel_id": channel, "type": 0})
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[]}`)
		default:
			http.Error(w, "unexpected local fixture request", 404)
		}
	}))
	defer provider.Close()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	must(err)
	sa, err := json.Marshal(map[string]any{"type": "service_account", "project_id": "fixture", "private_key_id": "fixture", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(rsaKey)})), "client_email": "fixture@fixture.iam.gserviceaccount.com", "client_id": "123", "token_uri": provider.URL + "/token"})
	must(err)
	cc := clashy.DefaultClientConfig()
	cc.BaseURL = provider.URL + "/v1"
	cc.LookupCache = false
	cc.UpdateCache = false
	client, err := clashy.NewClient(cc)
	must(err)
	defer client.Close()
	logFile, err := os.CreateTemp("", "ck-manual-mobile-routing-*.log")
	must(err)
	logPath := logFile.Name()
	defer os.Remove(logPath)
	defer logFile.Close()
	defer func() {
		if problem := recover(); problem != nil {
			b, _ := os.ReadFile(logPath)
			fmt.Fprint(os.Stderr, string(b))
			panic(problem)
		}
	}()
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, EventStreamName: stream, EventStreamConsumer: "fixture", EventStreamRetentionSeconds: 300, EventStreamReclaimIdleSeconds: 1, ReminderRequestsPerSecond: 10, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.fixture", DiscordAPIURL: provider.URL, DiscordDeliveryBatchSize: 20, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", MobilePushTokenKey: currentKey, MobilePushLegacyTokenKey: key, MobilePushFCMProjectID: "fixture", MobilePushFCMServiceAccountJSON: string(sa), MobilePushFCMAPIOrigin: provider.URL}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(logFile, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	_, err = db.Exec(ctx, `INSERT INTO auth_users(user_id,provider) VALUES($1,'discord');`, user)
	must(err)
	_, err = db.Exec(ctx, `INSERT INTO servers(id,name,last_command_at) VALUES($1,'Fixture',now())`, guild)
	must(err)
	_, err = db.Exec(ctx, `INSERT INTO player_links(tag,user_id,is_verified,source) VALUES($1,$2,true,'token')`, player, user)
	must(err)
	_, err = db.Exec(ctx, `INSERT INTO user_bookmarks(user_id,entity_type,tag,war_notifications_enabled,war_active_until) VALUES($1,'clan',$2,true,now()+interval '1 day')`, user, clan)
	must(err)
	_, err = db.Exec(ctx, `INSERT INTO mobile_notification_preferences(user_id,war_reminders_enabled,reminder_timings) VALUES($1,true,ARRAY[15])`, user)
	must(err)
	_, err = db.Exec(ctx, `INSERT INTO mobile_push_devices(user_id,device_id,platform,provider,environment,token_ciphertext,token_hash,authorization_status) VALUES($1,'fixture-device','ios','fcm','sandbox',$2,'fixture','authorized')`, user, encrypt("fixture-device-token", key))
	must(err)
	// Exercise both previous-key devices and current-API-key devices through
	// the actual notification stream consumer and local FCM HTTP provider.
	_, err = db.Exec(ctx, `INSERT INTO mobile_push_devices(user_id,device_id,platform,provider,environment,token_ciphertext,token_hash,authorization_status) VALUES($1,'rotated-device','ios','fcm','sandbox',$2,'fixture-v2','authorized')`, user, encrypt("rotated-device-token", currentKey))
	must(err)
	var reminder string
	must(db.QueryRow(ctx, `INSERT INTO reminders(server_id,type,type_name,clan_tag,webhook_token,channel_id,webhook_id,minutes_remaining,roles,townhalls) VALUES($1,1,'War',$2,'',$3,$4,15,ARRAY['member'],ARRAY[16]) RETURNING id::text`, guild, clan, channel, hook).Scan(&reminder))
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 3)
	for _, domain := range []platform.Domain{scripts.NewRemindersDomain(), scripts.NewDiscordDeliveryDomain()} {
		go func(d platform.Domain) { done <- d.Run(runCtx, app) }(domain)
	}
	until(ctx, "Postgres listener", func() bool {
		var n int
		err := db.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE query='LISTEN clashking_tracking_wake_v1'`).Scan(&n)
		return err == nil && n > 0
	})
	until(ctx, "both delivery groups", func() bool {
		v, e := cache.Do(ctx, cache.B().Arbitrary("XINFO", "GROUPS", stream).Build()).ToArray()
		return e == nil && len(v) >= 2
	})
	wake, _ := json.Marshal(map[string]string{"kind": "reminder_send", "serverId": guild, "reminderId": reminder})
	_, err = db.Exec(ctx, `SELECT pg_notify('clashking_tracking_wake_v1',$1)`, string(wake))
	must(err)
	until(ctx, "manual webhook delivery", func() bool { return discordSends.Load() == 1 })
	events, err := cache.Do(ctx, cache.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	must(err)
	var manual map[string]any
	manualID := ""
	for _, event := range events {
		var v map[string]any
		must(json.Unmarshal([]byte(event.FieldValues["value"]), &v))
		if v["manual"] == true {
			manual = v
			manualID = event.ID
		}
	}
	check(manualID != "", "real manual event absent")
	check(manual["minutes_remaining"] == nil, "manual unexpectedly has top-level timing")
	check(manual["reminder"].(map[string]any)["minutes_remaining"] == float64(15), "manual nested timing missing")
	// Seed the real manual event as pending, as if the previous consumer had failed.
	must(cache.Do(ctx, cache.B().Arbitrary("XGROUP", "CREATE", stream, "mobilepush", "0").Build()).Error())
	_, err = cache.Do(ctx, cache.B().Arbitrary("XREADGROUP", "GROUP", "mobilepush", "previous-worker", "COUNT", "1", "STREAMS", stream, ">").Build()).AsXRead()
	must(err)
	pending, err := cache.Do(ctx, cache.B().Arbitrary("XPENDING", stream, "mobilepush", manualID, manualID, "1").Build()).ToArray()
	must(err)
	check(len(pending) == 1, "manual pending fixture absent")
	go func() { done <- scripts.NewMobileEventsDomain().Run(runCtx, app) }()
	until(ctx, "old pending manual event acknowledged", func() bool {
		p, e := cache.Do(ctx, cache.B().Arbitrary("XPENDING", stream, "mobilepush", manualID, manualID, "1").Build()).ToArray()
		return e == nil && len(p) == 0
	})
	check(fcmSends.Load() == 0, "manual Discord event sent an unexpected mobile push")
	b, _ := os.ReadFile(logPath)
	check(!strings.Contains(string(b), "war reminder minutes_remaining must be a positive integer"), "manual event reached strict mobile decoder")
	schedule := models.ComputeWarKey(clan, enemy, prep)
	_, err = db.Exec(ctx, `INSERT INTO war_schedule(schedule_key,source_clan_tag,opponent_tag,prep_time,end_time,next_run_at,war_type) VALUES($1,$2,$3,$4,$5,now(),'random')`, schedule, clan, enemy, prep, end)
	must(err)
	_, err = db.Exec(ctx, `INSERT INTO war_reminder_jobs(schedule_key,minutes_remaining,run_at) VALUES($1,15,now()-interval '1 second')`, schedule)
	must(err)
	until(ctx, "scheduled delivery after discarding old manual event", func() bool { return fcmSends.Load() == 2 && discordSends.Load() == 2 })
	var jobs int
	must(db.QueryRow(ctx, `SELECT count(*) FROM war_reminder_jobs WHERE schedule_key=$1`, schedule).Scan(&jobs))
	check(jobs == 0, "due clock not consumed")
	time.Sleep(1200 * time.Millisecond)
	check(fcmSends.Load() == 2, "unexpected duplicate scheduled push")
	events, err = cache.Do(ctx, cache.B().Xrange().Key(stream).Start("-").End("+").Build()).AsXRange()
	must(err)
	scheduledCount := 0
	scheduledPending := 0
	for _, event := range events {
		var v map[string]any
		must(json.Unmarshal([]byte(event.FieldValues["value"]), &v))
		if v["schedule_key"] == schedule {
			scheduledCount++
			check(v["minutes_remaining"] == float64(15), "scheduled timing absent")
			check(v["manual"] == nil, "scheduled event marked manual")
			p, e := cache.Do(ctx, cache.B().Arbitrary("XPENDING", stream, "mobilepush", event.ID, event.ID, "1").Build()).ToArray()
			must(e)
			scheduledPending += len(p)
		}
	}
	check(scheduledCount == 2 && scheduledPending == 0, "shared clock's two side events not acknowledged")
	// A malformed shared event is still rejected; the manual exception must not
	// weaken the scheduled-event contract or send a notification with no timing.
	malformedID, err := cache.Do(ctx, cache.B().Xadd().Key(stream).Id("*").FieldValue().FieldValue("topic", "reminder").FieldValue("clan_tag", clan).FieldValue("timestamp", time.Now().UTC().Format(time.RFC3339Nano)).FieldValue("value", string(func() []byte { v, e := json.Marshal(map[string]any{"type": "war", "data": war}); must(e); return v }())).Build()).ToString()
	must(err)
	until(ctx, "malformed scheduled event remains rejected", func() bool {
		p, e := cache.Do(ctx, cache.B().Arbitrary("XPENDING", stream, "mobilepush", malformedID, malformedID, "1").Build()).ToArray()
		b, _ := os.ReadFile(logPath)
		return e == nil && len(p) == 1 && strings.Contains(string(b), "war reminder minutes_remaining must be a positive integer")
	})
	check(fcmSends.Load() == 2, "malformed scheduled event produced a push")
	stop()
	for range 3 {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			panic("domain did not stop")
		}
	}
	result := map[string]any{"environment": "disposable local Timescale + Valkey + real tracking domains; Clash/Discord/FCM are loopback providers", "manual": map[string]any{"produced_by": "PostgreSQL NOTIFY -> real reminders.processManualReminder", "nested_minutes": 15, "top_level_minutes_present": false, "discord_webhooks_delivered": 1, "mobile_pushes": 0, "old_pending_acknowledged": true, "mobile_decoder_errors": 0}, "scheduled": map[string]any{"produced_by": "real due war job -> real reminders.finishDueWarJob", "side_events": scheduledCount, "top_level_minutes": 15, "discord_webhooks_delivered": 1, "mobile_pushes_delivered": fcmSends.Load(), "remaining_mobile_pending": scheduledPending, "due_job_removed": true}, "malformed_scheduled_event_rejected": true, "war_provider_reads": warReads.Load(), "production_writes": 0}
	output, err := json.MarshalIndent(result, "", "  ")
	must(err)
	fmt.Println(string(output))
	if path := os.Getenv("CLASHKING_E2E_RESULT"); path != "" {
		must(os.WriteFile(path, append(output, '\n'), 0644))
	}
}
