// Exercise the real post worker with disposable SQL and a loopback FCM server.
// No production credentials, tokens, or provider requests are used.
package main

import (
	"bytes"
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
	"sync"
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
func check(ok bool, why string) {
	if !ok {
		panic(why)
	}
}
func hash(token string) string { return fmt.Sprintf("%x", sha256.Sum256([]byte(token))) }
func encrypt(value, key string) string {
	digest := sha256.Sum256([]byte(key))
	block, err := aes.NewCipher(digest[:])
	must(err)
	gcm, err := cipher.NewGCM(block)
	must(err)
	nonce := make([]byte, gcm.NonceSize())
	_, err = rand.Read(nonce)
	must(err)
	return "v1." + base64.RawURLEncoding.EncodeToString(append(nonce, gcm.Seal(nil, nonce, []byte(value), nil)...))
}
func main() {
	dsn := os.Getenv("TEST_DATABASE_URL")
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && strings.Contains(dsn, "@127.0.0.1:"), "disposable loopback SQL required")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	var count int
	must(db.QueryRow(ctx, "SELECT count(*) FROM mobile_push_devices").Scan(&count))
	check(count == 0, "empty device store required")
	const key = "local-token-key"
	cases := []string{"unregistered", "bare404", "wrongtype", "auth", "malformed", "trailing", "oversized", "unknown", "rotated", "success"}
	token := func(name string) string { return "fixture-sensitive-token-" + name }
	var mu sync.Mutex
	calls := map[string]int{}
	end := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	stamp := func(t time.Time) string { return t.Format("20060102T150405.000Z") }
	liveWar := map[string]any{"state": "inWar", "teamSize": 1, "attacksPerMember": 2, "preparationStartTime": stamp(end.Add(-48 * time.Hour)), "startTime": stamp(end.Add(-24 * time.Hour)), "endTime": stamp(end), "clan": map[string]any{"tag": "#PYL", "name": "Fixture Clan", "members": []any{map[string]any{"tag": "#PYLQ", "name": "Fixture", "townhallLevel": 16, "mapPosition": 1, "attacks": []any{}}}}, "opponent": map[string]any{"tag": "#QYL", "name": "Opponent", "members": []any{}}}
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/token" {
			fmt.Fprint(w, `{"access_token":"fixture","token_type":"Bearer","expires_in":3600}`)
			return
		}
		if r.Method == "GET" {
			switch {
			case strings.HasSuffix(r.URL.Path, "/currentwar"):
				json.NewEncoder(w).Encode(liveWar)
			case strings.HasSuffix(r.URL.Path, "/capitalraidseasons"):
				json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"state": "ongoing", "startTime": stamp(end.Add(-time.Hour)), "endTime": stamp(end), "members": []any{map[string]any{"tag": "#PYLQ", "name": "Fixture", "attacks": 0, "attackLimit": 5, "bonusAttackLimit": 1}}}}})
			default:
				json.NewEncoder(w).Encode(map[string]any{"tag": "#PYL", "name": "Fixture Clan", "memberList": []any{map[string]any{"tag": "#PYLQ", "name": "Fixture", "townHallLevel": 16}}})
			}
			return
		}
		check(r.URL.Path == "/v1/projects/fixture/messages:send", "unexpected request")
		var body struct {
			Message struct {
				Token string `json:"token"`
			} `json:"message"`
		}
		must(json.NewDecoder(r.Body).Decode(&body))
		name := strings.TrimPrefix(body.Message.Token, "fixture-sensitive-token-")
		mu.Lock()
		calls[name]++
		mu.Unlock()
		if name == "success" {
			fmt.Fprint(w, `{"name":"local-message"}`)
			return
		}
		if name == "rotated" {
			_, err := db.Exec(ctx, "UPDATE mobile_push_devices SET token_hash=$1,token_ciphertext=$2,last_seen_at=now() WHERE device_id='rotated'", hash(token("replacement")), encrypt(token("replacement"), key))
			must(err)
		}
		if name == "auth" {
			w.WriteHeader(403)
			fmt.Fprint(w, `{"error":{"details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"SENDER_ID_MISMATCH"}]}}`)
			return
		}
		w.WriteHeader(404)
		switch name {
		case "bare404":
			fmt.Fprint(w, `{"error":{"message":"fixture-sensitive-message"}}`)
		case "wrongtype":
			fmt.Fprint(w, `{"error":{"details":[{"@type":"other.Type","errorCode":"UNREGISTERED"}]}}`)
		case "malformed":
			fmt.Fprint(w, `{"error":`)
		case "trailing":
			fmt.Fprint(w, `{"error":{"details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}junk`)
		case "oversized":
			fmt.Fprintf(w, `{"error":{"message":"%s","details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}`, strings.Repeat("x", 17000))
		case "unknown":
			fmt.Fprint(w, `{"error":{"details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"fixture-sensitive-message"}]}}`)
		default:
			fmt.Fprint(w, `{"error":{"message":"fixture-sensitive-message","details":[{"@type":"type.googleapis.com/google.firebase.fcm.v1.FcmError","errorCode":"UNREGISTERED"}]}}`)
		}
	}))
	defer provider.Close()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	must(err)
	sa, err := json.Marshal(map[string]any{"type": "service_account", "project_id": "fixture", "private_key_id": "fixture", "private_key": string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(rsaKey)})), "client_email": "fixture@fixture.iam.gserviceaccount.com", "token_uri": provider.URL + "/token"})
	must(err)
	_, err = db.Exec(ctx, "INSERT INTO auth_users(user_id,provider) VALUES('local-push-user','discord')")
	must(err)
	_, err = db.Exec(ctx, "INSERT INTO mobile_notification_preferences(user_id,announcements_enabled) VALUES('local-push-user',true)")
	must(err)
	for _, name := range cases {
		_, err = db.Exec(ctx, `INSERT INTO mobile_push_devices(user_id,device_id,platform,provider,environment,token_ciphertext,token_hash,enabled,authorization_status) VALUES('local-push-user',$1,'android','fcm','production',$2,$3,true,'authorized')`, name, encrypt(token(name), key), hash(token(name)))
		must(err)
	}
	var post string
	must(db.QueryRow(ctx, "INSERT INTO admin_posts(slug,title,summary,status,platforms) VALUES('local-cleanup','Local','Local','live',ARRAY['android']) RETURNING id::text").Scan(&post))
	_, err = db.Exec(ctx, "INSERT INTO admin_post_delivery_attempts(post_id,attempt_number,trigger,status) VALUES($1,1,'manual','queued')", post)
	must(err)
	var logs bytes.Buffer
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, MobilePushScanSeconds: 1, RunOnce: true, MobilePushTokenKey: key, MobilePushFCMProjectID: "fixture", MobilePushFCMServiceAccountJSON: string(sa), MobilePushFCMAPIOrigin: provider.URL}, Logger: slog.New(slog.NewTextHandler(&logs, nil)), Stats: platform.NewTracker()}
	must(scripts.NewMobilePushDomain().Run(ctx, app))
	for _, name := range cases {
		var enabled bool
		must(db.QueryRow(ctx, "SELECT enabled FROM mobile_push_devices WHERE device_id=$1", name).Scan(&enabled))
		check(enabled == (name != "unregistered"), "unexpected enabled state: "+name)
	}
	var currentHash string
	must(db.QueryRow(ctx, "SELECT token_hash FROM mobile_push_devices WHERE device_id='rotated'").Scan(&currentHash))
	check(currentHash == hash(token("replacement")), "replacement token lost")
	check(strings.Contains(logs.String(), "UNREGISTERED") && strings.Contains(logs.String(), "SENDER_ID_MISMATCH"), "structured provider codes missing")
	check(!strings.Contains(logs.String(), "fixture-sensitive"), "provider body/token leaked into logs")
	must(scripts.NewMobilePushDomain().Run(ctx, app))
	mu.Lock()

	for _, name := range cases {
		check(calls[name] == 1, "unexpected retry: "+name)
	}
	mu.Unlock()
	check(os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable Valkey required")
	addr := os.Getenv("TEST_VALKEY_ADDR")
	check(strings.HasPrefix(addr, "127.0.0.1:"), "loopback Valkey required")
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(err)
	defer cache.Close()
	cc := clashy.DefaultClientConfig()
	cc.BaseURL = provider.URL + "/v1"
	cc.LookupCache = false
	cc.UpdateCache = false
	client, err := clashy.NewClient(cc)
	must(err)
	defer client.Close()
	app.Clash = client
	_, err = db.Exec(ctx, `INSERT INTO player_links(tag,user_id,source,is_verified) VALUES('#PYLQ','local-push-user','discord',true)`)
	must(err)
	_, err = db.Exec(ctx, `INSERT INTO mobile_notification_accounts(user_id,player_tag,enabled) VALUES('local-push-user','#PYLQ',true)`)
	must(err)
	_, err = db.Exec(ctx, `UPDATE mobile_notification_preferences SET legend_defenses_enabled=true,war_reminders_enabled=true,raid_reminders_enabled=true,reminder_timings=ARRAY[15],raid_reminder_timings=ARRAY[60] WHERE user_id='local-push-user'`)
	must(err)
	app.Valkey = cache
	app.Config.ValkeyAddr = addr
	app.Config.EventStreamName = "fixture:mobile-fanout"
	app.Config.EventStreamConsumer = "fixture"
	app.Config.EventStreamRetentionSeconds = 300
	app.Config.EventStreamReclaimIdleSeconds = 1
	_, err = db.Exec(ctx, `UPDATE mobile_notification_preferences SET war_state_enabled=true WHERE user_id='local-push-user'`)
	must(err)
	_, err = db.Exec(ctx, `INSERT INTO user_bookmarks(user_id,entity_type,tag,war_notifications_enabled,war_active_until) VALUES('local-push-user','clan','#PYL',true,now()+interval '1 day')`)
	must(err)
	_, err = db.Exec(ctx, `UPDATE user_bookmarks SET raid_notifications_enabled=true,raid_active_until=now()+interval '1 day' WHERE user_id='local-push-user'`)
	must(err)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- scripts.NewMobileEventsDomain().Run(runCtx, app) }()
	events := []struct{ kind, state, role string }{
		{"new_war", "preparation", ""}, {"war_state", "inWar", ""},
		{"war_state", "warEnded", ""}, {"war_state", "warEnded", "battle"},
	}
	for _, item := range events {
		must(app.PublishEvent(ctx, platform.Event{Topic: "war", ClanTag: "#PYL", Value: map[string]any{"type": item.kind, "war_role": item.role, "war": map[string]any{"state": item.state, "preparationStartTime": time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339), "clan": map[string]any{"tag": "#PYL", "name": "Fixture Clan"}, "opponent": map[string]any{"tag": "#QYL", "name": "Opponent"}}}}))
	}
	must(app.PublishEvent(ctx, platform.Event{Topic: "legend", Value: map[string]any{"type": "legend_defense", "player_tag": "#PYLQ", "player_name": "Fixture", "opponent_name": "Opponent", "stars": 2, "destruction_percentage": 80, "event_id": "local-legend-1"}}))
	must(app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: "#PYL", Value: map[string]any{"type": "war", "minutes_remaining": 15, "data": liveWar}}))
	must(app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: "#PYL", Value: map[string]any{"type": "raid_mobile", "user_id": "local-push-user", "minutes_remaining": 60, "raid_end": end.Format(time.RFC3339)}}))
	expected := 1 + len(events) + 3
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := calls["success"]
		mu.Unlock()
		if n >= expected {
			break
		}
		check(time.Now().Before(deadline), "war end never sent")
		time.Sleep(20 * time.Millisecond)
	}
	// Mixed success and permanent recipient failures must never replay the
	// successful recipient, even after multiple reclaim intervals.
	time.Sleep(12 * time.Second)
	stop()
	<-done
	mu.Lock()
	n := calls["success"]
	mu.Unlock()
	check(n == expected, fmt.Sprintf("war events replayed successful recipient: %d sends for %d events", n-1, expected-1))
	// Simulate an interrupted previous worker: claim a new alert without
	// processing it, then restart. That pending alert must never be replayed.
	event := platform.Event{Topic: "war", ClanTag: "#PYL", Value: map[string]any{"type": "war_state", "war": map[string]any{"state": "warEnded", "preparationStartTime": time.Now().Add(-48 * time.Hour).UTC().Format(time.RFC3339), "clan": map[string]any{"tag": "#PYL", "name": "Fixture Clan"}, "opponent": map[string]any{"tag": "#QYL", "name": "Opponent"}}}}
	must(app.PublishEvent(ctx, event))
	_, err = cache.Do(ctx, cache.B().Arbitrary("XREADGROUP", "GROUP", "mobilepush", "interrupted", "COUNT", "1", "NOACK", "STREAMS", app.Config.EventStreamName, ">").Build()).AsXRead()
	must(err)
	resumedCtx, resumedStop := context.WithCancel(ctx)
	resumedDone := make(chan error, 1)
	go func() { resumedDone <- scripts.NewMobileEventsDomain().Run(resumedCtx, app) }()
	time.Sleep(2 * time.Second)
	mu.Lock()
	beforeFresh := calls["success"]
	mu.Unlock()
	check(beforeFresh == n, "restart replayed interrupted event")
	must(app.PublishEvent(ctx, event))
	until := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		count := calls["success"]
		mu.Unlock()
		if count == n+1 {
			break
		}
		check(time.Now().Before(until), "new alert blocked by old pending event")
		time.Sleep(20 * time.Millisecond)
	}
	pending, err := cache.Do(ctx, cache.B().Arbitrary("XPENDING", app.Config.EventStreamName, "mobilepush").Build()).ToArray()
	must(err)
	pendingCount, err := pending[0].AsInt64()
	must(err)
	check(pendingCount == 0, "interrupted event left pending")
	resumedStop()
	<-resumedDone
	fmt.Println("PASS real mobile fanout: mixed recipient failures do not replay successful war preparation/start/end or CWL-end, Legend, war reminder and raid reminder notifications; restart does not replay interrupted alerts")
}
