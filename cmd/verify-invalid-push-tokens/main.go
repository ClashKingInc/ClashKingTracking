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
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"github.com/jackc/pgx/v5/pgxpool"
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
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/token" {
			fmt.Fprint(w, `{"access_token":"fixture","token_type":"Bearer","expires_in":3600}`)
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
	// Use the production-style pooled DSN in the real worker, including cleanup.
	pooledURL, err := url.Parse(dsn)
	must(err)
	query := pooledURL.Query()
	query.Set("pool_max_conns", "4")
	query.Set("pool_min_conns", "0")
	pooledURL.RawQuery = query.Encode()
	var logs bytes.Buffer
	app := &platform.App{Config: platform.Config{TimescaleURL: pooledURL.String(), MobilePushScanSeconds: 1, RunOnce: true, MobilePushTokenKey: key, MobilePushFCMProjectID: "fixture", MobilePushFCMServiceAccountJSON: string(sa), MobilePushFCMAPIOrigin: provider.URL}, Logger: slog.New(slog.NewTextHandler(&logs, nil)), Stats: platform.NewTracker()}
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
	defer mu.Unlock()
	for _, name := range cases {
		check(calls[name] == 1, "unexpected retry: "+name)
	}
	fmt.Println("PASS real post worker: only typed UNREGISTERED disabled; bare404/wrong-type/auth/malformed/unknown preserved; token replacement race protected; safe codes logged; no automatic retry")
}
