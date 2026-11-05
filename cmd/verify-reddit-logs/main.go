// Exercise the actual Discord delivery service with local SQL, Valkey and HTTP.
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
	"sync/atomic"
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
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable services required")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback required")
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	const guild = "999999999999992801"
	const appID = "999999999999992802"
	const channel = "999999999999992803"
	const thread = "999999999999992804"
	const hook = "999999999999992805"
	var sent atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[]}`)
		case strings.HasSuffix(r.URL.Path, "/webhooks/"+hook):
			json.NewEncoder(w).Encode(map[string]any{"id": hook, "type": 1, "guild_id": guild, "channel_id": channel, "application_id": appID, "token": "fixture"})
		case strings.Contains(r.URL.Path, "/webhooks/"+hook+"/fixture") && r.Method == "POST":
			check(r.URL.Query().Get("thread_id") == thread, "thread lost")
			var body map[string]any
			must(json.NewDecoder(r.Body).Decode(&body))
			content, _ := body["content"].(string)
			check(strings.Contains(content, "<@&999999999999992899>"), "configured role missing")
			mentions, _ := body["allowed_mentions"].(map[string]any)
			roles, _ := mentions["roles"].([]any)
			check(len(roles) == 1 && roles[0] == "999999999999992899", "explicit allowed role missing")
			parse, _ := mentions["parse"].([]any)
			check(len(parse) == 0, "automatic mentions enabled")
			check(!strings.Contains(fmt.Sprint(body), "DO_NOT_DELIVER"), "unsubscribed comment delivered")
			sent.Add(1)
			json.NewEncoder(w).Encode(map[string]any{"id": "999999999999992806", "channel_id": thread, "type": 0})
		default:
			http.Error(w, "unexpected fixture request", 404)
		}
	}))
	defer provider.Close()
	_, e = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'Reddit fixture')", guild)
	must(e)
	_, e = db.Exec(ctx, "INSERT INTO server_logs(server_id,clan_tag,type,webhook_id,thread_id,reddit_role_id,reddit_accounts) VALUES($1,NULL,'reddit_feed',$2,$3,'999999999999992899',ARRAY['Recruiter'])", guild, hook, thread)
	must(e)
	stream := fmt.Sprintf("reddit-log-fixture:%d", os.Getpid())
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", DiscordDeliveryBatchSize: 10, EventStreamName: stream, EventStreamReclaimIdleSeconds: 1}, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	for _, event := range []map[string]any{
		{"type": "redditcomment", "data": map[string]any{"submission_author": "Other", "author": "Commenter", "body": "DO_NOT_DELIVER", "url": "/r/ClashOfClansRecruit/comments/abc/comment1"}},
		{"type": "reddit", "data": map[string]any{"title": "Searching", "selftext": "Looking for a clan", "url": "https://www.reddit.com/r/ClashOfClansRecruit/comments/abc", "comments_link": "https://www.reddit.com/r/ClashOfClansRecruit/comments/abc", "score": 1}},
		{"type": "redditcomment", "data": map[string]any{"submission_author": "recruiter", "author": "Commenter", "body": "An actual reply", "url": "/r/ClashOfClansRecruit/comments/abc/comment2", "score": 2}},
	} {
		must(app.PublishEvent(ctx, platform.Event{Topic: "reddit", Timestamp: time.Now(), Value: event}))
	}
	until := time.Now().Add(15 * time.Second)
	for sent.Load() < 2 && time.Now().Before(until) {
		time.Sleep(30 * time.Millisecond)
	}
	check(sent.Load() == 2, "post and subscribed comment did not reach the thread")
	stop()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		panic("delivery did not stop")
	}
	fmt.Println("PASS actual Reddit delivery: posts, case-insensitive submission-author filter, unmatched comment excluded, explicit role allowlist, forum thread; no production writes")
}
