// Exercise the actual delivery domain, SQL summaries and Discord REST adapter
// against disposable databases and a loopback provider. No live Discord writes.
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
	"sync"
	"sync/atomic"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, s string) {
	if !ok {
		panic(s)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "Disposable fixtures required")
	dsn := os.Getenv("TEST_DATABASE_URL")
	check(strings.Contains(dsn, "@127.0.0.1:"), "Loopback database required")
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{os.Getenv("TEST_VALKEY_ADDR")}})
	must(err)
	defer cache.Close()
	const guild = "999999999999994101"
	const hook = "999999999999994102"
	const channel = "999999999999994103"
	const application = "999999999999994104"
	var writes atomic.Int32
	var patchError atomic.Int32
	var lock sync.Mutex
	var payloads []string
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/clans/"):
			must(json.NewEncoder(w).Encode(map[string]any{"tag": "#PYL", "name": "Board Clan", "members": 1, "memberList": []any{map[string]any{"tag": "#PYLQ", "name": "Farmer", "townHallLevel": 16, "donations": 1234, "donationsReceived": 50}}}))
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[]}`)
		case r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/webhooks/"+hook):
			must(json.NewEncoder(w).Encode(map[string]any{"id": hook, "type": 1, "guild_id": guild, "channel_id": channel, "token": "local-only"}))
		case strings.Contains(r.URL.Path, "/webhooks/") && (r.Method == "POST" || r.Method == "PATCH"):
			if r.Method == "PATCH" && patchError.Load() != 0 {
				status := int(patchError.Load())
				code := 50013
				if status == 404 {
					code = 10008
				}
				if status == 410 {
					status = 404
					code = 10015
				}
				w.WriteHeader(status)
				must(json.NewEncoder(w).Encode(map[string]any{"code": code, "message": "Local fixture failure"}))
				return
			}
			var raw json.RawMessage
			must(json.NewDecoder(r.Body).Decode(&raw))
			lock.Lock()
			payloads = append(payloads, string(raw))
			lock.Unlock()
			number := writes.Add(1)
			must(json.NewEncoder(w).Encode(map[string]any{"id": fmt.Sprintf("999999999999995%03d", number), "channel_id": channel, "type": 0}))
		default:
			http.Error(w, "Unexpected local request", 500)
		}
	}))
	defer provider.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = provider.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	clash, err := clashy.NewClient(cfg)
	must(err)
	defer clash.Close()
	_, err = db.Exec(ctx, "INSERT INTO servers(id,name) VALUES($1,'Board fixture')", guild)
	must(err)
	var donation, farming string
	must(db.QueryRow(ctx, `INSERT INTO autoboards(server_id,board_type,target_scope,delivery_mode,webhook_id,interval_minutes,next_run_at,row_limit) VALUES($1,'donations','family','refresh',$2,1,now(),10) RETURNING id::text`, guild, hook).Scan(&donation))
	_, err = db.Exec(ctx, `INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received) VALUES('#PYL','Board Clan',true,0,1,'',0,0);INSERT INTO server_clans(server_id,tag) VALUES('`+guild+`','#PYL')`)
	must(err)
	tx, err := db.Begin(ctx)
	must(err)
	defer tx.Rollback(ctx)
	must(tx.QueryRow(ctx, `INSERT INTO autoboards(server_id,board_type,target_scope,delivery_mode,webhook_id,interval_seconds,next_run_at,row_limit) VALUES($1,'farming','custom','refresh',$2,30,now(),5) RETURNING id::text`, guild, hook).Scan(&farming))
	_, err = tx.Exec(ctx, `INSERT INTO autoboard_targets(autoboard_id,target,position) VALUES($1,'#PYL',0)`, farming)
	must(err)
	_, err = tx.Exec(ctx, `INSERT INTO autoboard_farming_settings(autoboard_id,window_mode,time_zone,counting_from) VALUES($1,'since_start','UTC',now()-interval '1 day')`, farming)
	must(err)
	must(tx.Commit(ctx))
	_, err = db.Exec(ctx, `INSERT INTO battles_farming(player_tag,battle_time,stars,destruction_percentage,duration_seconds,looted_resources) VALUES('#PYLQ',now()-interval '1 hour',3,100,90,'{"gold":1234,"elixir":500,"darkElixir":5}')`)
	must(err)
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: os.Getenv("TEST_VALKEY_ADDR"), DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(application)) + ".local.local", DiscordAPIURL: provider.URL, DiscordDeliveryBatchSize: 20, EventStreamName: "board-fixture-events", EventStreamReclaimIdleSeconds: 1, TrackedClanSnapshotPrefix: "board-fixture:"}, Clash: clash, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	wait := func(label string, predicate func() bool) {
		until := time.Now().Add(25 * time.Second)
		for time.Now().Before(until) {
			select {
			case err := <-done:
				panic(fmt.Sprintf("Domain stopped: %v", err))
			default:
			}
			if predicate() {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		panic("Timeout: " + label)
	}
	stored := func(id string) string {
		var value string
		must(db.QueryRow(ctx, "SELECT COALESCE(message_id,'') FROM autoboards WHERE id=$1", id).Scan(&value))
		return value
	}
	wait("initial boards", func() bool { return stored(donation) != "" && stored(farming) != "" })
	lock.Lock()
	initial := strings.Join(payloads, " ")
	lock.Unlock()
	check(strings.Contains(initial, "1.2K") && strings.Contains(initial, "Top Looters"), "Donation/farming payload missing recorded values")
	previous := stored(donation)
	before := writes.Load()
	patchError.Store(404)
	_, err = db.Exec(ctx, "UPDATE autoboards SET next_run_at=now() WHERE id=$1", donation)
	must(err)
	wait("deleted donation recreation", func() bool { return stored(donation) != previous })
	check(writes.Load() == before+1, "Deleted donation was not recreated once")
	previous = stored(donation)
	before = writes.Load()
	patchError.Store(403)
	_, err = db.Exec(ctx, "UPDATE autoboards SET next_run_at=now(),last_run_at=NULL WHERE id=$1", donation)
	must(err)
	wait("permission failure reschedule", func() bool {
		var yes bool
		must(db.QueryRow(ctx, "SELECT next_run_at>now()+interval '30 seconds' FROM autoboards WHERE id=$1", donation).Scan(&yes))
		return yes
	})
	check(writes.Load() == before && stored(donation) == previous, "Permission error created a duplicate")
	patchError.Store(410)
	_, err = db.Exec(ctx, "UPDATE autoboards SET next_run_at=now() WHERE id=$1", donation)
	must(err)
	wait("missing webhook reschedule", func() bool {
		var yes bool
		must(db.QueryRow(ctx, "SELECT next_run_at>now()+interval '30 seconds' FROM autoboards WHERE id=$1", donation).Scan(&yes))
		return yes
	})
	check(writes.Load() == before && stored(donation) == previous, "Missing webhook created a duplicate")
	patchError.Store(404)
	previous = stored(farming)
	_, err = db.Exec(ctx, "UPDATE autoboards SET next_run_at=now() WHERE id=$1", farming)
	must(err)
	wait("deleted farming recreation", func() bool { return stored(farming) != previous })
	cancel()
	select {
	case err = <-done:
		check(err == nil || strings.Contains(err.Error(), "context canceled"), fmt.Sprint(err))
	case <-time.After(7 * time.Second):
		panic("Domain did not stop")
	}
	fmt.Println("PASS actual donation/farming delivery, SQL summary, deleted message recovery, permission failure preserves ID, missing webhook is not message deletion; external Discord writes: 0")
}
