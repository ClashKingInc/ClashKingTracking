// Exercise the actual delivery process, SQL selection and Valkey events with
// loopback HTTP dependencies. No production credentials or destinations are used.
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

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, msg string) {
	if !ok {
		panic(msg)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable stores required")
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback stores required")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(err)
	defer cache.Close()
	const guild = "999999999999992801"
	const appID = "999999999999992802"
	const old = "cutover-old"
	const current = "cutover-current"
	boundary := time.Now().UTC().Add(-time.Hour)
	_, err = db.Exec(ctx, `INSERT INTO servers(id,name) VALUES($1,'Cutover fixture')`, guild)
	must(err)
	for _, id := range []string{old, current} {
		ended := boundary.Add(time.Minute)
		entries := "[]"
		if id == old {
			ended = boundary.Add(-24 * time.Hour)
			entries = "null"
		}
		_, err = db.Exec(ctx, `INSERT INTO giveaways(id,server_id,prize,channel_id,status,start_time,end_time,winners,message_id,entries) VALUES($1,$2,'Fixture','999999999999992803','ended',$3,$4,1,'999999999999992804',$5::jsonb)`, id, guild, ended.Add(-time.Hour), ended, entries)
		must(err)
	}
	var oldCalls, currentCalls, discordWrites atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(r.URL.Path, "/giveaways/") && strings.HasSuffix(r.URL.Path, "/publications/prepare"):
			if strings.Contains(r.URL.Path, old) {
				oldCalls.Add(1)
			} else if strings.Contains(r.URL.Path, current) {
				currentCalls.Add(1)
			}
			fmt.Fprint(w, `{"outcome":"pending"}`)
		case strings.HasSuffix(r.URL.Path, "/emojis"):
			fmt.Fprint(w, `{"items":[]}`)
		case r.Method == "POST":
			discordWrites.Add(1)
			http.Error(w, "unexpected write", 500)
		default:
			fmt.Fprint(w, `{}`)
		}
	}))
	defer provider.Close()
	cc := clashy.DefaultClientConfig()
	cc.BaseURL = provider.URL + "/v1"
	cc.LookupCache = false
	cc.UpdateCache = false
	clash, err := clashy.NewClient(cc)
	must(err)
	defer clash.Close()
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ValkeyAddr: addr, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte(appID)) + ".local.local", DiscordAPIURL: provider.URL, ClashKingAPIURL: provider.URL, ClashKingAPIToken: "fixture", GiveawayRecoverySince: boundary.Format(time.RFC3339Nano), DiscordGuildAllowlist: map[string]struct{}{guild: {}}, DiscordDeliveryBatchSize: 20, EventStreamName: "giveaway-cutover-fixture", EventStreamReclaimIdleSeconds: 1}, Clash: clash, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(runCtx, app) }()
	for currentCalls.Load() == 0 && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
	}
	check(currentCalls.Load() > 0, "new ending did not reach API")
	must(app.PublishEvent(ctx, platform.Event{Topic: "giveaway", Value: map[string]any{"type": "giveaway_end", "giveaway": map[string]any{"id": old, "server_id": guild}}}))
	time.Sleep(3 * time.Second)
	check(oldCalls.Load() == 0, "historical ending reached API")
	check(discordWrites.Load() == 0, "unexpected Discord message")
	var preserved bool
	must(db.QueryRow(ctx, `SELECT entries='null'::jsonb AND status='ended' FROM giveaways WHERE id=$1`, old).Scan(&preserved))
	check(preserved, "historical data changed")
	stop()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			panic(err)
		}
	case <-time.After(15 * time.Second):
		panic("delivery did not stop")
	}
	b, _ := json.Marshal(map[string]any{"historical_calls": oldCalls.Load(), "new_ending_calls": currentCalls.Load(), "historical_data_preserved": preserved})
	fmt.Println("PASS actual delivery cutover", string(b))
}
