// Run the actual notification stream consumer with canonical disposable stores
// and a loopback Clash API. No real devices, guilds or provider writes.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/json"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
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
func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1", "disposable stores required")
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "loopback stores required")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(err)
	defer cache.Close()
	var code, reads atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		check(strings.HasSuffix(r.URL.Path, "/currentwar"), "unexpected provider request")
		reads.Add(1)
		status := int(code.Load())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if status == 200 {
			json.NewEncoder(w).Encode(map[string]any{"state": "notInWar"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"reason": "accessDenied"})
	}))
	defer provider.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = provider.URL + "/v1"
	cfg.LookupCache = false
	cfg.UpdateCache = false
	client, err := clashy.NewClient(cfg)
	must(err)
	defer client.Close()
	for _, status := range []int{400, 403, 404, 401, 429, 503} {
		code.Store(int32(status))
		reads.Store(0)
		stream := fmt.Sprintf("notification-source-e2e:%d:%d", os.Getpid(), status)
		app := &platform.App{Config: platform.Config{TimescaleURL: dsn, EventStreamName: stream, EventStreamReclaimIdleSeconds: 1, EventStreamRetentionSeconds: 60, MobilePushFCMProjectID: "local-only", MobilePushTokenKey: "local-only"}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker()}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- scripts.NewMobileEventsDomain().Run(runCtx, app) }()
		must(app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: "#PYLQ", Value: map[string]any{"type": "war", "minutes_remaining": 15, "data": map[string]any{"state": "inWar", "endTime": time.Now().Add(time.Hour).UTC().Format("20060102T150405.000Z")}}}))
		pending := func() int64 {
			values, e := cache.Do(ctx, cache.B().Arbitrary("XPENDING", stream, "mobilepush").Build()).ToArray()
			if e != nil || len(values) == 0 {
				return -1
			}
			n, _ := values[0].AsInt64()
			return n
		}
		wait := func(predicate func() bool) {
			until := time.Now().Add(8 * time.Second)
			for !predicate() {
				check(time.Now().Before(until), fmt.Sprintf("status %d timed out", status))
				time.Sleep(20 * time.Millisecond)
			}
		}
		wait(func() bool { return reads.Load() > 0 })
		expectedPending := int64(0)
		wait(func() bool { return pending() == expectedPending })
		must(app.PublishEvent(ctx, platform.Event{Topic: "ignored-health-marker", Value: map[string]any{}}))
		time.Sleep(1200 * time.Millisecond)
		check(reads.Load() == 1, "source failure automatically retried")
		stats := app.Stats.Snapshot().Domains
		check(len(stats) == 1 && stats[0].Errors == 1 && stats[0].Requests == 1 && !stats[0].Healthy, "ignored traffic erased failed health")
		code.Store(200)
		// A new event can recover; the previously attempted event stays untouched.
		must(app.PublishEvent(ctx, platform.Event{Topic: "reminder", ClanTag: "#PYLQ", Value: map[string]any{"type": "war", "minutes_remaining": 15, "data": map[string]any{"state": "inWar", "endTime": time.Now().Add(time.Hour).UTC().Format("20060102T150405.000Z")}}}))
		wait(func() bool { return reads.Load() == 2 && app.Stats.Snapshot().Domains[0].Healthy })
		check(pending() == expectedPending, "old pending source event replayed")

		stop()
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			panic("notification consumer did not stop")
		}
		must(cache.Do(ctx, cache.B().Del().Key(stream).Build()).Error())
	}
	fmt.Println("PASS actual notification consumer: permanent 400/403/404 acknowledged once; 401/429/503 acknowledged without replay; new events recover; ignored traffic preserves failed health; attempt/error metrics recorded")
}
