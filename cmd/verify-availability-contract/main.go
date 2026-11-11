// Exercise the actual availability domain with disposable SQL/Valkey and a local upstream.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/json"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"time"
)

func main() {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" || os.Getenv("CLASHKING_DISPOSABLE_VALKEY") != "1" {
		panic("disposable stores required")
	}
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	if !strings.Contains(dsn, "@127.0.0.1:") || !strings.HasPrefix(addr, "127.0.0.1:") {
		panic("loopback only")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(err)
	defer cache.Close()
	status, reason := 503, "inMaintenance"
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]string{"reason": reason, "message": "This backend failure mentions maintenance but is not evidence of it"})
	}))
	defer provider.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = provider.URL
	cfg.LookupCache = false
	cfg.UpdateCache = false
	client, err := clashy.NewClient(cfg)
	must(err)
	defer client.Close()
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, RunOnce: true, DryRun: true}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), Stats: platform.NewTracker()}
	for _, c := range []struct {
		status   int
		reason   string
		official bool
	}{{503, "inMaintenance", true}, {503, "upstreamUnavailable", false}, {500, "unknownException", false}} {
		status, reason = c.status, c.reason
		_, err = db.Exec(ctx, "UPDATE tracking_availability SET available=true,official_started_at=NULL,observed_at=clock_timestamp()-interval '1 minute' WHERE singleton")
		must(err)
		must(scripts.NewAvailabilityDomain().Run(ctx, app))
		var official bool
		must(db.QueryRow(ctx, "SELECT official_started_at IS NOT NULL FROM tracking_availability WHERE singleton").Scan(&official))
		if official != c.official {
			panic(fmt.Sprintf("%d %s classified official=%v", status, reason, official))
		}
	}
	fmt.Println("PASS actual availability domain: confirmed maintenance, generic 503 and misleading 500 messages")
}
func must(err error) {
	if err != nil {
		panic(err)
	}
}
