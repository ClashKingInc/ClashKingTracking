// Run the real delivery domain against the local API and a loopback Discord
// provider. The parent integration owns and inspects the synthetic giveaway.
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
)

func main() {
	if os.Getenv("CLASHKING_GIVEAWAY_E2E") != "1" {
		panic("Local giveaway integration flag required")
	}
	dsn, api, provider := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_API_URL"), os.Getenv("TEST_DISCORD_URL")
	for _, raw := range []string{dsn, api, provider} {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() != "127.0.0.1" {
			panic("Loopback providers required")
		}
	}
	guild, id := os.Getenv("TEST_GUILD_ID"), os.Getenv("TEST_GIVEAWAY_ID")
	if guild != "999999999999996510" || !strings.HasPrefix(id, "local-reroll-") {
		panic("Synthetic giveaway scope required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	cache, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{os.Getenv("TEST_VALKEY_ADDR")}})
	if err != nil {
		panic(err)
	}
	defer cache.Close()
	stream := fmt.Sprintf("giveaway-delivery-e2e:%d", os.Getpid())
	defer cache.Do(context.Background(), cache.B().Del().Key(stream).Build())
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, DiscordBotToken: base64.RawStdEncoding.EncodeToString([]byte("999999999999996501")) + ".local.local", DiscordAPIURL: provider, ClashKingAPIURL: api, ClashKingAPIToken: os.Getenv("TEST_API_TOKEN"), DiscordGuildAllowlist: map[string]struct{}{guild: {}}, DiscordDeliveryBatchSize: 10, EventStreamName: stream, EventStreamReclaimIdleSeconds: 1}, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	done := make(chan error, 1)
	go func() { done <- scripts.NewDiscordDeliveryDomain().Run(ctx, app) }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-done:
			panic(fmt.Sprintf("Delivery stopped before completion: %v", err))
		case <-ctx.Done():
			panic("Delivery timed out")
		case <-ticker.C:
			var count int
			err = db.QueryRow(ctx, "SELECT count(*) FROM giveaway_publication_effects WHERE giveaway_id=$1 AND server_id=$2 AND kind='reroll' AND state='succeeded'", id, guild).Scan(&count)
			if err != nil {
				panic(err)
			}
			if count == 1 {
				cancel()
				<-done
				fmt.Println("PASS actual giveaway delivery domain completed local reroll")
				return
			}
		}
	}
}
