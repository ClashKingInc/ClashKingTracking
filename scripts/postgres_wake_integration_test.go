//go:build integration

package scripts

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestTrackingWakeListenerReceivesValidNotifyAndIgnoresMalformedPayload(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" {
		t.Skip("requires disposable Goose fixture")
	}
	dsn := os.Getenv("TEST_DATABASE_URL")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	received := make(chan trackingWakeEvent, 1)
	done := make(chan error, 1)
	go func() {
		done <- listenForTrackingWakes(ctx, dsn, func(event trackingWakeEvent) {
			select {
			case received <- event:
			default:
			}
		})
	}()

	sender, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close(t.Context())
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := sender.Exec(t.Context(), `SELECT pg_notify($1, $2), pg_notify($1, $3)`,
			trackingWakeChannel, `{"v":1,"kind":"guild_reactivated","serverId":"bad"}`,
			`{"v":1,"kind":"guild_reactivated","serverId":"123"}`); err != nil {
			t.Fatal(err)
		}
		select {
		case event := <-received:
			if event.Kind != "guild_reactivated" || event.ServerID != "123" {
				t.Fatalf("received wake = %#v", event)
			}
			cancel()
			select {
			case err := <-done:
				if err == nil || !errors.Is(err, context.Canceled) {
					t.Fatalf("listener shutdown error = %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("listener did not stop after cancellation")
			}
			return
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("listener did not receive the valid PostgreSQL notification")
		}
	}
}
