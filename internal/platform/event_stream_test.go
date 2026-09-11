//go:build platform_internal_tests

package platform

import (
	"context"
	"os"
	"testing"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

func TestEventStreamMinIDUsesRetentionWindow(t *testing.T) {
	now := time.UnixMilli(1_700_000_300_123).UTC()

	if got := eventStreamMinID(now, 300); got != "1700000000123-0" {
		t.Fatalf("eventStreamMinID() = %q, want %q", got, "1700000000123-0")
	}
}

func TestAppendEventOnceAtomicallyDeduplicatesStreamEntry(t *testing.T) {
	addr := os.Getenv("TEST_VALKEY_ADDR")
	if addr == "" {
		t.Skip("TEST_VALKEY_ADDR is required")
	}
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}, SelectDB: 15})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	cfg := Config{EventStreamName: "tracking:test:event-once:" + t.Name(), EventStreamRetentionSeconds: 300}
	dedupeKey := cfg.EventStreamName + ":dedupe:one"
	defer client.Do(context.Background(), client.B().Arbitrary("DEL").Keys(cfg.EventStreamName, dedupeKey).Build())
	event := Event{Topic: "legend", Timestamp: time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC), Value: map[string]any{"type": "legend_defense"}}

	inserted, err := AppendEventOnce(t.Context(), client, cfg, "one", time.Minute, event)
	if err != nil || !inserted {
		t.Fatalf("first append = %t, %v", inserted, err)
	}
	inserted, err = AppendEventOnce(t.Context(), client, cfg, "one", time.Minute, event)
	if err != nil || inserted {
		t.Fatalf("duplicate append = %t, %v", inserted, err)
	}
	length, err := client.Do(t.Context(), client.B().Xlen().Key(cfg.EventStreamName).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	if length != 1 {
		t.Fatalf("stream length = %d, want 1", length)
	}
}
