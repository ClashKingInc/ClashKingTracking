//go:build platform_internal_tests

package platform

import (
	"testing"
	"time"
)

func TestEventStreamMinIDUsesRetentionWindow(t *testing.T) {
	now := time.UnixMilli(1_700_000_300_123).UTC()

	if got := eventStreamMinID(now, 300); got != "1700000000123-0" {
		t.Fatalf("eventStreamMinID() = %q, want %q", got, "1700000000123-0")
	}
}
