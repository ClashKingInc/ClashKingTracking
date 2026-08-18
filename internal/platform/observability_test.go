package platform

import (
	"errors"
	"testing"
	"time"
)

func TestTrackerSuccessfulReadinessClearsLastError(t *testing.T) {
	stats := NewTracker()
	stats.RecordRequest("wars", time.Millisecond, errors.New("proxy unavailable"))
	stats.SetReady("wars", true, "")

	got := stats.Domain("wars")
	if !got.Healthy || got.LastError != "" {
		t.Fatalf("readiness after recovery = healthy %t last error %q", got.Healthy, got.LastError)
	}
}

func TestTrackerRecordStore(t *testing.T) {
	stats := NewTracker()
	stats.RecordStore("globalclans", 100*time.Millisecond, 10, 8)
	stats.RecordStore("globalclans", 300*time.Millisecond, 20, 15)

	got := stats.Domain("globalclans")
	if got.StoreBatches != 2 {
		t.Fatalf("StoreBatches = %d, want 2", got.StoreBatches)
	}
	if got.StoreRowsRequested != 30 || got.StoreRowsAffected != 23 {
		t.Fatalf("store rows = requested %d affected %d, want 30/23", got.StoreRowsRequested, got.StoreRowsAffected)
	}
	if got.StoreDurationTotal != 400*time.Millisecond {
		t.Fatalf("StoreDurationTotal = %s, want 400ms", got.StoreDurationTotal)
	}
}

func TestTrackerTrackingProgress(t *testing.T) {
	stats := NewTracker()
	stats.SetTrackingTargets("battlelogs.targets", 2)
	stats.RecordTrackedTarget("battlelogs.targets")

	got := stats.Domain("battlelogs.targets")
	if got.TargetCount != 2 || got.TargetCycle != 1 || got.TargetProcessed != 1 {
		t.Fatalf("unexpected in-cycle progress: %#v", got)
	}

	stats.RecordTrackedTarget("battlelogs.targets")
	got = stats.Domain("battlelogs.targets")
	if got.TargetCycle != 2 || got.TargetProcessed != 0 {
		t.Fatalf("unexpected rollover progress: %#v", got)
	}
}

func TestTrackerSnapshotIncludesRuntimeAndDomains(t *testing.T) {
	stats := NewTracker()
	stats.RecordRequest("wars", 25*time.Millisecond, nil)
	stats.RecordWrite("wars", 3)
	stats.SetQueueDepth("wars", 7)

	snapshot := stats.Snapshot()
	if snapshot.ObservedAt.IsZero() || snapshot.Uptime <= 0 || snapshot.Goroutines <= 0 {
		t.Fatalf("missing runtime snapshot fields: %#v", snapshot)
	}
	if len(snapshot.Domains) != 1 {
		t.Fatalf("domains len = %d, want 1", len(snapshot.Domains))
	}
	got := snapshot.Domains[0]
	if got.Name != "wars" || got.Requests != 1 || got.Writes != 3 || got.QueueDepth != 7 {
		t.Fatalf("unexpected domain snapshot: %#v", got)
	}
	if got.RequestLatencyTotal != 25*time.Millisecond {
		t.Fatalf("RequestLatencyTotal = %s, want 25ms", got.RequestLatencyTotal)
	}
}
