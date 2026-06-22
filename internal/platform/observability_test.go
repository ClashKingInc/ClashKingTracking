package platform

import (
	"testing"
	"time"
)

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
	if got.LastStoreRowsRequested != 20 || got.LastStoreRowsAffected != 15 {
		t.Fatalf("last store rows = requested %d affected %d, want 20/15", got.LastStoreRowsRequested, got.LastStoreRowsAffected)
	}
	if got.LastStoreMs != 300 {
		t.Fatalf("LastStoreMs = %f, want 300", got.LastStoreMs)
	}
	if got.AvgStoreMs != 200 {
		t.Fatalf("AvgStoreMs = %f, want 200", got.AvgStoreMs)
	}
}
