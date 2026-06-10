//go:build script_internal_tests

package scripts

import (
	"reflect"
	"testing"

	"clashking_tracking/models"
)

func TestPlayerChangesBuildsSQLRows(t *testing.T) {
	previous := map[string]any{
		"name":              "Old",
		"donations":         float64(10),
		"donationsReceived": float64(4),
		"townHallLevel":     float64(15),
		"clan":              map[string]any{"tag": "#CLAN"},
	}
	current := map[string]any{
		"name":              "New",
		"donations":         float64(15),
		"donationsReceived": float64(8),
		"townHallLevel":     float64(16),
		"clan":              map[string]any{"tag": "#CLAN"},
	}

	changes, activity, stats := playerChanges("#PLAYER", previous, current)

	if activity == 0 {
		t.Fatalf("activity score was not detected")
	}
	if len(changes) != 2 {
		t.Fatalf("profile change count = %d, want 2", len(changes))
	}
	if got := playerChangeTypes(changes); !reflect.DeepEqual(got, []string{"name", "townHallLevel"}) {
		t.Fatalf("change types = %v", got)
	}
	if len(stats) != 1 {
		t.Fatalf("season stat rows = %d, want 1", len(stats))
	}
	if stats[0].Donated != 5 || stats[0].Received != 4 || stats[0].ActivityScore != 1 {
		t.Fatalf("season stats = %+v", stats[0])
	}
}

func TestMemoryBotPlayerStoreCursorWraps(t *testing.T) {
	store := newMemoryBotPlayerStore()
	store.targets = []models.BotPlayerTarget{{Tag: "#A"}, {Tag: "#B"}, {Tag: "#C"}}

	first, err := store.NextTargetBatch(t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []models.BotPlayerTarget{{Tag: "#A"}, {Tag: "#B"}}; !reflect.DeepEqual(first.Targets, want) {
		t.Fatalf("first targets = %#v, want %#v", first.Targets, want)
	}
	if first.Cursor != "#B" {
		t.Fatalf("first cursor = %q, want #B", first.Cursor)
	}
	if err := store.CommitTargetBatch(t.Context(), first); err != nil {
		t.Fatal(err)
	}

	second, err := store.NextTargetBatch(t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []models.BotPlayerTarget{{Tag: "#C"}}; !reflect.DeepEqual(second.Targets, want) {
		t.Fatalf("second targets = %#v, want %#v", second.Targets, want)
	}
	if second.Cursor != "" {
		t.Fatalf("second cursor = %q, want wrap cursor", second.Cursor)
	}
}
