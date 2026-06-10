//go:build script_internal_tests

package scripts

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestValidateScheduledConfig(t *testing.T) {
	cfg := platform.Config{MockDB: true}
	cfg.ScheduledIntervalSeconds = 0
	if err := validateScheduledConfig(cfg); err == nil {
		t.Fatal("expected invalid interval error")
	}
	cfg.ScheduledIntervalSeconds = 60
	if err := validateScheduledConfig(cfg); err != nil {
		t.Fatalf("valid mock config rejected: %v", err)
	}
}

func TestJSONAnyNormalizesStructPayloads(t *testing.T) {
	type payload struct {
		Name  string `json:"name"`
		Score int    `json:"score"`
	}
	got := jsonAny(payload{Name: "global", Score: 10})
	want := map[string]any{"name": "global", "score": float64(10)}
	if !reflect.DeepEqual(got, want) {
		raw, _ := json.Marshal(got)
		t.Fatalf("jsonAny = %s, want %#v", raw, want)
	}
}

func TestLeaderboardLocationIDsAddsGlobal(t *testing.T) {
	got := leaderboardLocationIDs([]clashy.Location{
		{ID: 32000006, Name: "International"},
		{ID: 32000007, Name: "Afghanistan"},
	})
	want := []string{"32000006", "32000007", "global"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("leaderboardLocationIDs = %#v, want %#v", got, want)
	}
}

func TestLeaderboardPayloadHasItems(t *testing.T) {
	if leaderboardPayloadHasItems([]string{}) {
		t.Fatal("empty slices should be skipped")
	}
	if !leaderboardPayloadHasItems([]string{"one"}) {
		t.Fatal("non-empty slices should be stored")
	}
	if !leaderboardPayloadHasItems(struct{ Name string }{Name: "not a leaderboard slice"}) {
		t.Fatal("non-collection payloads should be stored")
	}
}

func TestShouldStoreLeaderboardKindSkipsCapitalExceptTuesday(t *testing.T) {
	monday := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	tuesday := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	if shouldStoreLeaderboardKind(leaderboardKindCapital, monday) {
		t.Fatal("capital rankings should not be stored outside Tuesday")
	}
	if !shouldStoreLeaderboardKind(leaderboardKindCapital, tuesday) {
		t.Fatal("capital rankings should be stored on Tuesday")
	}
	if !shouldStoreLeaderboardKind("player_trophies", monday) {
		t.Fatal("non-capital rankings should be stored every run")
	}
}

func TestLeaderboardSnapshotItemsExtractsQueryableRows(t *testing.T) {
	date := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	got := leaderboardSnapshotItems("clan_trophies", "global", date, []clashy.RankedClan{{
		Clan: clashy.Clan{Tag: "#2ABC", Name: "One", Points: 70000},
		Rank: 1,
	}})
	if len(got) != 1 {
		t.Fatalf("leaderboardSnapshotItems returned %d rows, want 1", len(got))
	}
	row := got[0]
	if row.Kind != "clan_trophies" || row.LocationID != "global" || !row.Date.Equal(date) || row.Tag != "#2ABC" {
		t.Fatalf("unexpected row identity: %#v", row)
	}
	if row.Name != "One" || row.Rank != 1 {
		t.Fatalf("unexpected row fields: %#v", row)
	}

	players := leaderboardSnapshotItems("player_versus_trophies", "32000006", date, []clashy.RankedPlayer{{
		Player:       clashy.Player{Tag: "#2PLY", Name: "Player", BuilderBaseTrophies: 6000},
		Rank:         3,
		PreviousRank: 4,
	}})
	if len(players) != 1 || players[0].Kind != "player_versus_trophies" || players[0].Rank != 3 {
		t.Fatalf("unexpected player row: %#v", players)
	}
}
