//go:build script_internal_tests

package scripts

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestParseArmyColumnsAndHashIgnoresOrder(t *testing.T) {
	left := parseArmyColumns("u10x0-2x1s4x35i3x53d1x70h0p4e8_14-1p9e39")
	right := parseArmyColumns("h1p9e39-0p4e8_14d1x70i3x53s4x35u2x1-10x0")

	want := map[string]uint16{
		"u_0":  10,
		"u_1":  2,
		"s_35": 4,
		"i_53": 3,
		"d_70": 1,
		"h_0":  1,
		"p_4":  1,
		"e_8":  1,
		"e_14": 1,
		"h_1":  1,
		"p_9":  1,
		"e_39": 1,
	}
	for key, value := range want {
		if left[key] != value {
			t.Fatalf("left[%s] = %d, want %d", key, left[key], value)
		}
		if right[key] != value {
			t.Fatalf("right[%s] = %d, want %d", key, right[key], value)
		}
	}
	if armyHash(left) != armyHash(right) {
		t.Fatalf("hash should ignore army link ordering")
	}
}

func TestParseArmyColumnsAggregatesDuplicates(t *testing.T) {
	got := parseArmyColumns("u1x0-2x0-3x1s1x35-2x35")
	if got["u_0"] != 3 {
		t.Fatalf("u_0 = %d, want 3", got["u_0"])
	}
	if got["u_1"] != 3 {
		t.Fatalf("u_1 = %d, want 3", got["u_1"])
	}
	if got["s_35"] != 3 {
		t.Fatalf("s_35 = %d, want 3", got["s_35"])
	}
}

func TestArmyItemsAndCounts(t *testing.T) {
	items, rawCounts, err := armyItemsAndCounts(map[string]uint16{
		"u_5":  7,
		"s_2":  2,
		"h_1":  1,
		"noop": 9,
		"e_10": 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantItems := []string{"h_1", "s_2", "u_5"}
	if !reflect.DeepEqual(items, wantItems) {
		t.Fatalf("items = %#v, want %#v", items, wantItems)
	}

	var counts map[string]uint16
	if err := json.Unmarshal([]byte(rawCounts), &counts); err != nil {
		t.Fatal(err)
	}
	wantCounts := map[string]uint16{"h_1": 1, "s_2": 2, "u_5": 7}
	if !reflect.DeepEqual(counts, wantCounts) {
		t.Fatalf("counts = %#v, want %#v", counts, wantCounts)
	}
}

func TestLootedResourceColumns(t *testing.T) {
	gold, elixir, darkElixir := lootedResourceColumns([]clashy.Resource{
		{Name: "Gold", Amount: 10},
		{Name: "Elixir", Amount: 20},
		{Name: "DarkElixir", Amount: 3},
		{Name: "Gold", Amount: 5},
		{Name: "BuilderGold", Amount: 999},
	})
	if gold != 15 || elixir != 20 || darkElixir != 3 {
		t.Fatalf("resources = %d/%d/%d, want 15/20/3", gold, elixir, darkElixir)
	}
}

func TestEntriesAfterTimestamp(t *testing.T) {
	old := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)
	cutoff := old.Add(2 * time.Hour)
	entries := []clashy.BattleLogEntry{
		{OpponentPlayerTag: "#AAA", Stars: 3, ArmyShareCode: "u1x0", Timestamp: old.Add(3 * time.Hour)},
		{OpponentPlayerTag: "#BBB", Stars: 2, ArmyShareCode: "u1x1", Timestamp: old.Add(time.Hour)},
		{OpponentPlayerTag: "#CCC", Stars: 1, ArmyShareCode: "u1x2", Timestamp: old},
	}
	got := entriesAfterTimestamp(entries, cutoff)
	if len(got) != 1 || got[0].OpponentPlayerTag != "#AAA" {
		t.Fatalf("unexpected entries after timestamp: %#v", got)
	}
	if latest := latestBattlelogTimestamp(entries); !latest.Equal(old.Add(3 * time.Hour)) {
		t.Fatalf("latest timestamp = %s", latest)
	}
}

func TestActiveBattlelogTargetSQLUsesRecentActivity(t *testing.T) {
	for _, want := range []string{
		"FROM basic_player",
		"last_activity >= now() - interval '10 days'",
		"tag > $1",
		"ORDER BY tag",
	} {
		if !strings.Contains(activeBattlelogTargetSQL, want) {
			t.Fatalf("active target SQL missing %q: %s", want, activeBattlelogTargetSQL)
		}
	}
}

func TestPlayerSnapshotKey(t *testing.T) {
	if got := playerSnapshotKey("#ABC"); got != "ps:#ABC" {
		t.Fatalf("playerSnapshotKey = %q, want ps:#ABC", got)
	}
}

func TestBattlelogStatsEligibility(t *testing.T) {
	rows := []models.BattlelogRow{
		{Attack: true, PlayerTH: 16, OpponentTH: 16, BattleType: "ranked"},
		{Attack: true, PlayerTH: 16, OpponentTH: 15, BattleType: "ranked"},
		{Attack: false, PlayerTH: 16, OpponentTH: 16, BattleType: "ranked"},
		{Attack: true, PlayerTH: 16, OpponentTH: 16, BattleType: "friendly"},
		{Attack: true, PlayerTH: 17, OpponentTH: 17, BattleType: "legend"},
	}

	searchRows := qualifyingDynamicSearchRows(rows)
	if len(searchRows) != 2 {
		t.Fatalf("qualifyingDynamicSearchRows len = %d, want 2", len(searchRows))
	}
	if searchRows[0].BattleType != "ranked" || searchRows[1].BattleType != "legend" {
		t.Fatalf("unexpected qualifying rows: %#v", searchRows)
	}

	usage := attackRows(rows)
	if len(usage) != 3 {
		t.Fatalf("attackRows len = %d, want 3", len(usage))
	}
}

func TestAccumulateItemRollups(t *testing.T) {
	timestamp := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	rows := []models.BattlelogRow{
		{
			Attack:      true,
			PlayerTH:    16,
			OpponentTH:  16,
			LeagueID:    29000022,
			BattleType:  "legend",
			Stars:       3,
			Timestamp:   timestamp,
			ArmyColumns: map[string]uint16{"u_5": 7, "h_1": 1},
		},
		{
			Attack:      true,
			PlayerTH:    16,
			OpponentTH:  15,
			LeagueID:    29000022,
			BattleType:  "legend",
			Stars:       2,
			Timestamp:   timestamp,
			ArmyColumns: map[string]uint16{"u_5": 1},
		},
		{
			Attack:      false,
			PlayerTH:    16,
			OpponentTH:  16,
			LeagueID:    29000022,
			BattleType:  "legend",
			Stars:       0,
			Timestamp:   timestamp,
			ArmyColumns: map[string]uint16{"u_5": 1},
		},
	}

	buffer := newItemRollupBuffer()
	accumulateItemRollups(rows, &buffer)

	if buffer.PendingAttacks != 2 {
		t.Fatalf("PendingAttacks = %d, want 2", buffer.PendingAttacks)
	}

	usageKey := models.ItemRollupKey{
		DayStart:   dayStart(timestamp),
		PlayerTH:   16,
		LeagueID:   29000022,
		BattleType: "legend",
		ItemKey:    "u_5",
	}
	if got := buffer.Usage[usageKey]; got != 2 {
		t.Fatalf("usage u_5 = %d, want 2", got)
	}

	hitrateKey := models.ItemRollupKey{
		DayStart:   dayStart(timestamp),
		PlayerTH:   16,
		LeagueID:   29000022,
		BattleType: "legend",
		ItemKey:    "u_5",
	}
	if got := buffer.Hitrate[hitrateKey]; got.Attacks != 1 || got.ThreeStars != 1 {
		t.Fatalf("hitrate u_5 = %#v, want 1 attack and 1 three star", got)
	}
}

func TestRollupDateHelpers(t *testing.T) {
	value := time.Date(2026, 5, 16, 18, 30, 0, 0, time.UTC)
	if got := dayStart(value); !got.Equal(time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("dayStart = %s", got)
	}
}

func TestStarBuckets(t *testing.T) {
	tests := []struct {
		stars uint8
		want  [4]uint64
	}{
		{0, [4]uint64{1, 0, 0, 0}},
		{1, [4]uint64{0, 1, 0, 0}},
		{2, [4]uint64{0, 0, 1, 0}},
		{3, [4]uint64{0, 0, 0, 1}},
		{4, [4]uint64{0, 0, 0, 0}},
	}

	for _, tt := range tests {
		zeroStars, oneStars, twoStars, threeStars := starBuckets(models.BattlelogRow{Stars: tt.stars})
		got := [4]uint64{zeroStars, oneStars, twoStars, threeStars}
		if got != tt.want {
			t.Fatalf("starBuckets(%d) = %#v, want %#v", tt.stars, got, tt.want)
		}
	}
}

type fakeBattlelogStore struct {
	ingest models.BattlelogIngest
	calls  int
}

func (s *fakeBattlelogStore) NextTargetBatch(context.Context, int) (battlelogTargetBatch, error) {
	return battlelogTargetBatch{}, nil
}

func (s *fakeBattlelogStore) CommitTargetBatch(context.Context, battlelogTargetBatch) error {
	return nil
}

func (s *fakeBattlelogStore) LoadBasicPlayers(context.Context, []string) (map[string]models.BasicPlayerRow, error) {
	return nil, nil
}

func (s *fakeBattlelogStore) Store(_ context.Context, ingest models.BattlelogIngest) error {
	s.ingest = ingest
	s.calls++
	return nil
}

func (s *fakeBattlelogStore) Close() error { return nil }

func TestBattlelogsStorePersistsRowsAndNames(t *testing.T) {
	sink := &fakeBattlelogStore{}
	domain := &battlelogsDomain{sink: sink}
	app := &platform.App{
		Config: platform.Config{RunOnce: true},
		Stats:  platform.NewTracker(),
	}
	ingest := models.BattlelogIngest{
		Rows:        []models.BattlelogRow{{PlayerTag: "#PLAYER"}},
		Players:     []models.BasicPlayerRow{{Tag: "#PLAYER", Name: "Current", TownHall: 16}},
		Checkpoints: []models.BattlelogCheckpoint{{Tag: "#PLAYER", Timestamp: time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)}},
	}
	if err := domain.store(context.Background(), app, ingest); err != nil {
		t.Fatal(err)
	}
	if sink.calls != 1 {
		t.Fatalf("sink calls = %d, want 1", sink.calls)
	}
	if len(sink.ingest.Rows) != 1 || len(sink.ingest.Players) != 1 || len(sink.ingest.Checkpoints) != 1 {
		t.Fatalf("unexpected stored ingest: %#v", sink.ingest)
	}
}

func TestBasicPlayerUpsertSQLSkipsUnchangedProfiles(t *testing.T) {
	if !strings.Contains(utils.UpsertBasicPlayerSQL, "basic_player.name IS DISTINCT FROM EXCLUDED.name") {
		t.Fatalf("basic player upsert should skip unchanged profiles: %s", utils.UpsertBasicPlayerSQL)
	}
	if strings.Contains(utils.UpsertBasicPlayerSQL, "last_updated") || strings.Contains(utils.UpsertBasicPlayerSQL, "last_activity") {
		t.Fatalf("basic player upsert should not touch activity columns: %s", utils.UpsertBasicPlayerSQL)
	}
}
