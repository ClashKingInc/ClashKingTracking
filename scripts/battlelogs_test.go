//go:build script_internal_tests

package scripts

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestParseArmyColumnsAndNormalizeIgnoresOrder(t *testing.T) {
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
	wantCode := "h0p4e8_14-1p9e39i3x53d1x70u10x0-2x1s4x35"
	if got := normalizeArmyShareCode("u10x0-2x1s4x35i3x53d1x70h0p4e14_8-1p9e39"); got != wantCode {
		t.Fatalf("normalized left = %q, want %q", got, wantCode)
	}
	if got := normalizeArmyShareCode("h1p9e39-0p4e8_14d1x70i3x53s4x35u2x1-10x0"); got != wantCode {
		t.Fatalf("normalized right = %q, want %q", got, wantCode)
	}
}

func TestNormalizeArmyShareCodeFromLink(t *testing.T) {
	link := "https://link.clashofclans.com/en?action=CopyArmy&army=s1x120-4x35u4x65-10x8h6p16e49_35-1p9e17_48i2x65-1x5d1x9"
	want := "h1p9e17_48-6p16e35_49i1x5-2x65d1x9u10x8-4x65s4x35-1x120"
	if got := normalizeArmyShareCode(link); got != want {
		t.Fatalf("normalized army = %q, want %q", got, want)
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
		{OpponentPlayerTag: "#AAA", Stars: 3, ArmyShareCode: "u1x0", Timestamp: clashTimestamp(old.Add(3 * time.Hour))},
		{OpponentPlayerTag: "#BBB", Stars: 2, ArmyShareCode: "u1x1", Timestamp: clashTimestamp(old.Add(time.Hour))},
		{OpponentPlayerTag: "#CCC", Stars: 1, ArmyShareCode: "u1x2", Timestamp: clashTimestamp(old)},
	}
	got := entriesAfterTimestamp(entries, cutoff)
	if len(got) != 1 || got[0].OpponentPlayerTag != "#AAA" {
		t.Fatalf("unexpected entries after timestamp: %#v", got)
	}
	if latest := latestBattlelogTimestamp(entries); !latest.Equal(old.Add(3 * time.Hour)) {
		t.Fatalf("latest timestamp = %s", latest)
	}
}

func TestBattlelogTargetCursorRoundTrip(t *testing.T) {
	ttl := time.Date(2026, 5, 20, 10, 0, 0, 123, time.UTC)
	cursor := encodeBattlelogTargetCursor(ttl, "#PLAYER")
	got, err := decodeBattlelogTargetCursor(cursor)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}
	if !got.Valid || !got.TTL.Equal(ttl) || got.Tag != "#PLAYER" {
		t.Fatalf("unexpected cursor: %#v", got)
	}
}

func TestPlayerSnapshotKey(t *testing.T) {
	if got := playerSnapshotKey("#ABC"); got != "ps:#ABC" {
		t.Fatalf("playerSnapshotKey = %q, want ps:#ABC", got)
	}
}

func TestBattlelogRowFromEntryStoresPlayerAndOpponentNames(t *testing.T) {
	entry := clashy.BattleLogEntry{
		OpponentPlayerTag:     "#OPP",
		OpponentName:          "Opponent Name",
		OpponentTownHallLevel: 16,
		Duration:              173,
		Timestamp:             clashTimestamp(time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)),
	}

	row := battlelogRowFromEntry("#PLAYER", entry)
	if row.OpponentName != "Opponent Name" {
		t.Fatalf("opponent name = %q, want %q", row.OpponentName, "Opponent Name")
	}
	if row.Duration != 173 {
		t.Fatalf("duration = %d, want 173", row.Duration)
	}
	if row.OpponentTH != 17 {
		t.Fatalf("opponent th = %d, want 17", row.OpponentTH)
	}
	if row.ArmyShareCode != "" {
		t.Fatalf("army share code = %q, want empty", row.ArmyShareCode)
	}
}

func TestBattlelogBattleIDDedupesSwappedTagsAndAttackFlag(t *testing.T) {
	timestamp := clashTimestamp(time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC))
	attackEntry := clashy.BattleLogEntry{
		BattleType:        clashy.BattleTypeRanked,
		Attack:            true,
		OpponentPlayerTag: "#DEFENDER",
		Timestamp:         timestamp,
	}
	defenseEntry := clashy.BattleLogEntry{
		BattleType:        clashy.BattleTypeRanked,
		Attack:            false,
		OpponentPlayerTag: "#ATTACKER",
		Timestamp:         timestamp,
	}

	left := battlelogBattleID("#ATTACKER", attackEntry)
	right := battlelogBattleID("#DEFENDER", defenseEntry)
	if left != right {
		t.Fatalf("swapped player/opponent perspectives should share battle id: %s != %s", left, right)
	}
}

func TestBattlelogBattleIDUsesOnlyTagsAndTimestamp(t *testing.T) {
	base := clashy.BattleLogEntry{
		BattleType:        clashy.BattleTypeRanked,
		OpponentPlayerTag: "#DEFENDER",
		Timestamp:         clashTimestamp(time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)),
	}
	differentTimestamp := base
	differentTimestamp.Timestamp = clashTimestamp(time.Date(2026, 5, 20, 10, 0, 1, 0, time.UTC))
	differentType := base
	differentType.BattleType = clashy.BattleTypeLegend

	baseID := battlelogBattleID("#ATTACKER", base)
	if baseID == battlelogBattleID("#ATTACKER", differentTimestamp) {
		t.Fatalf("battle id should change when timestamp changes")
	}
	if baseID != battlelogBattleID("#ATTACKER", differentType) {
		t.Fatalf("battle id should ignore battle type")
	}
}

type fakeBattlelogStore struct {
	ingest models.BattlelogIngest
	calls  int
}

func (s *fakeBattlelogStore) NextTargetPage(context.Context, string, string, int) (battlelogTargetPage, error) {
	return battlelogTargetPage{}, nil
}

func (s *fakeBattlelogStore) CountTargets(context.Context, string) (int, error) {
	return 0, nil
}

func (s *fakeBattlelogStore) Store(_ context.Context, ingest models.BattlelogIngest) (int, error) {
	s.ingest = ingest
	s.calls++
	return len(ingest.Rows), nil
}

func (s *fakeBattlelogStore) Close() error { return nil }

func TestBattlelogsStorePersistsRowsAndNames(t *testing.T) {
	sink := &fakeBattlelogStore{}
	domain := &battlelogsDomain{sink: sink}
	app := &platform.App{
		Stats: platform.NewTracker(),
	}
	ingest := models.BattlelogIngest{
		Rows:        []models.BattlelogRow{{PlayerTag: "#PLAYER"}},
		Checkpoints: []models.BattlelogCheckpoint{{Tag: "#PLAYER", Timestamp: time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)}},
	}
	if err := domain.store(context.Background(), app, ingest); err != nil {
		t.Fatal(err)
	}
	if sink.calls != 1 {
		t.Fatalf("sink calls = %d, want 1", sink.calls)
	}
	if len(sink.ingest.Rows) != 1 || len(sink.ingest.Checkpoints) != 1 {
		t.Fatalf("unexpected stored ingest: %#v", sink.ingest)
	}
}

func TestMergeBattlelogIngestsKeepsLatestCheckpoint(t *testing.T) {
	older := time.Date(2026, 6, 28, 10, 0, 0, 0, time.UTC)
	newer := older.Add(time.Hour)
	got := mergeBattlelogIngests([]models.BattlelogIngest{
		{
			Rows:        []models.BattlelogRow{{PlayerTag: "#A"}},
			Checkpoints: []models.BattlelogCheckpoint{{Tag: "#A", Timestamp: older}},
		},
		{
			Rows: []models.BattlelogRow{{PlayerTag: "#B"}},
			Checkpoints: []models.BattlelogCheckpoint{
				{Tag: "#A", Timestamp: newer},
				{Tag: "#B", Timestamp: older},
			},
		},
	})
	if len(got.Rows) != 2 {
		t.Fatalf("unexpected merged rows: %#v", got)
	}
	if len(got.Checkpoints) != 2 {
		t.Fatalf("checkpoint len = %d, want 2: %#v", len(got.Checkpoints), got.Checkpoints)
	}
	if got.Checkpoints[0].Tag != "#A" || !got.Checkpoints[0].Timestamp.Equal(newer) {
		t.Fatalf("latest #A checkpoint not kept: %#v", got.Checkpoints)
	}
	if got.Checkpoints[1].Tag != "#B" || !got.Checkpoints[1].Timestamp.Equal(older) {
		t.Fatalf("unexpected #B checkpoint: %#v", got.Checkpoints)
	}
}

func TestTimescaleBattlelogStoreCopiesRowsThroughStage(t *testing.T) {
	dsn := os.Getenv("TRACKING_INTEGRATION_TIMESCALE_URL")
	if dsn == "" {
		t.Skip("TRACKING_INTEGRATION_TIMESCALE_URL is not set")
	}
	ctx := context.Background()
	store, err := newTimescaleBattlelogStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	now := time.Now().UTC().Truncate(time.Millisecond)
	entry := clashy.BattleLogEntry{
		ArmyShareCode:         "u1x8",
		OpponentPlayerTag:     "#CODEXOPP",
		OpponentName:          "Smoke Opponent",
		OpponentTownHallLevel: 16,
		BattleType:            clashy.BattleTypeRanked,
		Attack:                true,
		Stars:                 3,
		DestructionPercentage: 100,
		Duration:              180,
		Timestamp:             clashTimestamp(now),
	}
	row := battlelogRowFromEntry("#CODEXSMOKE", entry)
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM battlelogs WHERE battle_id = $1 AND timestamp = $2`, row.BattleID, row.Timestamp)
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM basic_player WHERE tag = '#CODEXSMOKE'`)
	})
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO basic_player (tag, name, league_id, clan_tag, townhall_level, battlelogs_tracking_ttl, trophies)
		VALUES ('#CODEXSMOKE', 'Smoke Player', 105000035, NULL, 17, NULL, 5000)
		ON CONFLICT (tag) DO UPDATE SET
			name = EXCLUDED.name,
			league_id = EXCLUDED.league_id,
			townhall_level = EXCLUDED.townhall_level,
			battlelogs_tracking_ttl = EXCLUDED.battlelogs_tracking_ttl,
			trophies = EXCLUDED.trophies
	`); err != nil {
		t.Fatal(err)
	}
	inserted, err := store.Store(ctx, models.BattlelogIngest{Rows: []models.BattlelogRow{row}})
	if err != nil {
		t.Fatal(err)
	}
	if inserted != 1 {
		t.Fatalf("inserted rows = %d, want 1", inserted)
	}
	var playerName string
	var playerTH int
	var armyCounts string
	if err := store.pool.QueryRow(ctx, `
		SELECT player_name, player_th, army_counts::text
		FROM battlelogs
		WHERE battle_id = $1 AND timestamp = $2
	`, row.BattleID, row.Timestamp).Scan(&playerName, &playerTH, &armyCounts); err != nil {
		t.Fatal(err)
	}
	if playerName != "Smoke Player" || playerTH != 17 {
		t.Fatalf("joined player = %q TH%d, want Smoke Player TH17", playerName, playerTH)
	}
	if !strings.Contains(armyCounts, `"u_8": 1`) {
		t.Fatalf("army_counts = %s, want u_8 count", armyCounts)
	}
}

func clashTimestamp(value time.Time) string {
	return value.UTC().Format("20060102T150405.000Z")
}
