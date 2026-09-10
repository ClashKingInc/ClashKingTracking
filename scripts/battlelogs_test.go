//go:build script_internal_tests

package scripts

import (
	"context"
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

func TestNormalizeArmyShareCodePreservesZeroBasedPetID(t *testing.T) {
	withFirstPet := normalizeArmyShareCode("h0p0e1_8u1x5")
	withoutPet := normalizeArmyShareCode("h0e1_8u1x5")
	if withFirstPet != "h0p0e1_8u1x5" {
		t.Fatalf("zero-based pet was dropped: %q", withFirstPet)
	}
	if withFirstPet == withoutPet {
		t.Fatalf("armies with and without pet ID 0 normalized identically: %q", withFirstPet)
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

func TestLootedResourceColumns(t *testing.T) {
	gold, elixir, darkElixir := lootedResourceColumns([]clashy.Resource{
		{Name: "Gold", Amount: 10},
		{Name: "Elixir", Amount: 20},
		{Name: "DarkElixir", Amount: 3},
		{Name: "Gold", Amount: 5},
		{Name: "BuilderGold", Amount: 999},
		{Name: "SourElixir", Amount: 999},
	}, []clashy.Resource{
		{Name: "Gold", Amount: 7},
		{Name: "Elixir", Amount: 8},
		{Name: "DarkElixir", Amount: 9},
	})
	if gold != 22 || elixir != 28 || darkElixir != 12 {
		t.Fatalf("resources = %d/%d/%d, want 22/28/12", gold, elixir, darkElixir)
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
}

func TestBattlelogCheckpointTracksCompleteBattleDataInsteadOfPollTime(t *testing.T) {
	now := time.Date(2026, 9, 8, 5, 0, 0, 0, time.UTC)
	checkpoint := models.BattlelogCheckpoint{Tag: "#PLAYER", Timestamp: now.Add(-2 * time.Hour)}
	lateBattleTime := now.Add(-time.Hour)
	incomplete := clashy.BattleLogEntry{
		BattleType: clashy.BattleTypeRanked,
		Attack:     true,
		Timestamp:  clashTimestamp(lateBattleTime),
	}

	for name, entries := range map[string][]clashy.BattleLogEntry{
		"empty":      nil,
		"incomplete": {incomplete},
	} {
		ingest, err := battlelogIngestFromEntries(entries, "#PLAYER", checkpoint, now, 14)
		if err != nil {
			t.Fatalf("%s response: %v", name, err)
		}
		if len(ingest.Rows) != 0 || len(ingest.Checkpoints) != 0 {
			t.Fatalf("%s response advanced durable state: %#v", name, ingest)
		}
	}

	complete := incomplete
	complete.OpponentPlayerTag = "#OPPONENT"
	complete.OpponentTownHallLevel = 17
	complete.ArmyShareCode = "u1x0"
	ingest, err := battlelogIngestFromEntries([]clashy.BattleLogEntry{complete}, "#PLAYER", checkpoint, now.Add(time.Minute), 14)
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.Rows) != 1 || len(ingest.Checkpoints) != 1 || !ingest.Checkpoints[0].Timestamp.Equal(lateBattleTime) {
		t.Fatalf("completed late battle was not recovered: %#v", ingest)
	}
}

func TestBattlelogRequestConcurrencyIsMemoryBounded(t *testing.T) {
	tests := []struct {
		rps  int
		want int
	}{
		{rps: 0, want: 0},
		{rps: 100, want: 300},
		{rps: 900, want: 1000},
		{rps: 4000, want: 1000},
	}
	for _, test := range tests {
		if got := battlelogRequestConcurrency(test.rps); got != test.want {
			t.Fatalf("battlelogRequestConcurrency(%d) = %d, want %d", test.rps, got, test.want)
		}
	}
}

func TestBattlelogRowFromEntryConvertsZeroIndexedOpponentTownHall(t *testing.T) {
	entry := clashy.BattleLogEntry{
		OpponentPlayerTag:     "#OPP",
		OpponentName:          "Opponent Name",
		OpponentTownHallLevel: 16,
		Duration:              173,
		Timestamp:             clashTimestamp(time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)),
	}

	row, err := battlelogRowFromEntry("#PLAYER", entry)
	if err != nil {
		t.Fatal(err)
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

func TestBattlelogStorageModeAcceptsWireAndConstantValues(t *testing.T) {
	tests := map[clashy.BattleType]string{
		clashy.BattleTypeHomeVillage:     "farming",
		clashy.BattleTypeRanked:          "ranked",
		clashy.BattleTypeLegend:          "legend",
		clashy.BattleType("homeVillage"): "farming",
		clashy.BattleType("ranked"):      "ranked",
		clashy.BattleType("legend"):      "legend",
	}
	for input, want := range tests {
		if got := battlelogStorageMode(input); got != want {
			t.Errorf("battlelogStorageMode(%q) = %q, want %q", input, got, want)
		}
	}
}

type fakeBattlelogStore struct {
	ingest models.BattlelogIngest
	calls  int
}

func (s *fakeBattlelogStore) LoadTargets(context.Context, string) ([]string, error) {
	return nil, nil
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

func clashTimestamp(value time.Time) string {
	return value.UTC().Format("20060102T150405.000Z")
}

func TestBattlelogZeroIndexedTownHallBounds(t *testing.T) {
	now := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	for _, th := range []int{0, 16, 19, -1, 20} {
		entry := clashy.BattleLogEntry{BattleType: clashy.BattleTypeLegend, Attack: true, OpponentPlayerTag: "#P0Y", OpponentTownHallLevel: th, ArmyShareCode: "u1x0", Timestamp: clashTimestamp(now)}
		ingest, err := battlelogIngestFromEntries([]clashy.BattleLogEntry{entry}, "#Y2", models.BattlelogCheckpoint{}, now, 14)
		if err != nil {
			t.Fatal(err)
		}
		if th < 0 || th >= 20 {
			if len(ingest.Rows) != 0 || len(ingest.Checkpoints) != 0 {
				t.Fatalf("invalid index %d advanced ingest", th)
			}
			continue
		}
		if len(ingest.Rows) != 1 || int(ingest.Rows[0].OpponentTH) != th+1 {
			t.Fatalf("index %d: %#v", th, ingest.Rows)
		}
	}
}
