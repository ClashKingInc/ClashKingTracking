//go:build script_internal_tests

package scripts

import (
	"context"
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

func TestBattlelogIngestQueuesOnlyNewLegendDefensesForNotification(t *testing.T) {
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	checkpoint := models.BattlelogCheckpoint{Tag: "#PLAYER", Timestamp: now.Add(-time.Hour)}
	entry := func(mode clashy.BattleType, attack bool, offset time.Duration) clashy.BattleLogEntry {
		return clashy.BattleLogEntry{
			BattleType: mode, Attack: attack, OpponentPlayerTag: "#P0Y",
			OpponentTownHallLevel: 17, Timestamp: clashTimestamp(now.Add(offset)),
		}
	}
	ingest, err := battlelogIngestFromEntries([]clashy.BattleLogEntry{
		entry(clashy.BattleTypeLegend, true, time.Minute),
		entry(clashy.BattleTypeLegend, false, 2*time.Minute),
		entry(clashy.BattleTypeRanked, false, 3*time.Minute),
	}, "#PLAYER", checkpoint, now.Add(time.Hour), 14)
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.Notifications) != 1 {
		t.Fatalf("notification rows = %#v, want one Legend defense", ingest.Notifications)
	}
	if row := ingest.Notifications[0]; row.Attack || battlelogStorageMode(clashy.BattleType(row.BattleType)) != "legend" {
		t.Fatalf("notification row = %#v, want Legend defense", row)
	}

	firstSeen, err := battlelogIngestFromEntries([]clashy.BattleLogEntry{
		entry(clashy.BattleTypeLegend, false, 4*time.Minute),
	}, "#PLAYER", models.BattlelogCheckpoint{}, now.Add(time.Hour), 14)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstSeen.Notifications) != 0 {
		t.Fatalf("first-seen history must not emit notifications: %#v", firstSeen.Notifications)
	}
}

func TestLegendDefenseEventIDUsesPlayerAndBattleTime(t *testing.T) {
	timestamp := time.Date(2026, 9, 11, 12, 0, 0, 123, time.UTC)
	base := models.BattlelogRow{PlayerTag: "#P0Y", Timestamp: timestamp}
	if left, right := legendDefenseEventID(base), legendDefenseEventID(base); left == "" || left != right {
		t.Fatalf("event ID is not deterministic: %q %q", left, right)
	}
	changedPlayer := base
	changedPlayer.PlayerTag = "#Q0Y"
	if legendDefenseEventID(base) == legendDefenseEventID(changedPlayer) {
		t.Fatal("event ID did not change with player tag")
	}
	changedTime := base
	changedTime.Timestamp = timestamp.Add(time.Nanosecond)
	if legendDefenseEventID(base) == legendDefenseEventID(changedTime) {
		t.Fatal("event ID did not change with battle time")
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

func TestBattlelogRowFromEntryRejectsValuesBeforeNarrowing(t *testing.T) {
	tests := []struct {
		name  string
		entry clashy.BattleLogEntry
		want  string
	}{
		{name: "negative destruction", entry: clashy.BattleLogEntry{DestructionPercentage: -1}, want: "destruction percentage"},
		{name: "oversized destruction", entry: clashy.BattleLogEntry{DestructionPercentage: 101}, want: "destruction percentage"},
		{name: "negative duration", entry: clashy.BattleLogEntry{Duration: -1}, want: "duration"},
		{name: "duration above smallint", entry: clashy.BattleLogEntry{Duration: 32768}, want: "duration"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := battlelogRowFromEntry("#PLAYER", test.entry)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("battlelogRowFromEntry error = %v, want %q validation error", err, test.want)
			}
		})
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
	ingest  models.BattlelogIngest
	calls   int
	onStore func()
}

func (s *fakeBattlelogStore) LoadTargets(context.Context, string) ([]string, error) {
	return nil, nil
}

func (s *fakeBattlelogStore) Store(_ context.Context, ingest models.BattlelogIngest) (int, error) {
	s.ingest = ingest
	s.calls++
	if s.onStore != nil {
		s.onStore()
	}
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

func TestBattlelogsStoreEnqueuesDefenseBeforeAdvancingCheckpoint(t *testing.T) {
	order := []string{}
	sink := &fakeBattlelogStore{onStore: func() { order = append(order, "sql") }}
	domain := &battlelogsDomain{
		sink: sink,
		publishLegendDefense: func(context.Context, *platform.App, models.BattlelogRow) error {
			order = append(order, "stream")
			return nil
		},
		updateCheckpoints: func(context.Context, []models.BattlelogCheckpoint) error {
			order = append(order, "checkpoint")
			return nil
		},
	}
	app := &platform.App{Stats: platform.NewTracker()}
	ingest := models.BattlelogIngest{
		Rows:          []models.BattlelogRow{{PlayerTag: "#P0Y"}},
		Notifications: []models.BattlelogRow{{PlayerTag: "#P0Y"}},
		Checkpoints:   []models.BattlelogCheckpoint{{Tag: "#P0Y", Timestamp: time.Now().UTC()}},
	}
	if err := domain.store(t.Context(), app, ingest); err != nil {
		t.Fatal(err)
	}
	want := []string{"sql", "stream", "checkpoint"}
	if len(order) != len(want) {
		t.Fatalf("store order = %#v, want %#v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("store order = %#v, want %#v", order, want)
		}
	}
}

func TestBattlelogsStoreDoesNotCheckpointWhenDefenseEnqueueFails(t *testing.T) {
	checkpointed := false
	domain := &battlelogsDomain{
		sink: &fakeBattlelogStore{},
		publishLegendDefense: func(context.Context, *platform.App, models.BattlelogRow) error {
			return context.Canceled
		},
		updateCheckpoints: func(context.Context, []models.BattlelogCheckpoint) error {
			checkpointed = true
			return nil
		},
	}
	app := &platform.App{Stats: platform.NewTracker()}
	err := domain.store(t.Context(), app, models.BattlelogIngest{
		Rows:          []models.BattlelogRow{{PlayerTag: "#P0Y"}},
		Notifications: []models.BattlelogRow{{PlayerTag: "#P0Y"}},
		Checkpoints:   []models.BattlelogCheckpoint{{Tag: "#P0Y", Timestamp: time.Now().UTC()}},
	})
	if err == nil {
		t.Fatal("expected stream enqueue failure")
	}
	if checkpointed {
		t.Fatal("checkpoint advanced after stream enqueue failure")
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
