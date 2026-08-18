//go:build script_internal_tests

package scripts

import (
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

	changes, activity := playerChanges(
		"#PLAYER",
		previous,
		current,
		time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC),
	)

	if !activity {
		t.Fatalf("activity was not detected")
	}
	if len(changes) != 2 {
		t.Fatalf("profile change count = %d, want 2", len(changes))
	}
	if got := playerChangeTypes(changes); !reflect.DeepEqual(got, []string{"name", "townHallLevel"}) {
		t.Fatalf("change types = %v", got)
	}
}

func TestMultipleActivitySignalsProduceOneOnlineObservation(t *testing.T) {
	snapshots := newTrackedPlayerSnapshotStore(nil)
	previous := clashy.Player{
		Tag:       "#PLAYER",
		Name:      "Old Name",
		Donations: 10,
		Clan:      &clashy.PlayerClan{Tag: "#CLAN"},
		Achievements: []clashy.Achievement{{
			Name: playerSeasonPassAchievement, Value: 100,
		}},
	}
	previousRaw, err := json.Marshal(previous)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.StoreAndClear(
		t.Context(), playerSnapshotKey(previous.Tag), playerStatPendingKey(previous.Tag), previousRaw,
	); err != nil {
		t.Fatal(err)
	}
	domain := &trackedPlayersDomain{snapshots: snapshots}
	current := previous
	current.Name = "New Name"
	current.Donations = 20
	current.Achievements = []clashy.Achievement{{
		Name: playerSeasonPassAchievement, Value: 150,
	}}
	ingest, err := domain.doPlayer(t.Context(), current.Tag, current)
	if err != nil {
		t.Fatal(err)
	}
	if ingest.LastOnlineAt == nil {
		t.Fatal("multiple activity signals did not produce an online observation")
	}
	if len(ingest.StatChanges) != 2 {
		t.Fatalf("stat changes = %d, want donations and season pass", len(ingest.StatChanges))
	}
}

func TestPlayerStatChangesBuildOnlyTypedPositiveDeltas(t *testing.T) {
	eventTime := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	previous := clashy.Player{
		Donations:                10,
		Received:                 4,
		ClanCapitalContributions: 100,
		Achievements: []clashy.Achievement{
			{Name: playerClanGamesAchievement, Value: 1_000},
			{Name: playerSeasonPassAchievement, Value: 10_000},
		},
	}
	current := clashy.Player{
		Donations:                15,
		Received:                 8,
		ClanCapitalContributions: 175,
		Clan:                     &clashy.PlayerClan{Tag: "#CURRENT"},
		Achievements: []clashy.Achievement{
			{Name: playerClanGamesAchievement, Value: 1_500},
			{Name: playerSeasonPassAchievement, Value: 10_200},
		},
	}

	rows := playerStatChanges("#PLAYER", previous, current, eventTime)
	if len(rows) != 5 {
		t.Fatalf("stat change rows = %d, want 5: %#v", len(rows), rows)
	}
	want := []struct {
		statType string
		previous int64
		current  int64
		delta    int64
	}{
		{playerStatDonated, 10, 15, 5},
		{playerStatReceived, 4, 8, 4},
		{playerStatClanGames, 1_000, 1_500, 500},
		{playerStatCapitalGoldDonated, 100, 175, 75},
		{playerStatSeasonPass, 10_000, 10_200, 200},
	}
	for index, expected := range want {
		row := rows[index]
		if row.EventTime != eventTime ||
			row.PlayerTag != "#PLAYER" ||
			row.ClanTag == nil ||
			*row.ClanTag != "#CURRENT" ||
			row.StatType != expected.statType ||
			row.PreviousValue != expected.previous ||
			row.CurrentValue != expected.current ||
			row.Delta != expected.delta {
			t.Fatalf("stat row %d = %#v, want %#v", index, row, expected)
		}
	}
}

func TestPlayerStatChangesIgnoreEqualAndResetCounters(t *testing.T) {
	previous := clashy.Player{
		Donations:                20,
		Received:                 10,
		ClanCapitalContributions: 200,
		Achievements: []clashy.Achievement{{
			Name:  playerClanGamesAchievement,
			Value: 2_000,
		}},
	}
	current := clashy.Player{
		Donations:                20,
		Received:                 0,
		ClanCapitalContributions: 100,
		Achievements: []clashy.Achievement{{
			Name:  playerClanGamesAchievement,
			Value: 500,
		}},
	}
	if rows := playerStatChanges("#PLAYER", previous, current, time.Now().UTC()); len(rows) != 0 {
		t.Fatalf("equal/reset counters emitted stat changes: %#v", rows)
	}

	current.ClanCapitalContributions = 250
	rows := playerStatChanges("#PLAYER", previous, current, time.Now().UTC())
	if len(rows) != 1 || rows[0].StatType != playerStatCapitalGoldDonated || rows[0].ClanTag != nil {
		t.Fatalf("clanless positive delta = %#v", rows)
	}
}

func TestFirstObservationWritesNoPlayerStatChanges(t *testing.T) {
	domain := &trackedPlayersDomain{snapshots: newTrackedPlayerSnapshotStore(nil)}
	ingest, err := domain.doPlayer(t.Context(), "#PLAYER", clashy.Player{
		Tag:       "#PLAYER",
		Donations: 100,
		Achievements: []clashy.Achievement{{
			Name:  playerClanGamesAchievement,
			Value: 2_000,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.StatChanges) != 0 {
		t.Fatalf("first observation emitted stat changes: %#v", ingest.StatChanges)
	}
}

func TestUnchangedPlayerDoesNotRewriteSnapshot(t *testing.T) {
	snapshots := newTrackedPlayerSnapshotStore(nil)
	player := clashy.Player{Tag: "#PLAYER", Name: "Same"}
	raw, err := json.Marshal(player)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.StoreAndClear(
		t.Context(), playerSnapshotKey(player.Tag), playerStatPendingKey(player.Tag), raw,
	); err != nil {
		t.Fatal(err)
	}
	domain := &trackedPlayersDomain{snapshots: snapshots}
	ingest, err := domain.doPlayer(t.Context(), player.Tag, player)
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.SnapshotRaw) != 0 {
		t.Fatalf("unchanged player requested a snapshot rewrite: %d bytes", len(ingest.SnapshotRaw))
	}
}

func TestTrackedPlayerTargetsAreTH9ConfiguredClanMembers(t *testing.T) {
	if strings.Contains(trackedPlayerTargetSetSQL, "tracked_player_targets") {
		t.Fatalf("obsolete explicit target table remains in query: %s", trackedPlayerTargetSetSQL)
	}
	for _, fragment := range []string{
		"server.last_command_at >= now() - interval '90 days'",
		"member->>'town_hall'",
		">= 9",
	} {
		if !strings.Contains(trackedPlayerTargetSetSQL, fragment) {
			t.Fatalf("tracked player target query omits %q: %s", fragment, trackedPlayerTargetSetSQL)
		}
	}
}

func TestCounterResetStillAdvancesSnapshotAfterSuccessfulIngest(t *testing.T) {
	snapshots := newTrackedPlayerSnapshotStore(nil)
	previous := clashy.Player{Tag: "#PLAYER", Donations: 100}
	previousRaw, _ := json.Marshal(previous)
	if err := snapshots.StoreAndClear(
		t.Context(),
		playerSnapshotKey("#PLAYER"),
		playerStatPendingKey("#PLAYER"),
		previousRaw,
	); err != nil {
		t.Fatal(err)
	}
	domain := &trackedPlayersDomain{
		snapshots: snapshots,
		store:     newMemoryTrackedPlayerStore(),
	}
	current := clashy.Player{Tag: "#PLAYER", Donations: 0}
	ingest, err := domain.doPlayer(t.Context(), "#PLAYER", current)
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.StatChanges) != 0 {
		t.Fatalf("counter reset emitted stat changes: %#v", ingest.StatChanges)
	}
	if ingest.Event.Topic != "" {
		t.Fatalf("counter reset emitted an empty player event: %#v", ingest.Event)
	}
	app := &platform.App{
		Config: platform.Config{MockDB: true},
		Stats:  platform.NewTracker(),
	}
	if err := domain.storePlayerIngest(t.Context(), app, ingest); err != nil {
		t.Fatal(err)
	}
	stored, ok, err := snapshots.Load(t.Context(), playerSnapshotKey("#PLAYER"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !equalBytes(stored, ingest.SnapshotRaw) {
		t.Fatalf("snapshot was not advanced after reset: ok=%v stored=%s", ok, stored)
	}
}

func TestSupportedProfileChangePublishesPlayerEvent(t *testing.T) {
	snapshots := newTrackedPlayerSnapshotStore(nil)
	previous := clashy.Player{
		Tag:  "#PLAYER",
		Name: "Before",
		Clan: &clashy.PlayerClan{Tag: "#CLAN"},
	}
	previousRaw, err := json.Marshal(previous)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshots.StoreAndClear(
		t.Context(), playerSnapshotKey(previous.Tag), playerStatPendingKey(previous.Tag), previousRaw,
	); err != nil {
		t.Fatal(err)
	}
	domain := &trackedPlayersDomain{
		snapshots: snapshots,
		eventInterests: map[string]map[string]struct{}{
			"#CLAN": {"name_change": {}},
		},
	}
	current := previous
	current.Name = "After"
	ingest, err := domain.doPlayer(t.Context(), current.Tag, current)
	if err != nil {
		t.Fatal(err)
	}
	if ingest.Event.Topic != "player" {
		t.Fatalf("supported profile change event = %#v", ingest.Event)
	}
	if _, exists := ingest.Event.Value["new_player"]; exists {
		t.Fatalf("player event contains the full new profile: %#v", ingest.Event.Value)
	}
	if _, exists := ingest.Event.Value["old_player"]; exists {
		t.Fatalf("player event contains the full old profile: %#v", ingest.Event.Value)
	}
	changes, ok := ingest.Event.Value["changes"].([]map[string]any)
	if !ok || len(changes) != 1 || changes[0]["type"] != "name" {
		t.Fatalf("compact player event changes = %#v", ingest.Event.Value["changes"])
	}
}

func TestPlayerEventRequiresMatchingConfiguredConsumer(t *testing.T) {
	domain := &trackedPlayersDomain{
		eventInterests: map[string]map[string]struct{}{
			"#CLAN": {"hero_upgrade": {}},
		},
	}
	changes := []models.PlayerProfileChangeRow{
		{ChangeType: "name"},
		{ChangeType: "heroes"},
		{ChangeType: "bestTrophies"},
	}
	filtered, logs := domain.interestedPlayerChanges("#CLAN", changes)
	if len(filtered) != 1 || filtered[0].ChangeType != "heroes" {
		t.Fatalf("filtered changes = %#v", filtered)
	}
	if !reflect.DeepEqual(logs, []string{"hero_upgrade"}) {
		t.Fatalf("matched log types = %#v", logs)
	}
	if filtered, _ := domain.interestedPlayerChanges("#OTHER", changes); len(filtered) != 0 {
		t.Fatalf("unconfigured clan received changes: %#v", filtered)
	}
}

func TestTrackedPlayerStatSQLUsesOnlyFinalTypedColumns(t *testing.T) {
	for _, column := range []string{
		"event_time",
		"player_tag",
		"clan_tag",
		"stat_type",
		"previous_value",
		"current_value",
		"delta",
	} {
		if !strings.Contains(insertPlayerStatChangesSQL, column) {
			t.Fatalf("player stat INSERT omits %q: %s", column, insertPlayerStatChangesSQL)
		}
	}
	for _, forbidden := range []string{
		"season",
		"townhall",
		"activity",
		"attack_wins",
		"last_online",
		"data",
		"trophies",
		"loot",
	} {
		if strings.Contains(insertPlayerStatChangesSQL, forbidden) {
			t.Fatalf("player stat INSERT retains forbidden field %q: %s", forbidden, insertPlayerStatChangesSQL)
		}
	}
	if !strings.Contains(insertPlayerProfileChangeSQL, "INSERT INTO player_change_history") {
		t.Fatalf("profile change INSERT does not use canonical table: %s", insertPlayerProfileChangeSQL)
	}
	for _, fragment := range []string{
		"WITH existing AS MATERIALIZED",
		"updated AS (",
		"UPDATE player_stat_changes",
		"WHERE event_time = $1",
		"AND player_tag = $2",
		"AND stat_type = $4",
		"AND current_value < $6",
		"WHERE NOT EXISTS (SELECT 1 FROM existing)",
		"AND NOT EXISTS (SELECT 1 FROM updated)",
	} {
		if !strings.Contains(insertPlayerStatChangesSQL, fragment) {
			t.Fatalf("player stat retry guard omits %q: %s", fragment, insertPlayerStatChangesSQL)
		}
	}
	if !strings.Contains(lockPlayerStatChangesSQL, "pg_advisory_xact_lock") {
		t.Fatalf("player stat writer lacks per-player transaction lock: %s", lockPlayerStatChangesSQL)
	}
	for _, fragment := range []string{
		"INSERT INTO player_online_events",
		"WHERE NOT EXISTS",
		"seen_at = $1",
		"tag = $2",
		"clan_tag = $3",
	} {
		if !strings.Contains(insertPlayerOnlineEventSQL, fragment) {
			t.Fatalf("player online INSERT omits %q: %s", fragment, insertPlayerOnlineEventSQL)
		}
	}
}

func TestTrackedPlayersSourceHasNoSeasonStatsRuntime(t *testing.T) {
	source, err := os.ReadFile("tracked_players.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, stale := range []string{
		"player_" + "season_stats",
		"Player" + "SeasonStatRow",
		"Season" + "Stats",
		"upsertPlayer" + "SeasonStats",
	} {
		if strings.Contains(string(source), stale) {
			t.Fatalf("tracked_players runtime retains %q", stale)
		}
	}
}

func TestMemoryTrackedPlayerStorePagesByLocalCursor(t *testing.T) {
	store := newMemoryTrackedPlayerStore()
	store.targets = []models.TrackedPlayerTarget{{Tag: "#A"}, {Tag: "#B"}, {Tag: "#C"}}

	first, err := store.NextTargetPage(t.Context(), "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []models.TrackedPlayerTarget{{Tag: "#A"}, {Tag: "#B"}}; !reflect.DeepEqual(first.Targets, want) {
		t.Fatalf("first targets = %#v, want %#v", first.Targets, want)
	}
	if first.NextCursor != "#B" {
		t.Fatalf("first cursor = %q, want #B", first.NextCursor)
	}

	second, err := store.NextTargetPage(t.Context(), first.NextCursor, 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []models.TrackedPlayerTarget{{Tag: "#C"}}; !reflect.DeepEqual(second.Targets, want) {
		t.Fatalf("second targets = %#v, want %#v", second.Targets, want)
	}
	if second.NextCursor != "" {
		t.Fatalf("second cursor = %q, want wrap cursor", second.NextCursor)
	}
}

func TestMemoryTrackedPlayerSnapshotsPersistRawBytes(t *testing.T) {
	store := newTrackedPlayerSnapshotStore(nil)
	if err := store.StoreAndClear(
		t.Context(),
		"ps:#A",
		playerStatPendingKey("#A"),
		[]byte("snapshot"),
	); err != nil {
		t.Fatal(err)
	}
	raw, ok, err := store.Load(t.Context(), "ps:#A")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(raw) != "snapshot" {
		t.Fatalf("snapshot = %q, ok = %t", raw, ok)
	}
}

func TestPlayerStatRetryReusesEventTimeUntilSnapshotAdvances(t *testing.T) {
	domain := &trackedPlayersDomain{snapshots: newTrackedPlayerSnapshotStore(nil)}
	previousRaw := []byte(`{"tag":"#A","donations":10}`)
	firstProposal := time.Date(2026, 7, 28, 12, 0, 0, 123456789, time.UTC)
	first, err := domain.reservePlayerStatEventTime(
		t.Context(),
		"#A",
		previousRaw,
		firstProposal,
	)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := domain.reservePlayerStatEventTime(
		t.Context(),
		"#A",
		previousRaw,
		firstProposal.Add(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Equal(first) {
		t.Fatalf("retry event time = %s, want stable %s", retry, first)
	}
	if first.Nanosecond()%1_000 != 0 {
		t.Fatalf("reserved event time exceeds PostgreSQL microsecond precision: %s", first)
	}

	if err := domain.savePlayerSnapshot(t.Context(), "#A", []byte(`{"tag":"#A","donations":15}`)); err != nil {
		t.Fatal(err)
	}
	nextProposal := firstProposal.Add(2 * time.Minute)
	next, err := domain.reservePlayerStatEventTime(
		t.Context(),
		"#A",
		previousRaw,
		nextProposal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !next.Equal(nextProposal.UTC().Truncate(time.Microsecond)) || next.Equal(first) {
		t.Fatalf("post-advance event time = %s, want new %s", next, nextProposal)
	}
}
