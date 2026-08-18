//go:build script_internal_tests

package scripts

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
)

type countingGlobalClanStore struct {
	*memoryGlobalClanStore
	loadCalls int
	loaded    []string
}

func (s *countingGlobalClanStore) Load(ctx context.Context, tags []string) (map[string]models.BasicClanRow, error) {
	s.loadCalls++
	s.loaded = append([]string(nil), tags...)
	return s.memoryGlobalClanStore.Load(ctx, tags)
}

func globalClanIngestForTest(current clashy.Clan, previous *models.BasicClanRow, now time.Time) models.GlobalClanIngest {
	row := basicClanRow(current)
	snapshot := globalClanSnapshot{Clan: current, Row: row, FetchedAt: now}
	if previous != nil {
		return buildGlobalClanIngest(snapshot, &globalClanSnapshot{Row: *previous})
	}
	return buildGlobalClanIngest(snapshot, nil)
}

func TestBasicClanRowUsesPersistedShape(t *testing.T) {
	clan := clashy.Clan{
		Tag:           "#CLAN",
		Name:          "Test Clan",
		Description:   "old words",
		Level:         12,
		PublicWarLog:  true,
		WarWins:       44,
		MemberCount:   2,
		Badge:         clashy.Badge{Small: "small", Medium: "medium"},
		Location:      &clashy.Location{ID: 32000006},
		WarLeague:     clashy.League{ID: 48000010},
		CapitalLeague: &clashy.League{ID: 85000006},
		Members: []clashy.ClanMember{
			{Tag: "#B", Donations: 3, Received: 4},
			{Tag: "#A", Donations: 5, Received: 6},
		},
	}

	got := basicClanRow(clan)
	if got.Tag != "#CLAN" || got.Name != "Test Clan" || got.Description != "old words" || got.ClanLevel != 12 {
		t.Fatalf("unexpected profile row: %#v", got)
	}
	if optionalIntValue(got.LocationID) != 32000006 ||
		got.CWLLeagueID != 48000010 ||
		optionalIntValue(got.CapitalLeagueID) != 85000006 {
		t.Fatalf("unexpected league/location ids: %#v", got)
	}
	if got.BadgeURL != "medium" || got.TroopsDonated != 8 || got.TroopsReceived != 10 {
		t.Fatalf("unexpected badge/troops: %#v", got)
	}
	if want := []models.BasicClanMember{{Tag: "#A"}, {Tag: "#B"}}; !reflect.DeepEqual(got.Members, want) {
		t.Fatalf("Members = %#v, want %#v", got.Members, want)
	}
}

func TestGlobalClanTargetSQLClassifiesBuckets(t *testing.T) {
	if !strings.Contains(activeGlobalClanTargetSQL, "member_count > 5") {
		t.Fatalf("active query should require member_count > 5: %s", activeGlobalClanTargetSQL)
	}
	if !strings.Contains(activeGlobalClanTargetSQL, "last_active >= now() - interval '7 days'") {
		t.Fatalf("active query should require recent last_active: %s", activeGlobalClanTargetSQL)
	}
	for _, want := range []string{
		"member_count > 0",
		"member_count <= 5",
		"last_active IS NULL",
		"last_active < now() - interval '7 days'",
	} {
		if !strings.Contains(inactiveGlobalClanTargetSQL, want) {
			t.Fatalf("inactive query missing %q: %s", want, inactiveGlobalClanTargetSQL)
		}
	}
}

func TestMemoryGlobalClanTargetListsSeedActiveAndInactiveBuckets(t *testing.T) {
	now := time.Now().UTC()
	store := newMemoryGlobalClanStore()
	old := now.Add(-8 * 24 * time.Hour)
	store.rows = map[string]models.BasicClanRow{
		"#A": {Tag: "#A", MemberCount: 10, LastActive: &now},
		"#B": {Tag: "#B", MemberCount: 4, LastActive: &now},
		"#C": {Tag: "#C", MemberCount: 10, LastActive: &old},
		"#D": {Tag: "#D", MemberCount: 10},
		"#E": {Tag: "#E", MemberCount: 0},
	}

	active, err := store.ListTargetTags(context.Background(), "active", 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"#A"}; !reflect.DeepEqual(active, want) {
		t.Fatalf("active tags = %#v, want %#v", active, want)
	}

	inactive, err := store.ListTargetTags(context.Background(), "inactive", 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"#B", "#C", "#D"}; !reflect.DeepEqual(inactive, want) {
		t.Fatalf("inactive tags = %#v, want %#v", inactive, want)
	}
}

func TestGlobalClanWriterBulkLoadsPreviousRows(t *testing.T) {
	store := &countingGlobalClanStore{memoryGlobalClanStore: newMemoryGlobalClanStore()}
	store.rows["#A"] = models.BasicClanRow{Tag: "#A", Name: "Before A", MemberCount: 1}
	store.rows["#B"] = models.BasicClanRow{Tag: "#B", Name: "Before B", MemberCount: 1}
	app := &platform.App{
		Config: platform.Config{MockDB: true},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Stats:  platform.NewTracker(),
	}
	writer := newGlobalClanAsyncWriter(app, store)
	now := time.Now().UTC()
	err := writer.writeBatch(t.Context(), []globalClanWriteJob{{
		Group: "priority",
		Snapshots: []globalClanSnapshot{
			{Clan: clashy.Clan{Tag: "#A", Name: "After A", MemberCount: 1}, Row: models.BasicClanRow{Tag: "#A", Name: "After A", MemberCount: 1}, FetchedAt: now},
			{Clan: clashy.Clan{Tag: "#B", Name: "After B", MemberCount: 1}, Row: models.BasicClanRow{Tag: "#B", Name: "After B", MemberCount: 1}, FetchedAt: now},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if store.loadCalls != 1 {
		t.Fatalf("previous row loads = %d, want one bulk load", store.loadCalls)
	}
	if want := []string{"#A", "#B"}; !reflect.DeepEqual(store.loaded, want) {
		t.Fatalf("loaded tags = %#v, want %#v", store.loaded, want)
	}
}

func TestGlobalClanWriterShardsEachClanDeterministically(t *testing.T) {
	store := newMemoryGlobalClanStore()
	app := &platform.App{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Stats:  platform.NewTracker(),
	}
	writer := newGlobalClanAsyncWriter(app, store, 3)
	snapshots := []globalClanSnapshot{
		{Row: models.BasicClanRow{Tag: "#A"}},
		{Row: models.BasicClanRow{Tag: "#B"}},
		{Row: models.BasicClanRow{Tag: "#C"}},
		{Row: models.BasicClanRow{Tag: "#D"}},
	}
	if err := writer.enqueue(t.Context(), globalClanWriteJob{Group: "non_priority", Snapshots: snapshots}); err != nil {
		t.Fatal(err)
	}

	seen := make(map[string]int)
	for index, queue := range writer.jobs {
		for len(queue) > 0 {
			job := <-queue
			for _, snapshot := range job.Snapshots {
				if got := globalClanWriterShard(snapshot.Row.Tag, len(writer.jobs)); got != index {
					t.Fatalf("tag %s queued on shard %d, want %d", snapshot.Row.Tag, index, got)
				}
				seen[snapshot.Row.Tag]++
			}
		}
	}
	for _, snapshot := range snapshots {
		if seen[snapshot.Row.Tag] != 1 {
			t.Fatalf("tag %s queued %d times, want once", snapshot.Row.Tag, seen[snapshot.Row.Tag])
		}
	}
	if writer.batchSize != globalClanAsyncWriteBatchSize*3 {
		t.Fatalf("batch size = %d, want %d", writer.batchSize, globalClanAsyncWriteBatchSize*3)
	}
	for index, queue := range writer.jobs {
		if cap(queue) != globalClanAsyncWriteQueueSize {
			t.Fatalf("queue %d capacity = %d, want %d", index, cap(queue), globalClanAsyncWriteQueueSize)
		}
	}
}

func TestGlobalClanFetchWorkerCountUsesOneSecondRequestPopulation(t *testing.T) {
	if got := bulkFetchWorkerCount(3750, 11250); got != 3750 {
		t.Fatalf("workers = %d, want 3750", got)
	}
	if got := bulkFetchWorkerCount(100, 40); got != 40 {
		t.Fatalf("capped workers = %d, want 40", got)
	}
}

func TestGlobalClanOnlyDefersGatewayTimeout(t *testing.T) {
	timeout := &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 504}}
	if !isDeferredBulkFetch(timeout) {
		t.Fatal("504 gateway timeout should be deferred to the next scan")
	}
	unavailable := &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 502}}
	if isDeferredBulkFetch(unavailable) {
		t.Fatal("502 proxy outage must remain governed by the availability gate")
	}
	throttled := &clashy.HTTPException{Status: 429}
	if !isDeferredBulkFetch(throttled) {
		t.Fatal("exhausted 429 should be deferred to the next scan")
	}
	if !isDeferredBulkFetch(io.ErrUnexpectedEOF) {
		t.Fatal("exhausted truncated response should be deferred to the next scan")
	}
}

func TestBasicClanRowUsesUnrankedWarLeagueWhenMissing(t *testing.T) {
	got := basicClanRow(clashy.Clan{
		Tag:               "#CLAN",
		MemberCount:       1,
		Points:            50000,
		BuilderBasePoints: 42000,
		CapitalPoints:     3100,
	})
	if got.LocationID != nil || got.CWLLeagueID != unrankedWarLeagueID || got.CapitalLeagueID != nil {
		t.Fatalf("optional ids/war league mismatch when missing: %#v", got)
	}
	if got.ClanPoints != 50000 || got.BuilderBasePoints != 42000 || got.CapitalPoints != 3100 {
		t.Fatalf("typed clan points were not preserved: %#v", got)
	}
}

func TestBasicClanUpsertStoresAllTypedPointFields(t *testing.T) {
	for _, field := range []string{"clan_points", "builder_base_points", "capital_points"} {
		if !strings.Contains(upsertBasicClanSQL, field+" = EXCLUDED."+field) {
			t.Fatalf("basic clan upsert does not update %s: %s", field, upsertBasicClanSQL)
		}
		if !strings.Contains(upsertBasicClanSQL, "basic_clan."+field+" IS DISTINCT FROM EXCLUDED."+field) {
			t.Fatalf("basic clan upsert does not compare %s: %s", field, upsertBasicClanSQL)
		}
	}
}

func TestBuildGlobalClanIngestOnlyUsesJoinLeaveForMembers(t *testing.T) {
	now := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)
	previous := models.BasicClanRow{
		Tag:             "#CLAN",
		Description:     "before",
		ClanLevel:       10,
		CWLLeagueID:     48000001,
		CapitalLeagueID: intPtr(85000001),
		MemberCount:     2,
		Members:         []models.BasicClanMember{{Tag: "#A", Name: "Ay"}, {Tag: "#B", Name: "Bee"}},
	}
	current := clashy.Clan{
		Tag:           "#CLAN",
		Description:   "after",
		Level:         11,
		Points:        34000,
		WarWinStreak:  7,
		MemberCount:   2,
		WarLeague:     clashy.League{ID: 48000002},
		CapitalLeague: &clashy.League{ID: 85000002},
		Members: []clashy.ClanMember{
			{Tag: "#B", Name: "Bee", TownHall: 15, Donations: 100, Received: 50},
			{Tag: "#C", Name: "Sea", TownHall: 16, Donations: 200, Received: 70},
		},
	}

	ingest := globalClanIngestForTest(current, &previous, now)
	if len(ingest.Clans) != 1 {
		t.Fatalf("Clans len = %d, want 1", len(ingest.Clans))
	}
	if ingest.Clans[0].ClanPoints != 34000 || ingest.Clans[0].WarWinStreak != 7 {
		t.Fatalf("current clan records should be stored on basic clan row: %#v", ingest.Clans[0])
	}
	if len(ingest.Players) != 2 || ingest.Players[0].ClanTag != "#CLAN" || ingest.Players[1].ClanTag != "#CLAN" {
		t.Fatalf("player rows should carry source clan tag: %#v", ingest.Players)
	}
	nowPtr := now
	if want := []models.ClanRecordRow{
		{Tag: "#CLAN", ClanPoints: 34000, ClanPointsAt: &nowPtr, WarWinStreak: 7, WarWinStreakAt: &nowPtr},
	}; !reflect.DeepEqual(ingest.ClanRecords, want) {
		t.Fatalf("ClanRecords = %#v, want %#v", ingest.ClanRecords, want)
	}
	if len(ingest.JoinLeaves) != 2 {
		t.Fatalf("JoinLeaves len = %d, want 2: %#v", len(ingest.JoinLeaves), ingest.JoinLeaves)
	}
	if want := []string{"#CLAN"}; !reflect.DeepEqual(ingest.ActiveClanTags, want) {
		t.Fatalf("ActiveClanTags = %#v, want %#v", ingest.ActiveClanTags, want)
	}
	if ingest.JoinLeaves[0].EventType != "leave" || ingest.JoinLeaves[0].PlayerTag != "#A" || ingest.JoinLeaves[0].PlayerName != "Ay" {
		t.Fatalf("unexpected first join/leave row: %#v", ingest.JoinLeaves[0])
	}
	if ingest.JoinLeaves[1].EventType != "join" || ingest.JoinLeaves[1].PlayerTag != "#C" || ingest.JoinLeaves[1].PlayerName != "Sea" || ingest.JoinLeaves[1].TownHallLevel != 16 {
		t.Fatalf("unexpected second join/leave row: %#v", ingest.JoinLeaves[1])
	}
	if len(ingest.ClanChanges) != 4 {
		t.Fatalf("ClanChanges len = %d, want 4: %#v", len(ingest.ClanChanges), ingest.ClanChanges)
	}
	for _, row := range ingest.ClanChanges {
		if row.ChangeType == "donations" || row.ChangeType == "donationsReceived" {
			t.Fatalf("member donation change should not be emitted: %#v", row)
		}
	}
}

func TestClanRecordRowsSkipsMissingValues(t *testing.T) {
	now := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)
	if got := clanRecordRows(clashy.Clan{Tag: "#CLAN"}, models.BasicClanRow{}, now); len(got) != 0 {
		t.Fatalf("zero-valued records should be skipped: %#v", got)
	}
	if got := clanRecordRows(clashy.Clan{Points: 100, WarWinStreak: 3}, models.BasicClanRow{}, now); len(got) != 0 {
		t.Fatalf("records without clan tag should be skipped: %#v", got)
	}
	if got := clanRecordRows(
		clashy.Clan{Tag: "#CLAN", Points: 100, WarWinStreak: 3},
		models.BasicClanRow{RecordClanPoints: 100, RecordWarWinStreak: 3},
		now,
	); len(got) != 0 {
		t.Fatalf("records that do not beat stored highs should be skipped: %#v", got)
	}
}

func TestBuildGlobalClanIngestDoesNotMarkLeaveOnlyClanActive(t *testing.T) {
	now := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)
	previous := models.BasicClanRow{
		Tag:         "#CLAN",
		MemberCount: 2,
		Members:     []models.BasicClanMember{{Tag: "#A", Name: "Ay"}, {Tag: "#B", Name: "Bee"}},
	}
	current := clashy.Clan{
		Tag:         "#CLAN",
		MemberCount: 1,
		Members: []clashy.ClanMember{
			{Tag: "#B", Name: "Bee", TownHall: 15},
		},
	}

	ingest := globalClanIngestForTest(current, &previous, now)
	if len(ingest.JoinLeaves) != 1 || ingest.JoinLeaves[0].EventType != "leave" {
		t.Fatalf("expected one leave row: %#v", ingest.JoinLeaves)
	}
	if ingest.JoinLeaves[0].PlayerName != "Ay" {
		t.Fatalf("leave row should use previous member name: %#v", ingest.JoinLeaves[0])
	}
	if len(ingest.ActiveClanTags) != 0 {
		t.Fatalf("leave-only change should not mark active: %#v", ingest.ActiveClanTags)
	}
}

func TestZeroMemberClanDeletesRow(t *testing.T) {
	now := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)
	current := clashy.Clan{Tag: "#CLAN", Name: "Deleted Clan", Level: 6, MemberCount: 0}

	ingest := globalClanIngestForTest(current, nil, now)
	if len(ingest.Clans) != 0 {
		t.Fatalf("zero-member clan should not be upserted: %#v", ingest.Clans)
	}
	if want := []string{"#CLAN"}; !reflect.DeepEqual(ingest.DeletedClanTags, want) {
		t.Fatalf("zero-member clan should be written as a delete: %#v", ingest.DeletedClanTags)
	}
}

func TestBuildGlobalClanIngestSkipsHistoryForFirstHydration(t *testing.T) {
	now := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)
	previous := models.BasicClanRow{
		Tag:         "#CLAN",
		MemberCount: 2,
	}
	current := clashy.Clan{
		Tag:           "#CLAN",
		Name:          "Hydrated Clan",
		Description:   "real description",
		Level:         12,
		MemberCount:   2,
		WarLeague:     clashy.League{ID: 48000005},
		CapitalLeague: &clashy.League{ID: 85000001},
		Members: []clashy.ClanMember{
			{Tag: "#A", Name: "Ay", TownHall: 15},
			{Tag: "#B", Name: "Bee", TownHall: 16},
		},
	}

	ingest := globalClanIngestForTest(current, &previous, now)
	if len(ingest.Clans) != 1 || ingest.Clans[0].MemberCount != 2 {
		t.Fatalf("hydrated clan should be upserted: %#v", ingest.Clans)
	}
	if len(ingest.JoinLeaves) != 0 {
		t.Fatalf("first hydration should not emit join/leave history: %#v", ingest.JoinLeaves)
	}
	if len(ingest.ClanChanges) != 0 {
		t.Fatalf("first hydration should not emit clan change history: %#v", ingest.ClanChanges)
	}
	if len(ingest.ActiveClanTags) != 0 {
		t.Fatalf("first hydration should not mark active from synthetic joins: %#v", ingest.ActiveClanTags)
	}
}
