//go:build script_internal_tests

package scripts

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
)

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
	if want := []string{"#A", "#B"}; !reflect.DeepEqual(got.MemberTags, want) {
		t.Fatalf("MemberTags = %#v, want %#v", got.MemberTags, want)
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
		"member_count <= 5",
		"last_active IS NULL",
		"last_active < now() - interval '7 days'",
	} {
		if !strings.Contains(inactiveGlobalClanTargetSQL, want) {
			t.Fatalf("inactive query missing %q: %s", want, inactiveGlobalClanTargetSQL)
		}
	}
}

func TestMemoryGlobalClanTargetCursorWrapsAfterBucketEnd(t *testing.T) {
	now := time.Now().UTC()
	store := newMemoryGlobalClanStore()
	store.rows = map[string]models.BasicClanRow{
		"#A": {Tag: "#A", MemberCount: 10, LastActive: &now},
		"#B": {Tag: "#B", MemberCount: 10, LastActive: &now},
		"#C": {Tag: "#C", MemberCount: 10, LastActive: &now},
	}

	first, err := store.NextTargetBatch(context.Background(), "active", 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"#A", "#B"}; !reflect.DeepEqual(first.Tags, want) {
		t.Fatalf("first tags = %#v, want %#v", first.Tags, want)
	}
	if first.ActiveCursor != "#B" {
		t.Fatalf("first active cursor = %q, want #B", first.ActiveCursor)
	}

	secondWithoutCommit, err := store.NextTargetBatch(context.Background(), "active", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(secondWithoutCommit.Tags, first.Tags) {
		t.Fatalf("cursor advanced before commit: %#v vs %#v", secondWithoutCommit.Tags, first.Tags)
	}

	if err := store.CommitTargetBatch(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second, err := store.NextTargetBatch(context.Background(), "active", 2)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"#C"}; !reflect.DeepEqual(second.Tags, want) {
		t.Fatalf("second tags = %#v, want %#v", second.Tags, want)
	}
	if second.ActiveCursor != "" {
		t.Fatalf("second active cursor = %q, want empty wrap", second.ActiveCursor)
	}
}

func TestBasicClanRowUsesUnrankedWarLeagueWhenMissing(t *testing.T) {
	got := basicClanRow(clashy.Clan{Tag: "#CLAN", MemberCount: 1})
	if got.LocationID != nil || got.CWLLeagueID != unrankedWarLeagueID || got.CapitalLeagueID != nil {
		t.Fatalf("optional ids/war league mismatch when missing: %#v", got)
	}
}

func TestBuildGlobalClanIngestOnlyUsesJoinLeaveForMembers(t *testing.T) {
	now := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)
	previous := &models.BasicClanRow{
		Tag:             "#CLAN",
		Description:     "before",
		ClanLevel:       10,
		CWLLeagueID:     48000001,
		CapitalLeagueID: intPtr(85000001),
		MemberTags:      []string{"#A", "#B"},
	}
	current := clashy.Clan{
		Tag:           "#CLAN",
		Description:   "after",
		Level:         11,
		MemberCount:   2,
		WarLeague:     clashy.League{ID: 48000002},
		CapitalLeague: &clashy.League{ID: 85000002},
		Members: []clashy.ClanMember{
			{Tag: "#B", Name: "Bee", TownHall: 15, Donations: 100, Received: 50},
			{Tag: "#C", Name: "Sea", TownHall: 16, Donations: 200, Received: 70},
		},
	}

	ingest := buildGlobalClanIngest(current, previous, now)
	if len(ingest.Clans) != 1 {
		t.Fatalf("Clans len = %d, want 1", len(ingest.Clans))
	}
	if len(ingest.JoinLeaves) != 2 {
		t.Fatalf("JoinLeaves len = %d, want 2: %#v", len(ingest.JoinLeaves), ingest.JoinLeaves)
	}
	if want := []string{"#CLAN"}; !reflect.DeepEqual(ingest.ActiveClanTags, want) {
		t.Fatalf("ActiveClanTags = %#v, want %#v", ingest.ActiveClanTags, want)
	}
	if ingest.JoinLeaves[0].EventType != "leave" || ingest.JoinLeaves[0].PlayerTag != "#A" {
		t.Fatalf("unexpected first join/leave row: %#v", ingest.JoinLeaves[0])
	}
	if ingest.JoinLeaves[1].EventType != "join" || ingest.JoinLeaves[1].PlayerTag != "#C" || ingest.JoinLeaves[1].TownHallLevel != 16 {
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

func TestBuildGlobalClanIngestDoesNotMarkLeaveOnlyClanActive(t *testing.T) {
	now := time.Date(2026, 5, 20, 10, 0, 0, 0, time.UTC)
	previous := &models.BasicClanRow{
		Tag:        "#CLAN",
		MemberTags: []string{"#A", "#B"},
	}
	current := clashy.Clan{
		Tag:         "#CLAN",
		MemberCount: 1,
		Members: []clashy.ClanMember{
			{Tag: "#B", Name: "Bee", TownHall: 15},
		},
	}

	ingest := buildGlobalClanIngest(current, previous, now)
	if len(ingest.JoinLeaves) != 1 || ingest.JoinLeaves[0].EventType != "leave" {
		t.Fatalf("expected one leave row: %#v", ingest.JoinLeaves)
	}
	if len(ingest.ActiveClanTags) != 0 {
		t.Fatalf("leave-only change should not mark active: %#v", ingest.ActiveClanTags)
	}
}
