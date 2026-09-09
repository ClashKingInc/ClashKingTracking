package scripts

import (
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/wararchive"
)

func TestArchiveStatsCountCWLFromPendingThroughUploadPayload(t *testing.T) {
	stats := wararchive.NewPackStats()
	stats.CWL = wararchive.NewCWLStats()
	end := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	wars := []pendingArchiveWar{
		{ID: 1, WarType: "random", War: validArchiveStatsWar(end)},
		{ID: 2, WarType: "cwl", CWLLeagueID: 48000001, War: validArchiveStatsWar(end)},
		{ID: 3, WarType: "cwl", War: validArchiveStatsWar(end.Add(time.Hour))},
	}
	for _, pending := range wars {
		if err := addPendingArchiveWarStats(&stats, pending); err != nil {
			t.Fatal(err)
		}
	}

	if got := stats.ByDay["2026-09-01"].WarsByType["cwl"]; got != 2 {
		t.Fatalf("base pending CWL total = %d, want 2", got)
	}
	if got := stats.CWL.Coverage.WarCount; got != 2 {
		t.Fatalf("uploaded CWL coverage total = %d, want 2", got)
	}
	if stats.CWL.Coverage.LeagueComplete || stats.CWL.Coverage.UnknownLeagueWarCount != 1 {
		t.Fatalf("unknown historic attribution was not retained: %+v", stats.CWL.Coverage)
	}
	if got := stats.CWL.ByDay["2026-09-01"].ByLeague["48000001"]["18:17"].Attacks; got != 1 {
		t.Fatalf("known league attacks = %d, want 1", got)
	}
	if got := stats.CWL.ByDay["2026-09-01"].ByLeague["unknown"]["18:17"].Attacks; got != 1 {
		t.Fatalf("unknown league attacks = %d, want 1", got)
	}
}

func TestArchiveCWLLeagueAttributionUsesUniqueStoredGroup(t *testing.T) {
	for _, fragment := range []string{
		"count(DISTINCT groups.cwl_id) = 1",
		"min(groups.cwl_league_id)",
		"jsonb_array_elements(groups.rounds)",
		"WHEN 'array' THEN round.value",
		"WHEN 'object' THEN COALESCE(round.value -> 'warTags'",
		"cwl_attribution.war_tag = wars.war_tag",
	} {
		if !strings.Contains(claimArchivePackWarsSQL, fragment) {
			t.Fatalf("historic CWL attribution query missing %q", fragment)
		}
	}
	if strings.Contains(claimArchivePackWarsSQL, "basic_clan") {
		t.Fatal("archive attribution must not use the clan's current league")
	}
}

func validArchiveStatsWar(end time.Time) wararchive.War {
	return wararchive.War{
		TeamSize:         1,
		AttacksPerMember: 1,
		EndTime:          end,
		Clan: wararchive.Clan{Members: []wararchive.Member{{
			Tag: "#A", TownhallLevel: 18,
			Attacks: []wararchive.Attack{{DefenderTag: "#B", Stars: 3, DestructionPercentage: 100, Duration: 90}},
		}}},
		Opponent: wararchive.Clan{Members: []wararchive.Member{{Tag: "#B", TownhallLevel: 17}}},
	}
}
