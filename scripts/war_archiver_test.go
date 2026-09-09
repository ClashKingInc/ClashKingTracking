package scripts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/wararchive"
)

func TestArchiveStatsTreatCWLAsAnExistingWarTypeWithoutLeagueBreakdown(t *testing.T) {
	stats := wararchive.NewPackStats()
	end := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	wars := []pendingArchiveWar{
		{ID: 1, WarType: "random", War: validArchiveStatsWar(end)},
		{ID: 2, WarType: "cwl", War: validArchiveStatsWar(end)},
		{ID: 3, WarType: "cwl", War: validArchiveStatsWar(end.Add(time.Hour))},
	}
	for _, pending := range wars {
		stats.Add(pending.WarType, pending.War)
	}

	if got := stats.ByDay["2026-09-01"].WarsByType["cwl"]; got != 2 {
		t.Fatalf("base pending CWL total = %d, want 2", got)
	}
	payload, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	var topLevel map[string]any
	if err := json.Unmarshal(payload, &topLevel); err != nil {
		t.Fatal(err)
	}
	if _, exists := topLevel["cwl"]; exists {
		t.Fatalf("new CWL league statistics leaked into pack metadata: %s", payload)
	}
}

func TestArchivePackClaimDoesNotComputeNewCWLLeagueStatistics(t *testing.T) {
	for _, fragment := range []string{"WHERE pending.pack_id = $1", "JOIN wars ON wars.war_id = pending.war_id"} {
		if !strings.Contains(claimArchivePackWarsSQL, fragment) {
			t.Fatalf("archive pack query missing %q", fragment)
		}
	}
	for _, excluded := range []string{"cwl_groups", "cwl_league_id", "jsonb_array_elements", "war_tag"} {
		if strings.Contains(claimArchivePackWarsSQL, excluded) {
			t.Fatalf("archive pack query must not compute CWL league statistics through %q", excluded)
		}
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
