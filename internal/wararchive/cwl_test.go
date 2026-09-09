package wararchive

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestCWLStatsUsesCanonicalDailyLeagueHistogram(t *testing.T) {
	stats := NewCWLStats()
	war := War{EndTime: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
		Clan: Clan{Members: []Member{{Tag: "#A", TownhallLevel: 18, Attacks: []Attack{
			{DefenderTag: "#B", Stars: 0, DestructionPercentage: 40, Duration: 60},
			{DefenderTag: "#B", Stars: 1, DestructionPercentage: 60, Duration: 90},
		}}}},
		Opponent: Clan{Members: []Member{{Tag: "#B", TownhallLevel: 17, Attacks: []Attack{
			{DefenderTag: "#A", Stars: 2, DestructionPercentage: 80, Duration: 120},
			{DefenderTag: "#A", Stars: 3, DestructionPercentage: 100, Duration: 150},
		}}}},
	}
	if err := stats.AddWar(war, 48000001); err != nil {
		t.Fatal(err)
	}
	if stats.Version != 1 || !stats.Coverage.Complete || !stats.Coverage.LeagueComplete || stats.Coverage.WarCount != 1 {
		t.Fatalf("bad coverage: %+v", stats)
	}
	hits := stats.ByDay["2026-09-01"].ByLeague["48000001"]
	if hits["18:17"].Attacks != 2 || hits["18:17"].OneStars.DestructionPercent != 60 || hits["17:18"].ThreeStars.DurationSeconds != 150 {
		t.Fatalf("bad histogram: %+v", hits)
	}
	raw, err := json.Marshal(stats)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	if result["coverage"].(map[string]any)["unknownLeagueWarCount"] != float64(0) {
		t.Fatalf("missing zero coverage field: %s", raw)
	}
	fixture, err := os.ReadFile("testdata/cwl-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected map[string]any
	if err := json.Unmarshal(fixture, &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result, expected) {
		t.Fatalf("writer differs from shared API fixture: %s", raw)
	}
}

func TestCWLStatsUnknownLeagueAndInvalidFrameCoverage(t *testing.T) {
	stats := NewCWLStats()
	war := War{EndTime: time.Date(2026, 9, 2, 1, 0, 0, 0, time.FixedZone("offset", 7200)), Clan: Clan{Members: []Member{{Tag: "#A", TownhallLevel: 18, Attacks: []Attack{{DefenderTag: "#B", Stars: 3, DestructionPercentage: 100}}}}}, Opponent: Clan{Members: []Member{{Tag: "#B", TownhallLevel: 17}}}}
	if err := stats.AddWar(war, 0); err != nil {
		t.Fatal(err)
	}
	if !stats.Coverage.Complete || stats.Coverage.LeagueComplete || stats.Coverage.UnknownLeagueWarCount != 1 {
		t.Fatalf("incorrect unknown attribution: %+v", stats.Coverage)
	}
	if stats.ByDay["2026-09-01"].ByLeague["unknown"]["18:17"].Attacks != 1 {
		t.Fatal("unknown hits must remain available globally and use UTC day")
	}
	war.Clan.Members[0].Attacks[0].DefenderTag = "#MISSING"
	if err := stats.AddWar(war, 48000001); err == nil {
		t.Fatal("invalid frame accepted")
	}
	if stats.Coverage.Complete {
		t.Fatal("partial data must not report complete")
	}
	if len(stats.ByDay["2026-09-01"].ByLeague["48000001"]) != 0 {
		t.Fatal("invalid war published partial hits")
	}
}
