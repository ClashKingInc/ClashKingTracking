package scripts

import (
	clashy "github.com/clashkinginc/clashy.go"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWardenModePreservedButCompositionUnchanged(t *testing.T) {
	a, e := normalizeArmyShareCodeChecked("h2m1p16e5_41u2x0")
	if e != nil || a != "h2m1p16e5_41u2x0" {
		t.Fatalf("mode lost: %s %v", a, e)
	}
	b, e := normalizeArmyShareCodeChecked("h2m0p16e5_41u2x0")
	if e != nil || a == b {
		t.Fatalf("distinct stored modes: %s %v", b, e)
	}
	if !reflect.DeepEqual(parseArmyColumns(a), parseArmyColumns(b)) {
		t.Fatal("mode changed composition columns")
	}
	for _, bad := range []string{"h2m", "h2m1m0p16", "h2z1p16"} {
		if _, e := normalizeArmyShareCodeChecked(bad); e == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}

func TestPreviousRankSentinelIsNotGenericNegativeAcceptance(t *testing.T) {
	rank, e := optionalPreviousRank(-1)
	if rank != nil || e != nil {
		t.Fatalf("sentinel: %v %v", rank, e)
	}
	if _, e = optionalPreviousRank(-2); e == nil {
		t.Fatal("accepted invalid rank")
	}
	if _, e = optionalHistoryPositiveInt(-1); e == nil {
		t.Fatal("generic validator relaxed")
	}
}

func TestExpectedMissingLocalRankingsOnly(t *testing.T) {
	e := &clashy.NotFound{HTTPException: &clashy.HTTPException{Status: 404, Message: "Rankings not found for location"}}
	if !unavailableLocalRanking("32000000", e) || unavailableLocalRanking("global", e) {
		t.Fatal("incorrect missing scope")
	}
	other := &clashy.NotFound{HTTPException: &clashy.HTTPException{Status: 404, Message: "invalid endpoint"}}
	if unavailableLocalRanking("32000000", other) {
		t.Fatal("hid unrelated failure")
	}
}

func TestIncompleteClanDoesNotRejectPlayerHistory(t *testing.T) {
	a, b, c, e := leaderboardPlayerClan(&clashy.PlayerClan{Tag: "#P0Y", Name: "Clan"})
	if e != nil || a != nil || b != nil || c != nil {
		t.Fatalf("optional metadata: %v %v %v %v", a, b, c, e)
	}
}

func TestAlreadyArchivedAndOldSeasonIdentifiersAreSkipped(t *testing.T) {
	now := time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC)
	got, e := missingCompletedLegendSeasons([]string{"2025-10-06", "2025-09"}, map[string]struct{}{"2025-10-06": {}}, now)
	if e != nil || len(got) != 0 {
		t.Fatalf("old history retried: %v %v", got, e)
	}
	got, e = missingCompletedLegendSeasons([]string{"2025-10-06"}, nil, now)
	if e != nil || len(got) != 0 {
		t.Fatalf("old unlisted history retried: %v %v", got, e)
	}
}

func TestWeeklyResetWindow(t *testing.T) {
	for _, test := range []struct {
		at   string
		want bool
	}{
		{"2026-09-21T16:29:59Z", false}, {"2026-09-21T16:30:00Z", true},
		{"2026-09-21T16:59:59Z", true}, {"2026-09-21T17:00:00Z", false},
		{"2026-09-22T16:30:00Z", false}, {"2026-09-21T11:30:00-05:00", true},
	} {
		at, _ := time.Parse(time.RFC3339, test.at)
		_, got := rankedResetWindow(at)
		if got != test.want {
			t.Errorf("%s: %v", test.at, got)
		}
	}
}

func TestCurrentClanRankingsUseExistingSchema(t *testing.T) {
	if strings.Contains(replaceCurrentClanRankingGroupSQL, "updated_at") {
		t.Fatal("writer references removed column")
	}
}
