package scripts

import (
	"testing"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestSetupArmyNormalizationPreservesWardenModeAndMovesSiege(t *testing.T) {
	static, err := clashy.LoadStaticData()
	if err != nil {
		t.Fatal(err)
	}
	code := normalizeSetupArmyCode(static, "h2m1p16e5_41u2x0-1x51")
	if code != "h2m1p16e5_41i1x51u2x0" {
		t.Fatalf("normalized code = %q", code)
	}
	if siege := normalizedSetupSiegeID(static, code); siege != clashy.TroopBaseID+51 {
		t.Fatalf("normalized CC siege ID = %d", siege)
	}
	if other := normalizeSetupArmyCode(static, "h2m0p16e5_41u2x0-1x51"); other == code {
		t.Fatal("distinct Warden modes collapsed")
	}
	if mixed := normalizeSetupArmyCode(static, "i1x51u2x0-1x51"); mixed != "i1x51u2x0" {
		t.Fatalf("duplicate siege was not collapsed: %q", mixed)
	}
	if mixed := normalizeSetupArmyCode(static, "u1x52-1x51"); mixed != "i1x51" {
		t.Fatalf("multiple siege choices were not deterministic: %q", mixed)
	}
	if bad := normalizeSetupArmyCode(static, "h2m"); bad != "" {
		t.Fatalf("malformed army should remain unclassified, got %q", bad)
	}
}

func TestSetupCountsSiegeIsObservedPerAttack(t *testing.T) {
	counts := setupCounts{}
	for _, siege := range []int{123, 123, 456, 0} {
		if err := counts.add(setupAttack{stars: 3, destruction: 100, siegeID: siege}); err != nil {
			t.Fatal(err)
		}
	}
	if counts.attacks != 4 || counts.sieges[123] != 2 || counts.sieges[456] != 1 || len(counts.sieges) != 2 {
		t.Fatalf("siege observations = %+v", counts)
	}
}

func TestSetupPopulationKeepsUnknownArmiesAndRankCohorts(t *testing.T) {
	static, err := clashy.LoadStaticData()
	if err != nil {
		t.Fatal(err)
	}
	rank100, rank500, rank1500 := 100, 500, 1500
	code := normalizeSetupArmyCode(static, "u20x5")
	attacks := []setupAttack{
		{code: code, rank: &rank100, stars: 3, destruction: 100},
		{code: code, rank: &rank500, stars: 2, destruction: 80},
		{code: "", rank: &rank1500, stars: 0, destruction: 20},
	}
	cohorts, result, err := calculateDailySetups(attacks, static)
	if err != nil {
		t.Fatal(err)
	}
	if result.TotalAttacks != 3 || cohorts[0].counts.attacks != 3 || cohorts[1].counts.attacks != 2 || cohorts[2].counts.attacks != 1 {
		t.Fatalf("source totals result=%d cohorts=%d,%d,%d", result.TotalAttacks, cohorts[0].counts.attacks, cohorts[1].counts.attacks, cohorts[2].counts.attacks)
	}
	if result.ReviewAttacks < 1 || result.ClassifiedAttacks+result.ReviewAttacks != result.TotalAttacks {
		t.Fatalf("classified=%d review=%d total=%d", result.ClassifiedAttacks, result.ReviewAttacks, result.TotalAttacks)
	}
}
