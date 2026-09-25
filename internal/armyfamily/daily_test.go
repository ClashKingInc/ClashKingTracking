package armyfamily

import (
	"reflect"
	"slices"
	"testing"
)

func TestClassifyDailyTroopsFirstAndStableSignatures(t *testing.T) {
	rows := make([]DailyRecipe, 0, 14)
	for i := 0; i < 12; i++ {
		code := string(rune('a' + i))
		row := DailyRecipe{Code: code, Attacks: 100, TroopHousing: map[int]int{1: 180 + i, 99: 20 - i}}
		if i < 6 {
			row.Spells = map[int]int{20: 2}
			row.Equipment = []HeroEquipment{{HeroID: 30, EquipmentID: 40}}
		} else {
			row.Spells = map[int]int{21: 2}
			row.Equipment = []HeroEquipment{{HeroID: 30, EquipmentID: 41}}
		}
		rows = append(rows, row)
	}
	rows = append(rows,
		DailyRecipe{Code: "different", Attacks: 75, TroopHousing: map[int]int{2: 200}},
		DailyRecipe{Code: "hidden", Attacks: 15},
	)
	options := DailyOptions{SupportTroops: map[int]bool{99: true}}
	got := ClassifyDaily(rows, options)
	if got.TotalAttacks != 1290 || got.ClassifiedAttacks != 1275 || got.ReviewAttacks != 15 {
		t.Fatalf("attack accounting: %+v", got)
	}
	if len(got.Families) != 2 || got.Families[0].Signature != "[1]" || got.Families[0].Attacks != 1200 || got.Families[1].Signature != "[2]" {
		t.Fatalf("troop families: %+v", got.Families)
	}
	if got.CodeFamily["a"] != "[1]" || got.CodeFamily["different"] != "[2]" || got.CodeFamily["hidden"] != "" {
		t.Fatalf("transient membership: %+v", got.CodeFamily)
	}
	if len(got.Families[0].Setups) == 0 {
		t.Fatal("supported associated setup was not discovered")
	}
	for _, setup := range got.Families[0].Setups {
		if !slices.Equal(setup.CoreTroopIDs, []int{1}) || setup.Evidence.Attacks != 600 || setup.Evidence.RecipeCount != 6 || setup.RepresentativeCode == "" {
			t.Fatalf("setup overview and evidence: %+v", setup)
		}
		if !MatchSetup(rows[0], setup.Conditions) && !MatchSetup(rows[6], setup.Conditions) {
			t.Fatalf("setup does not match either observed pattern: %+v", setup)
		}
	}
	slices.Reverse(rows)
	second := ClassifyDaily(rows, options)
	if !reflect.DeepEqual(got, second) {
		t.Fatalf("classification depends on input order\nfirst: %#v\nsecond: %#v", got, second)
	}
}

func TestClassifyDailyGatesNeedDistinctRecipesAndKeepUnknownUsage(t *testing.T) {
	rows := []DailyRecipe{
		{Code: "one", Attacks: 1000, TroopHousing: map[int]int{1: 200}, Spells: map[int]int{20: 1}, Equipment: []HeroEquipment{{HeroID: 30, EquipmentID: 40}}},
		{Code: "two", Attacks: 1000, TroopHousing: map[int]int{1: 200}},
		{Code: "unknown", Attacks: 9},
	}
	got := ClassifyDaily(rows, DailyOptions{})
	if got.TotalAttacks != 2009 || got.ReviewAttacks != 9 || len(got.Families) != 1 || len(got.Families[0].Setups) != 0 {
		t.Fatalf("single-recipe coincidence should not become setup: %+v", got)
	}
	if MatchSetup(rows[0], []SetupCondition{{Kind: "equipment", HeroID: 31, ID: 40, Minimum: 1}}) {
		t.Fatal("equipment on another hero matched")
	}
}

func TestClassifyDailyDoesNotCapEvidenceBackedSetups(t *testing.T) {
	rows := make([]DailyRecipe, 0, 70)
	for pattern := 0; pattern < 7; pattern++ {
		for recipe := 0; recipe < 10; recipe++ {
			rows = append(rows, DailyRecipe{
				Code: string(rune(1000 + len(rows))), Attacks: 10,
				TroopHousing: map[int]int{1: 200 + recipe},
				Spells:       map[int]int{200 + pattern: 1},
				Equipment:    []HeroEquipment{{HeroID: 30, EquipmentID: 300 + pattern}},
			})
		}
	}
	got := ClassifyDaily(rows, DailyOptions{})
	if len(got.Families) != 1 || len(got.Families[0].Setups) != 7 {
		t.Fatalf("expected all 7 supported independent setups, got %+v", got.Families)
	}
	for _, setup := range got.Families[0].Setups {
		if setup.Evidence.Attacks != 100 || setup.Evidence.RecipeCount != 10 {
			t.Fatalf("setup evidence: %+v", setup)
		}
		if setup.Evidence.NovelAttacks != 100 || setup.Evidence.NovelShare != 1 {
			t.Fatalf("independent setup should have fully novel coverage: %+v", setup.Evidence)
		}
	}
}

func TestClassifyDailyIgnoresCodeOnlyVariantsForEvidence(t *testing.T) {
	rows := make([]DailyRecipe, 0, 12)
	for i := 0; i < 12; i++ {
		row := DailyRecipe{Code: string(rune(2000 + i)), Attacks: 100, TroopHousing: map[int]int{1: 200}}
		if i < 6 {
			row.Spells = map[int]int{20: 2}
			row.Equipment = []HeroEquipment{{HeroID: 30, EquipmentID: 40}}
		}
		rows = append(rows, row)
	}
	got := ClassifyDaily(rows, DailyOptions{})
	if len(got.Families) != 1 || got.Families[0].RecipeCount != 2 || len(got.Families[0].Setups) != 0 {
		t.Fatalf("code-only variants inflated distinct recipe support: %+v", got.Families)
	}
}

func TestClassifyDailyDeduplicatesNestedSpellThresholds(t *testing.T) {
	rows := make([]DailyRecipe, 0, 20)
	for i := 0; i < 20; i++ {
		row := DailyRecipe{Code: string(rune(3000 + i)), Attacks: 10, TroopHousing: map[int]int{1: 200 + i}}
		if i < 10 {
			row.Spells = map[int]int{20: 2}
			row.Equipment = []HeroEquipment{{HeroID: 30, EquipmentID: 40}}
		}
		rows = append(rows, row)
	}
	got := ClassifyDaily(rows, DailyOptions{})
	if len(got.Families) != 1 || len(got.Families[0].Setups) != 1 {
		t.Fatalf("nested count thresholds should have one representative: %+v", got.Families)
	}
}

func TestClassifyDailyRequiresNovelCoverageForSameGear(t *testing.T) {
	rows := make([]DailyRecipe, 0, 20)
	for i := 0; i < 20; i++ {
		row := DailyRecipe{Code: string(rune(4000 + i)), Attacks: 10, TroopHousing: map[int]int{1: 200 + i}}
		if i < 10 {
			row.Equipment = []HeroEquipment{{HeroID: 30, EquipmentID: 40}}
			row.Spells = map[int]int{20: 1}
			if i < 8 {
				row.Spells[21] = 1
			}
		}
		rows = append(rows, row)
	}
	got := ClassifyDaily(rows, DailyOptions{})
	if len(got.Families) != 1 || len(got.Families[0].Setups) != 1 {
		t.Fatalf("correlated second spell should not become a duplicate gear setup: %+v", got.Families)
	}
}

func TestClassifyDailyFeatureKeysPersistAcrossDays(t *testing.T) {
	rows := make([]DailyRecipe, 0, 12)
	for i := 0; i < 12; i++ {
		row := DailyRecipe{Code: string(rune(5000 + i)), Attacks: 10, TroopHousing: map[int]int{1: 200 + i}}
		if i < 6 {
			row.Spells = map[int]int{20: 1}
			row.Equipment = []HeroEquipment{{HeroID: 30, EquipmentID: 40}}
		}
		rows = append(rows, row)
	}
	first := ClassifyDaily(rows, DailyOptions{})
	for i := range rows {
		rows[i].Attacks = 20
		rows[i].Code += "-next-day"
	}
	second := ClassifyDaily(rows, DailyOptions{})
	if len(first.Families) != 1 || len(second.Families) != 1 || first.Families[0].Signature != second.Families[0].Signature || len(first.Families[0].Setups) != 1 || len(second.Families[0].Setups) != 1 || first.Families[0].Setups[0].Signature != second.Families[0].Setups[0].Signature {
		t.Fatalf("stable composition feature keys changed across daily usage: first=%+v second=%+v", first.Families, second.Families)
	}
}

func TestClassifyDailyUsesAttackWeightedHousingBaseline(t *testing.T) {
	rows := []DailyRecipe{
		{Code: "complete-a", Attacks: 1000, TroopHousing: map[int]int{1: 320}},
		{Code: "complete-b", Attacks: 500, TroopHousing: map[int]int{1: 300}},
		{Code: "near-limit", Attacks: 20, TroopHousing: map[int]int{1: 288}},
		{Code: "partial", Attacks: 10, TroopHousing: map[int]int{1: 280}},
		{Code: "outlier", Attacks: 200, TroopHousing: map[int]int{1: 100}},
	}
	got := ClassifyDaily(rows, DailyOptions{})
	if baseline := dailyHousingBaseline(rows); baseline != 320 {
		t.Fatalf("baseline = %d, want 320", baseline)
	}
	if got.TotalAttacks != 1730 || got.ClassifiedAttacks != 1520 || got.ReviewAttacks != 210 {
		t.Fatalf("daily housing gate = %+v", got)
	}
}

func TestClassifyDailyKeepsHealerAndHealerlessFamiliesDistinct(t *testing.T) {
	rows := []DailyRecipe{
		{Code: "healers", Attacks: 600, TroopHousing: map[int]int{1: 260, 2: 60}},
		{Code: "healerless", Attacks: 400, TroopHousing: map[int]int{1: 320}},
	}
	got := ClassifyDaily(rows, DailyOptions{SupportTroops: map[int]bool{2: true}, IdentitySupportTroops: map[int]bool{2: true}})
	if len(got.Families) != 2 || got.CodeFamily["healers"] != "[1,2]" || got.CodeFamily["healerless"] != "[1]" {
		t.Fatalf("healer family identities = %+v", got.Families)
	}
}

func TestClassifyDailyNeverUsesHealerAsOnlyCoreWhenCombatIsMixed(t *testing.T) {
	rows := []DailyRecipe{
		{Code: "rocket-super-healer", Attacks: 600, TroopHousing: map[int]int{1: 70, 2: 70, 3: 60, 4: 120}},
		{Code: "rocket-super-no-healer", Attacks: 400, TroopHousing: map[int]int{1: 70, 2: 70, 4: 180}},
	}
	got := ClassifyDaily(rows, DailyOptions{SupportTroops: map[int]bool{3: true, 4: true}, IdentitySupportTroops: map[int]bool{3: true}})
	if got.CodeFamily["rocket-super-healer"] != "[1,2,3]" || got.CodeFamily["rocket-super-no-healer"] != "[1,2]" {
		t.Fatalf("mixed combat identity was lost: %+v", got.Families)
	}
}

func TestClassifyDailySmallFurnaceDoesNotSplitRocketSuperButDominantFurnaceCan(t *testing.T) {
	rows := []DailyRecipe{
		{Code: "no-furnace", Attacks: 100, TroopHousing: map[int]int{1: 72, 2: 60, 3: 70, 5: 118}},
		{Code: "one-furnace", Attacks: 100, TroopHousing: map[int]int{1: 72, 2: 60, 3: 70, 4: 18, 5: 100}},
		{Code: "three-furnaces", Attacks: 100, TroopHousing: map[int]int{1: 72, 2: 60, 3: 70, 4: 54, 5: 64}},
		{Code: "five-furnaces", Attacks: 100, TroopHousing: map[int]int{1: 72, 2: 60, 3: 70, 4: 90, 5: 28}},
	}
	options := DailyOptions{SupportTroops: map[int]bool{3: true, 5: true},
		IdentitySupportTroops: map[int]bool{3: true}, ConditionalCoreTroops: map[int]bool{4: true}}
	got := ClassifyDaily(rows, options)
	for _, code := range []string{"no-furnace", "one-furnace", "three-furnaces"} {
		if got.CodeFamily[code] != "[1,2,3]" {
			t.Fatalf("small Furnace split %s into %s: %+v", code, got.CodeFamily[code], got.Families)
		}
	}
	if got.CodeFamily["five-furnaces"] != "[1,2,3,4]" {
		t.Fatalf("dominant Furnace was erased: %+v", got.Families)
	}
	if got.TotalAttacks != 400 || got.ClassifiedAttacks != 400 {
		t.Fatalf("attack accounting changed: %+v", got)
	}

	// The same exact three-Furnace code must not change identity merely because
	// the day's mix of other recipes changes.
	secondDay := ClassifyDaily([]DailyRecipe{rows[2], rows[1]}, options)
	if secondDay.CodeFamily["three-furnaces"] != "[1,2,3]" {
		t.Fatalf("same three-Furnace army changed daily identity: %+v", secondDay.Families)
	}
}

func TestClassifyDailyRequiresOnePercentOfAllAttacksForVariant(t *testing.T) {
	rows := make([]DailyRecipe, 0, 14)
	for i := 0; i < 6; i++ {
		rows = append(rows, DailyRecipe{Code: string(rune(6000 + i)), Attacks: 10,
			TroopHousing: map[int]int{1: 200 + i}, Spells: map[int]int{20: 1},
			Equipment: []HeroEquipment{{HeroID: 30, EquipmentID: 40}}})
	}
	for i := 0; i < 6; i++ {
		rows = append(rows, DailyRecipe{Code: string(rune(6100 + i)), Attacks: 10,
			TroopHousing: map[int]int{1: 200 + i}, Spells: map[int]int{21: 1},
			Equipment: []HeroEquipment{{HeroID: 30, EquipmentID: 41}}})
	}
	rows = append(rows, DailyRecipe{Code: "other-family", Attacks: 10000, TroopHousing: map[int]int{2: 200}})
	got := ClassifyDaily(rows, DailyOptions{})
	if len(got.Families) != 2 || len(got.Families[1].Setups) != 0 {
		t.Fatalf("variant under one percent of all attacks persisted: %+v", got.Families)
	}
}

func TestClassifyDailyOmitsFamiliesBelowOneTenthPercent(t *testing.T) {
	rows := []DailyRecipe{
		{Code: "common", Attacks: 1000, TroopHousing: map[int]int{1: 300}},
		{Code: "rare", Attacks: 1, TroopHousing: map[int]int{2: 300}},
	}
	got := ClassifyDaily(rows, DailyOptions{})
	if len(got.Families) != 1 || got.ClassifiedAttacks != 1000 || got.ReviewAttacks != 1 || got.TotalAttacks != 1001 {
		t.Fatalf("usage gate = %+v", got)
	}
	rows[0].Attacks = 999
	got = ClassifyDaily(rows, DailyOptions{})
	if len(got.Families) != 2 || got.CodeFamily["rare"] == "" {
		t.Fatalf("family at exactly 0.1 percent should be visible: %+v", got)
	}
}
