package scripts

import (
	"testing"
	"time"

	"github.com/clashkinginc/clashy.go"
)

func TestArmyCompositionUsesExactNumericAssignmentsAndClashySections(t *testing.T) {
	static, err := clashy.LoadStaticData()
	if err != nil {
		t.Fatal(err)
	}
	shareCode := normalizeArmyShareCode("h0p4e8_14-1p9e39i3x53d1x7u10x0-2x1s4x3")
	record := armyCompositionFromColumns(static, shareCode, parseArmyColumns(shareCode))
	if len(record.MainTroops) != 2 || record.MainTroops[0] != (armyQuantity{ID: clashy.TroopBaseID, Quantity: 10}) {
		t.Fatalf("main troops = %+v", record.MainTroops)
	}
	if len(record.ClanCastleTroops) != 1 || record.ClanCastleTroops[0].ID != clashy.TroopBaseID+53 {
		t.Fatalf("clan castle troops = %+v", record.ClanCastleTroops)
	}
	if len(record.Spells) != 2 || record.Spells[0].ClanCastle || !record.Spells[1].ClanCastle {
		t.Fatalf("spells are not sorted by id and clan-castle flag: %+v", record.Spells)
	}
	if len(record.Heroes) != 2 || record.Heroes[0] != int32(clashy.HeroBaseID) {
		t.Fatalf("heroes = %+v", record.Heroes)
	}
	if len(record.Equipment) != 3 || len(record.PetAssignments) != 2 || record.PetAssignments[0].HeroID != clashy.HeroBaseID {
		t.Fatalf("equipment=%+v pets=%+v", record.Equipment, record.PetAssignments)
	}
}

func TestArmyCompositionSeparatesSiegeMachine(t *testing.T) {
	static, err := clashy.LoadStaticData()
	if err != nil {
		t.Fatal(err)
	}
	shareCode := normalizeArmyShareCode("u10x0-1x51")
	record := armyCompositionFromColumns(static, shareCode, parseArmyColumns(shareCode))
	if record.SiegeMachineID == nil || *record.SiegeMachineID != int32(clashy.TroopBaseID+51) || len(record.MainTroops) != 1 {
		t.Fatalf("composition = %+v", record)
	}
}

func TestLeagueCloseoutSchedule(t *testing.T) {
	before := time.Date(2026, 9, 7, 5, 11, 59, 0, time.UTC)
	after := before.Add(time.Second)
	if got := latestEligibleLegendDay(before); !got.Equal(time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("before day = %s", got)
	}
	if got := latestEligibleLegendDay(after); !got.Equal(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("after day = %s", got)
	}
	if !rankedCloseoutDue(before) || !rankedCloseoutDue(after) {
		t.Fatal("Monday ranked closeout should remain due after 05:10 UTC")
	}
	if got := nextLeagueCloseout(after); !got.Equal(time.Date(2026, 9, 8, 5, 12, 0, 0, time.UTC)) {
		t.Fatalf("next = %s", got)
	}
}

func TestLegendDayWindowUsesShiftedBoundary(t *testing.T) {
	day := time.Date(2026, 9, 7, 18, 0, 0, 0, time.UTC)
	start, end := legendDayWindow(day)
	if want := time.Date(2026, 9, 7, 5, 10, 0, 0, time.UTC); !start.Equal(want) {
		t.Fatalf("start = %s, want %s", start, want)
	}
	if !end.Equal(start.Add(24 * time.Hour)) {
		t.Fatalf("end = %s", end)
	}
}

func TestAggregateLegendKeepsMissingCodesOnlyInGlobalTotals(t *testing.T) {
	duration := int32(120)
	attacks := []legendAttack{
		{player: "#A", code: "known", stars: 3, destruction: 100, duration: &duration},
		{player: "#B", code: "", stars: 2, destruction: 85},
	}
	decoded := map[string]decodedArmy{
		"known": {record: armyCompositionRecord{Heroes: []int32{100}, Equipment: []armyHeroEquipment{{EquipmentID: 200}}}},
	}
	global, families, heroes, _, equipment, _ := aggregateLegend(attacks, map[string]int64{"known": 7}, decoded)
	if global.attacks != 2 || global.three != 1 || global.two != 1 || len(global.players) != 2 {
		t.Fatalf("global = %+v", global)
	}
	if families[7].attacks != 1 || heroes[100].Uses != 1 || equipment[200].Uses != 1 {
		t.Fatalf("families=%+v heroes=%+v equipment=%+v", families, heroes, equipment)
	}
}

func TestLegendCloseoutUsesOnlyAgreedCohorts(t *testing.T) {
	want := [...]string{"legend_i", "top_1000", "top_200"}
	if legendCloseoutCohorts != want {
		t.Fatalf("cohorts = %#v, want %#v", legendCloseoutCohorts, want)
	}
}
