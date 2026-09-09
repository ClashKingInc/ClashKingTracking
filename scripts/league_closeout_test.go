package scripts

import (
	"fmt"
	"testing"
	"time"

	"github.com/clashkinginc/clashy.go"
)

func TestArmyHashV2ContractVectors(t *testing.T) {
	vectors := map[string]string{
		"h0p4e8_14-1p9e39i3x53d1x70u10x0-2x1s4x35": "381e0786e690608139a21c2a17b131cc4b265ce18353509ded94346e2bc0ff10",
		"h0p4e8_14-1p9e39":                         "6628d4ed049e5a5605474ceb2edac33b20978c3499274e8c57207583f52163af",
		"h0p9e14_8-1p4e39":                         "cbb0595e3e6bdcf7d00f7b6641532701e7109f5e0c9e35e715badb8983a5f1be",
	}
	for shareCode, want := range vectors {
		if got := fmt.Sprintf("%x", canonicalArmyHash(normalizeArmyShareCode(shareCode))); got != want {
			t.Fatalf("hash(%q) = %s, want %s", shareCode, got, want)
		}
	}
}

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
	before := time.Date(2026, 9, 7, 5, 9, 59, 0, time.UTC)
	after := before.Add(time.Second)
	if got := latestEligibleLegendDay(before); !got.Equal(time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("before day = %s", got)
	}
	if got := latestEligibleLegendDay(after); !got.Equal(time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("after day = %s", got)
	}
	if rankedCloseoutDue(before) || !rankedCloseoutDue(after) {
		t.Fatal("Monday ranked closeout boundary is not 05:10 UTC")
	}
	if got := nextLeagueCloseout(after); !got.Equal(time.Date(2026, 9, 8, 5, 10, 0, 0, time.UTC)) {
		t.Fatalf("next = %s", got)
	}
}
