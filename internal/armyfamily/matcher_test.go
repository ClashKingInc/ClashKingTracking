package armyfamily

import (
	"testing"
)

func TestFindDirectAnchorThresholdsAndLowestFamilyIDTie(t *testing.T) {
	composition := Composition{
		TroopHousing:  map[int]int{1: 86, 2: 14},
		SpellCapacity: map[int]int{10: 8, 11: 2},
		Heroes:        []int{20, 21}, Equipment: []int{30, 31, 32, 33},
	}
	anchors := []Anchor{
		{FamilyID: 2, Composition: Composition{
			TroopHousing: map[int]int{1: 100}, SpellCapacity: map[int]int{10: 10},
			Heroes: []int{21, 20}, Equipment: []int{30, 31, 32},
		}},
		{FamilyID: 1, Composition: Composition{
			TroopHousing: map[int]int{1: 100}, SpellCapacity: map[int]int{10: 10},
			Heroes: []int{20, 21}, Equipment: []int{30, 31, 32},
		}},
	}
	match, ok := FindDirectAnchor(composition, anchors)
	if !ok {
		t.Fatal("expected inclusive threshold match")
	}
	if match.FamilyID != anchors[1].FamilyID {
		t.Fatalf("family = %d, want lowest id %d", match.FamilyID, anchors[1].FamilyID)
	}
	if match.TroopHousingSimilarity != .86 || match.SpellCapacitySimilarity != .8 || match.EquipmentSimilarity != .75 {
		t.Fatalf("unexpected match: %+v", match)
	}
}

func TestFindDirectAnchorRejectsEachBoundaryFailure(t *testing.T) {
	base := Composition{TroopHousing: map[int]int{1: 100}, SpellCapacity: map[int]int{2: 10}, Heroes: []int{3}, Equipment: []int{4, 5, 6, 7}}
	tests := []Composition{
		{TroopHousing: map[int]int{1: 85, 9: 15}, SpellCapacity: map[int]int{2: 10}, Heroes: []int{3}, Equipment: []int{4, 5, 6, 7}},
		{TroopHousing: map[int]int{1: 100}, SpellCapacity: map[int]int{2: 7, 9: 3}, Heroes: []int{3}, Equipment: []int{4, 5, 6, 7}},
		{TroopHousing: map[int]int{1: 100}, SpellCapacity: map[int]int{2: 10}, Heroes: []int{8}, Equipment: []int{4, 5, 6, 7}},
		{TroopHousing: map[int]int{1: 100}, SpellCapacity: map[int]int{2: 10}, Heroes: []int{3}, Equipment: []int{4, 8, 9, 10}},
	}
	for i, candidate := range tests {
		if _, ok := FindDirectAnchor(candidate, []Anchor{{FamilyID: 1, Composition: base}}); ok {
			t.Fatalf("case %d matched", i)
		}
	}
}

func TestFindDirectAnchorDoesNotChainThroughMembers(t *testing.T) {
	anchor := Anchor{FamilyID: 1, Composition: Composition{TroopHousing: map[int]int{1: 100}, SpellCapacity: map[int]int{}, Heroes: []int{}, Equipment: []int{}}}
	memberNearAnchor := Composition{TroopHousing: map[int]int{1: 86, 2: 14}, SpellCapacity: map[int]int{}, Heroes: []int{}, Equipment: []int{}}
	candidateNearMember := Composition{TroopHousing: map[int]int{1: 72, 2: 28}, SpellCapacity: map[int]int{}, Heroes: []int{}, Equipment: []int{}}
	if _, ok := FindDirectAnchor(memberNearAnchor, []Anchor{anchor}); !ok {
		t.Fatal("expected first member to match anchor")
	}
	if _, ok := FindDirectAnchor(candidateNearMember, []Anchor{anchor}); ok {
		t.Fatal("candidate must be compared directly with immutable anchor")
	}
}

func TestFindDirectAnchorUsesEquipmentOverlapWithoutDifferenceGate(t *testing.T) {
	anchor := Anchor{FamilyID: 7, Composition: Composition{
		TroopHousing: map[int]int{1: 100}, SpellCapacity: map[int]int{2: 10}, Heroes: []int{3},
		Equipment: []int{10, 11, 12, 13, 14, 15, 16, 17},
	}}
	candidate := Composition{
		TroopHousing: map[int]int{1: 100}, SpellCapacity: map[int]int{2: 10}, Heroes: []int{3},
		Equipment: []int{10, 11, 12, 13, 14, 15, 18, 19},
	}
	match, ok := FindDirectAnchor(candidate, []Anchor{anchor})
	if !ok || match.EquipmentSimilarity != .75 {
		t.Fatalf("six-of-eight equipment overlap should match: %+v, %v", match, ok)
	}
}
