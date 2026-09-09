package armyfamily

import (
	"bytes"
	"sort"
	"time"
)

const (
	MatchingVersion             = "army-family-v1"
	MinimumTroopSimilarity      = 0.86
	MinimumSpellSimilarity      = 0.80
	MinimumEquipmentSimilarity  = 0.75
	MaximumEquipmentDifferences = 2
)

type Hash [32]byte

type Composition struct {
	TroopHousing  map[int]int
	SpellCapacity map[int]int
	Heroes        []int
	Equipment     []int
}

type Anchor struct {
	Hash        Hash
	Composition Composition
	CreatedAt   time.Time
}

type Match struct {
	AnchorHash               Hash
	TroopHousingSimilarity   float64
	SpellCapacitySimilarity  float64
	HeroesExact              bool
	EquipmentSimilarity      float64
	EquipmentDifferenceCount int
	MatchingVersion          string
}

func FindDirectAnchor(candidate Composition, anchors []Anchor) (Match, bool) {
	matches := make([]Match, 0, len(anchors))
	created := make(map[Hash]time.Time, len(anchors))
	for _, anchor := range anchors {
		match := compare(candidate, anchor)
		if match.TroopHousingSimilarity < MinimumTroopSimilarity ||
			match.SpellCapacitySimilarity < MinimumSpellSimilarity ||
			!match.HeroesExact ||
			match.EquipmentSimilarity < MinimumEquipmentSimilarity ||
			match.EquipmentDifferenceCount > MaximumEquipmentDifferences {
			continue
		}
		matches = append(matches, match)
		created[anchor.Hash] = anchor.CreatedAt
	}
	if len(matches) == 0 {
		return Match{}, false
	}
	sort.Slice(matches, func(i, j int) bool {
		left, right := aggregateSimilarity(matches[i]), aggregateSimilarity(matches[j])
		if left != right {
			return left > right
		}
		if !created[matches[i].AnchorHash].Equal(created[matches[j].AnchorHash]) {
			return created[matches[i].AnchorHash].Before(created[matches[j].AnchorHash])
		}
		return bytes.Compare(matches[i].AnchorHash[:], matches[j].AnchorHash[:]) < 0
	})
	return matches[0], true
}

func compare(candidate Composition, anchor Anchor) Match {
	return Match{
		AnchorHash:               anchor.Hash,
		TroopHousingSimilarity:   weightedSimilarity(candidate.TroopHousing, anchor.Composition.TroopHousing),
		SpellCapacitySimilarity:  weightedSimilarity(candidate.SpellCapacity, anchor.Composition.SpellCapacity),
		HeroesExact:              sameSet(candidate.Heroes, anchor.Composition.Heroes),
		EquipmentSimilarity:      setSimilarity(candidate.Equipment, anchor.Composition.Equipment),
		EquipmentDifferenceCount: symmetricDifferenceCount(candidate.Equipment, anchor.Composition.Equipment),
		MatchingVersion:          MatchingVersion,
	}
}

func aggregateSimilarity(match Match) float64 {
	return match.TroopHousingSimilarity + match.SpellCapacitySimilarity + match.EquipmentSimilarity
}

func weightedSimilarity(left, right map[int]int) float64 {
	leftTotal, rightTotal, overlap := 0, 0, 0
	for id, weight := range left {
		if weight < 0 {
			weight = 0
		}
		leftTotal += weight
		other := right[id]
		if other < weight {
			overlap += max(other, 0)
		} else {
			overlap += weight
		}
	}
	for _, weight := range right {
		rightTotal += max(weight, 0)
	}
	denominator := max(leftTotal, rightTotal)
	if denominator == 0 {
		if leftTotal == rightTotal {
			return 1
		}
		return 0
	}
	return float64(overlap) / float64(denominator)
}

func setSimilarity(left, right []int) float64 {
	l, r := intSet(left), intSet(right)
	intersection := 0
	for value := range l {
		if _, ok := r[value]; ok {
			intersection++
		}
	}
	denominator := max(len(l), len(r))
	if denominator == 0 {
		return 1
	}
	return float64(intersection) / float64(denominator)
}

func symmetricDifferenceCount(left, right []int) int {
	l, r := intSet(left), intSet(right)
	differences := 0
	for value := range l {
		if _, ok := r[value]; !ok {
			differences++
		}
	}
	for value := range r {
		if _, ok := l[value]; !ok {
			differences++
		}
	}
	return differences
}

func sameSet(left, right []int) bool { return symmetricDifferenceCount(left, right) == 0 }

func intSet(values []int) map[int]struct{} {
	out := make(map[int]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}
