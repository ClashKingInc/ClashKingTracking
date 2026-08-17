package utils

import (
	"encoding/json"
	"strings"
	"testing"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestPlayerProfileDetailsKeepOnlyRequestedProgress(t *testing.T) {
	player := clashy.Player{
		Tag:      "#PLAYER",
		Name:     "Player",
		TownHall: 17,
		Troops: []clashy.Troop{{
			Name: "Barbarian", Level: 12, MaxLevel: 12, Village: "home",
		}},
		Spells: []clashy.Spell{{
			Name: "Rage Spell", Level: 6, MaxLevel: 6, Village: "home",
		}},
		Heroes: []clashy.Hero{{
			Name: "Barbarian King", Level: 95, MaxLevel: 100, Village: "home",
		}},
		HeroEquipment: []clashy.Equipment{{
			Name: "Giant Gauntlet", Level: 24, MaxLevel: 27, Village: "home", Rarity: "epic",
		}},
		Achievements: []clashy.Achievement{{
			Name: "Gold Grab", Stars: 3, Value: 2_500_000_000, Target: 100_000_000,
			Village: "home", Info: "large repeated description", CompletionInfo: "another repeated description",
		}},
	}

	ingest := PlayerProfileFromClashy(player)
	if len(ingest.Heroes) != 1 || len(ingest.Equipment) != 1 || len(ingest.Achievements) != 1 {
		t.Fatalf("unexpected detail counts: heroes=%d equipment=%d achievements=%d",
			len(ingest.Heroes), len(ingest.Equipment), len(ingest.Achievements))
	}

	heroes, equipment, achievements, err := marshalPlayerDetails(ingest)
	if err != nil {
		t.Fatal(err)
	}
	combined := string(heroes) + string(equipment) + string(achievements)
	for _, excluded := range []string{"Barbarian\"", "Rage Spell", "large repeated description", "another repeated description"} {
		if strings.Contains(combined, excluded) {
			t.Fatalf("stored JSON contains excluded profile data %q: %s", excluded, combined)
		}
	}
	for _, required := range []string{"Barbarian King", "Giant Gauntlet", "Gold Grab", `"max_level"`, `"target"`} {
		if !strings.Contains(combined, required) {
			t.Fatalf("stored JSON is missing %q: %s", required, combined)
		}
	}
}

func TestMarshalPlayerDetailsUsesJSONArraysForMissingSections(t *testing.T) {
	heroes, equipment, achievements, err := marshalPlayerDetails(PlayerProfileFromClashy(clashy.Player{
		Tag: "#PLAYER", Name: "Player", TownHall: 1,
	}))
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{
		"heroes": heroes, "equipment": equipment, "achievements": achievements,
	} {
		var value any
		if err := json.Unmarshal(payload, &value); err != nil {
			t.Fatalf("%s is invalid JSON: %v", name, err)
		}
		items, ok := value.([]any)
		if !ok || len(items) != 0 {
			t.Fatalf("%s = %s, want empty JSON array", name, payload)
		}
	}
}
