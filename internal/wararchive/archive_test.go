package wararchive

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
)

func TestMarshalOnlyOmitsEmptyWarTag(t *testing.T) {
	payload, err := Marshal(War{
		BattleModifier: BattleModifierNone,
		StartTime:      time.Unix(1, 0).UTC(),
		Clan:           Clan{Members: []Member{{Attacks: []Attack{}}}},
		Opponent:       Clan{Members: []Member{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if _, exists := decoded["warTag"]; exists {
		t.Fatal("empty warTag must be omitted")
	}
	if _, exists := decoded["type"]; exists {
		t.Fatal("war type belongs in SQL metadata and must not be duplicated in archive JSON")
	}
	requireJSONKeys(t, decoded, "startTime", "battleModifier")
	clan := decoded["clan"].(map[string]any)
	requireJSONKeys(t, clan, "name", "badgeToken", "clanLevel", "attacks", "stars", "destructionPercentage")
	member := clan["members"].([]any)[0].(map[string]any)
	requireJSONKeys(t, member, "name", "townhallLevel", "mapPosition", "attacks")
}

func TestMarshalRejectsMissingStartTime(t *testing.T) {
	if _, err := Marshal(War{}); err == nil {
		t.Fatal("war without startTime was marshaled")
	}
}

func requireJSONKeys(t *testing.T, value map[string]any, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if _, exists := value[key]; !exists {
			t.Errorf("JSON object omitted %q: %#v", key, value)
		}
	}
}

func TestPackBuilderWritesIndependentlyDecodableFrames(t *testing.T) {
	builder, err := NewPackBuilder(42)
	if err != nil {
		t.Fatal(err)
	}
	defer builder.Close()
	war := War{
		State: "warended", TeamSize: 5, AttacksPerMember: 2,
		PreparationStartTime: time.Unix(1, 0).UTC(), StartTime: time.Unix(2, 0).UTC(), EndTime: time.Unix(3, 0).UTC(),
		Clan: Clan{Tag: "#A", Members: []Member{}}, Opponent: Clan{Tag: "#B", Members: []Member{}},
	}
	locator, err := builder.Add(42, war)
	if err != nil {
		t.Fatal(err)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1), zstd.WithDecoderDicts(Dictionary))
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	frame := builder.Bytes()[locator.Offset : locator.Offset+int64(locator.CompressedBytes)]
	raw, err := decoder.DecodeAll(frame, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Unmarshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Clan.Tag != "#A" || decoded.Opponent.Tag != "#B" || ObjectKey(42) != "packs/000042.pack" {
		t.Fatalf("unexpected decoded war or key: %#v %q", decoded, ObjectKey(42))
	}
}

func TestNormalizeBattleModifier(t *testing.T) {
	tests := map[string]string{
		"":            BattleModifierNone,
		"null":        BattleModifierNone,
		"NONE":        BattleModifierNone,
		"hardMode":    BattleModifierHardMode,
		"hard_mode":   BattleModifierHardMode,
		"minus-one":   BattleModifierMinusOne,
		"minusTwo":    BattleModifierMinusTwo,
		"minus three": BattleModifierMinusThree,
		"unexpected":  BattleModifierNone,
	}
	for input, want := range tests {
		if got := NormalizeBattleModifier(input); got != want {
			t.Errorf("NormalizeBattleModifier(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestPackStatsIncludeFinalDailyDimensions(t *testing.T) {
	stats := NewPackStats()
	stats.Add("random", War{
		TeamSize: 5, AttacksPerMember: 2, EndTime: time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC),
		Clan:     Clan{Tag: "#A", Stars: 3, DestructionPercentage: 100, Members: []Member{{Tag: "#P", TownhallLevel: 18, Attacks: []Attack{{DefenderTag: "#D", Stars: 3, DestructionPercentage: 100, Duration: 90}}}}},
		Opponent: Clan{Tag: "#B", Stars: 2, DestructionPercentage: 80, Members: []Member{{Tag: "#D", TownhallLevel: 17, Attacks: []Attack{{DefenderTag: "#P", Stars: 2, DestructionPercentage: 80, Duration: 120}}}}},
	})
	day := stats.ByDay["2026-08-23"]
	if day.WarsByType["random"] != 1 || day.WarsBySize["5"] != 1 || day.TotalAttacks != 2 || day.TotalMissedAttacks != 18 {
		t.Fatalf("missing daily totals: %#v", day)
	}
	if day.RegularHitRates["18:17"].ThreeStars.Attacks != 1 || day.RegularHitRates["17:18"].TwoStars.DestructionPercent != 80 {
		t.Fatalf("missing hit-rate outcomes: %#v", day.RegularHitRates)
	}
	bySize := day.RegularByWarSize["5"]
	if bySize.Wars != 1 || bySize.TotalStars != 5 || bySize.Wins != 1 || bySize.Losses != 1 || bySize.Townhalls["18"] != 1 || bySize.Townhalls["17"] != 1 {
		t.Fatalf("missing regular-war size stats: %#v", bySize)
	}
}
