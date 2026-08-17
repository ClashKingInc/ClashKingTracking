package scripts

import (
	"encoding/json"
	"testing"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestCapitalRaidUpdateEventUsesV2NestedObjects(t *testing.T) {
	previous := []byte(`{"state":"ongoing","capitalTotalLoot":100}`)
	current := []byte(`{"state":"ongoing","capitalTotalLoot":250}`)
	event, err := capitalRaidUpdateEvent("#CLAN", previous, current)
	if err != nil {
		t.Fatalf("build capital event: %v", err)
	}
	raw, err := json.Marshal(event.Value)
	if err != nil {
		t.Fatalf("marshal capital event: %v", err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode capital event: %v", err)
	}
	for _, key := range []string{"raid", "previous_raid"} {
		var object map[string]any
		if err := json.Unmarshal(payload[key], &object); err != nil {
			t.Fatalf("%s is not a nested object: %s (%v)", key, payload[key], err)
		}
	}
	if _, found := payload["data"]; found {
		t.Fatal("capital event retained an alternate compatibility payload")
	}
}

func TestNewRaidParticipantTagsIncludesOnlyFirstObservedPlayers(t *testing.T) {
	previous := []byte(`{"members":[{"tag":"#A"},{"tag":"#B"}]}`)
	current := raidWithMembers("#A", "#B", "#C", "#C", "")
	got := newRaidParticipantTags(previous, current)
	if len(got) != 1 || got[0] != "#C" {
		t.Fatalf("new participant tags = %#v, want [#C]", got)
	}

	got = newRaidParticipantTags(nil, current)
	if len(got) != 3 || got[0] != "#A" || got[1] != "#B" || got[2] != "#C" {
		t.Fatalf("cold participant tags = %#v, want [#A #B #C]", got)
	}
}

func raidWithMembers(tags ...string) clashy.RaidLogEntry {
	raid := clashy.RaidLogEntry{Members: make([]clashy.RaidMember, 0, len(tags))}
	for _, tag := range tags {
		raid.Members = append(raid.Members, clashy.RaidMember{Tag: tag})
	}
	return raid
}
