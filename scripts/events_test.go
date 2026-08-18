//go:build script_internal_tests

package scripts

import (
	"encoding/json"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/valkey-io/valkey-go"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestStreamEntryEventParsesPayload(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"type": "reddit"})
	now := time.Now().UTC().Truncate(time.Second)
	event, ok := streamEntryEvent(valkey.XRangeEntry{
		ID: "1-0",
		FieldValues: map[string]string{
			"topic":     "reddit",
			"clan_tag":  "#CLAN",
			"timestamp": now.Format(time.RFC3339Nano),
			"value":     string(raw),
		},
	})

	if !ok {
		t.Fatalf("event did not parse")
	}
	if event.Topic != "reddit" || event.ClanTag != "#CLAN" || !event.Timestamp.Equal(now) {
		t.Fatalf("event = %+v", event)
	}
}

func TestEventMatchesFiltersTopicAndClan(t *testing.T) {
	event := platform.Event{Topic: "reddit", ClanTag: "#CLAN"}
	if !eventMatches(platform.Filter{}, event) {
		t.Fatal("empty filter should match")
	}
	if !eventMatches(platform.Filter{
		Topics: map[string]struct{}{"reddit": {}},
		Clans:  map[string]struct{}{"#CLAN": {}},
	}, event) {
		t.Fatal("matching topic and clan should pass")
	}
	if eventMatches(platform.Filter{Topics: map[string]struct{}{"war": {}}}, event) {
		t.Fatal("non-matching topic should fail")
	}
	if eventMatches(platform.Filter{Clans: map[string]struct{}{"#OTHER": {}}}, event) {
		t.Fatal("non-matching clan should fail")
	}
}

func TestToSetReadsStringLists(t *testing.T) {
	value, err := structpb.NewStruct(map[string]any{
		"topics": []any{"reddit", "", "war"},
	})
	if err != nil {
		t.Fatal(err)
	}
	got := toSet(value, "topics")
	if _, ok := got["reddit"]; !ok {
		t.Fatalf("missing reddit in set: %#v", got)
	}
	if _, ok := got["war"]; !ok {
		t.Fatalf("missing war in set: %#v", got)
	}
	if _, ok := got[""]; ok {
		t.Fatalf("empty string should not be included: %#v", got)
	}
}
