package scripts

import (
	"encoding/json"
	"testing"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestBotClanSnapshotComparedDoesNotAdvanceSnapshot(t *testing.T) {
	store := &memoryBotClanSnapshotStore{values: make(map[string][]byte)}
	clan := clashy.Clan{Tag: "#CLAN", Name: "Before"}
	prefix := "botclans:test:"
	if err := store.StoreRaw(t.Context(), botClanSnapshotKey(prefix, "raid", clan.Tag), jsonBytes(clan)); err != nil {
		t.Fatal(err)
	}

	clan.Name = "After"
	previous, _, hasPrevious, changed, err := botClanSnapshotCompared(t.Context(), store, prefix, "raid", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || !changed || previous == nil || previous.Name != "Before" {
		t.Fatalf("comparison = previous %#v hasPrevious %v changed %v", previous, hasPrevious, changed)
	}
	stored, ok, err := store.LoadRaw(t.Context(), botClanSnapshotKey(prefix, "raid", clan.Tag))
	if err != nil || !ok {
		t.Fatalf("load stored snapshot: ok=%v err=%v", ok, err)
	}
	var persisted clashy.Clan
	if err := json.Unmarshal(stored, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Name != "Before" {
		t.Fatalf("comparison advanced snapshot to %q", persisted.Name)
	}
}
