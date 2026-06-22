//go:build script_internal_tests

package scripts

import (
	"reflect"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"

	clashy "github.com/clashkinginc/clashy.go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBotClanSnapshotChangedBaselinesThenDetectsChange(t *testing.T) {
	store := &memoryBotClanSnapshotStore{values: make(map[string][]byte)}
	clan := clashy.Clan{Tag: "#CLAN", Name: "Before"}
	prefix := "botclans:test:"

	_, _, hasPrevious, changed, err := botClanSnapshotChanged(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hasPrevious || !changed {
		t.Fatalf("first snapshot = hasPrevious %v changed %v, want false/true", hasPrevious, changed)
	}
	_, _, hasPrevious, changed, err = botClanSnapshotChanged(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || changed {
		t.Fatalf("same snapshot = hasPrevious %v changed %v, want true/false", hasPrevious, changed)
	}
	clan.Name = "After"
	previous, _, hasPrevious, changed, err := botClanSnapshotChanged(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || !changed || previous == nil || previous.Name != "Before" {
		t.Fatalf("changed snapshot = previous %#v hasPrevious %v changed %v", previous, hasPrevious, changed)
	}
}

func TestBotCWLWindowSkipsEndedAndNoSpinState(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	active := botCWLState{Season: utils.CurrentSeason(now), GroupState: "inWar"}
	if !shouldPollCWL(now, active) {
		t.Fatal("active current-season cwl should keep polling after discovery window")
	}
	ended := active
	ended.Ended = true
	if shouldPollCWL(now, ended) {
		t.Fatal("ended current-season cwl should not keep polling")
	}
	noSpin := active
	noSpin.NoSpin = true
	if shouldPollCWL(now, noSpin) {
		t.Fatal("current-season no-spin cwl should not keep polling")
	}
}

func TestLatestCWLWarTagsUsesNewestNonEmptyRound(t *testing.T) {
	group := &clashy.ClanWarLeagueGroup{Rounds: []struct {
		WarTags []string `json:"warTags,omitempty"`
	}{
		{WarTags: []string{"#OLD"}},
		{WarTags: []string{"#0", ""}},
		{WarTags: []string{"#NEW1", "#NEW2"}},
	}}
	tags, hash := latestCWLWarTags(group)
	if want := []string{"#NEW1", "#NEW2"}; !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags = %#v, want %#v", tags, want)
	}
	if hash != "#NEW1,#NEW2" {
		t.Fatalf("hash = %q", hash)
	}
}

func TestCWLLineupChanges(t *testing.T) {
	previous := clashy.ClanWar{
		Clan:     &clashy.WarClan{Tag: "#A", Members: []clashy.ClanWarMember{{Tag: "#P1"}, {Tag: "#P2"}}},
		Opponent: &clashy.WarClan{Tag: "#B", Members: []clashy.ClanWarMember{{Tag: "#O1"}}},
	}
	current := clashy.ClanWar{
		Clan:     &clashy.WarClan{Tag: "#A", Members: []clashy.ClanWarMember{{Tag: "#P2"}, {Tag: "#P3"}}},
		Opponent: &clashy.WarClan{Tag: "#B", Members: []clashy.ClanWarMember{{Tag: "#O1"}, {Tag: "#O2"}}},
	}
	changes := cwlLineupChanges(previous, current)
	if !cwlLineupChanged(changes) {
		t.Fatal("lineup change was not detected")
	}
	if added := changes["added"].([]clashy.ClanWarMember); len(added) != 1 || added[0].Tag != "#P3" {
		t.Fatalf("added = %#v", added)
	}
	if removed := changes["removed"].([]clashy.ClanWarMember); len(removed) != 1 || removed[0].Tag != "#P1" {
		t.Fatalf("removed = %#v", removed)
	}
}

func TestRaidMissingMembersUsesClanSnapshot(t *testing.T) {
	clan := clashy.Clan{Members: []clashy.ClanMember{
		{Tag: "#A", Name: "A", TownHall: 16, Role: clashy.RoleMember},
		{Tag: "#B", Name: "B", TownHall: 12, Role: clashy.RoleMember},
		{Tag: "#C", Name: "C", TownHall: 16, Role: clashy.RoleLeader},
	}}
	raid := clashy.RaidLogEntry{Members: []clashy.RaidMember{
		{Tag: "#A", AttackCount: 3, AttackLimit: 5},
		{Tag: "#B", AttackCount: 0, AttackLimit: 5},
	}}
	missing := raidMissingMembers(clan, raid, bson.M{
		"attack_threshold": 1,
		"roles":            bson.A{"member"},
		"townhalls":        bson.A{16},
	})
	if len(missing) != 1 || missing[0]["tag"] != "#A" {
		t.Fatalf("missing = %#v, want only #A", missing)
	}
}

func TestParseReminderHours(t *testing.T) {
	got, err := parseReminderHours("1.5hr")
	if err != nil {
		t.Fatal(err)
	}
	if got != 1.5 {
		t.Fatalf("parseReminderHours = %v, want 1.5", got)
	}
	if _, err := parseReminderHours("soon"); err == nil {
		t.Fatal("expected invalid reminder time error")
	}
}

func botClansTestApp() *platform.App {
	return &platform.App{
		Config: platform.Config{
			BotClanRequestsPerSecond: 950,
			BotClanSnapshotPrefix:    "botclans:test:",
			BotClanCWLStateSnapshot:  "cwlstate",
		},
		Stats: platform.NewTracker(),
	}
}
