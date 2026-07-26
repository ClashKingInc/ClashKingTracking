//go:build script_internal_tests

package scripts

import (
	"bytes"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestBotClanSnapshotDiffDoesNotAdvanceUntilStored(t *testing.T) {
	store := &memoryBotClanSnapshotStore{values: make(map[string][]byte)}
	clan := clashy.Clan{Tag: "#CLAN", Name: "Before"}
	prefix := "botclans:test:"

	_, raw, hasPrevious, changed, err := loadBotClanSnapshotChange(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hasPrevious || !changed {
		t.Fatalf("first snapshot = hasPrevious %v changed %v, want false/true", hasPrevious, changed)
	}
	if err := store.StoreRaw(t.Context(), botClanSnapshotKey(prefix, "clan", clan.Tag), raw); err != nil {
		t.Fatal(err)
	}
	_, _, hasPrevious, changed, err = loadBotClanSnapshotChange(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || changed {
		t.Fatalf("same snapshot = hasPrevious %v changed %v, want true/false", hasPrevious, changed)
	}
	clan.Name = "After"
	previous, _, hasPrevious, changed, err := loadBotClanSnapshotChange(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || !changed || previous == nil || previous.Name != "Before" {
		t.Fatalf("changed snapshot = previous %#v hasPrevious %v changed %v", previous, hasPrevious, changed)
	}
	stored, _, ok, err := loadBotClanSnapshot[clashy.Clan](t.Context(), store, prefix, "clan", clan.Tag)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || stored == nil || stored.Name != "Before" {
		t.Fatalf("snapshot advanced before store: %#v", stored)
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
	missing := raidMissingMembers(clan, raid, raidReminder{
		AttackThreshold: 1,
		Roles:           []string{"member"},
		TownHalls:       []int{16},
	})
	if len(missing) != 1 || missing[0]["tag"] != "#A" {
		t.Fatalf("missing = %#v, want only #A", missing)
	}
}

func TestCapitalRaidCacheKeys(t *testing.T) {
	const prefix = "botclans:snapshot:"
	if got, want := capitalRaidPayloadKey(prefix, "#CLAN"), "botclans:snapshot:raid:#CLAN"; got != want {
		t.Fatalf("payload key = %q, want %q", got, want)
	}
	if got, want := capitalRaidParticipantSetKey(prefix, "#CLAN"), "botclans:snapshot:raid-members:#CLAN"; got != want {
		t.Fatalf("participant set key = %q, want %q", got, want)
	}
	if got, want := capitalRaidMemberKey(prefix, "#PLAYER"), "botclans:snapshot:raid-member:#PLAYER"; got != want {
		t.Fatalf("member key = %q, want %q", got, want)
	}
}

func TestCapitalRaidCacheExpiryIsWeekendEndPlusTenMinutes(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	end := now.Add(36 * time.Hour)
	raid := clashy.RaidLogEntry{EndTime: &clashy.Timestamp{Time: end}}
	got, ok := capitalRaidCacheExpiry(raid, now)
	if !ok {
		t.Fatal("valid future raid end did not produce an expiry")
	}
	if want := end.Add(10 * time.Minute); !got.Equal(want) {
		t.Fatalf("expiry = %v, want %v", got, want)
	}

	for name, invalid := range map[string]clashy.RaidLogEntry{
		"missing": {},
		"zero":    {EndTime: &clashy.Timestamp{}},
		"elapsed": {EndTime: &clashy.Timestamp{Time: now.Add(-10 * time.Minute)}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, ok := capitalRaidCacheExpiry(invalid, now); ok {
				t.Fatal("invalid raid end produced cache state")
			}
		})
	}
}

func TestCapitalRaidCacheSnappyRoundTripAndPreviousResponse(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	cache := newMemoryCapitalRaidCache("botclans:snapshot:", func() time.Time { return now })
	previous := clashy.RaidLogEntry{
		State:   "ongoing",
		EndTime: &clashy.Timestamp{Time: now.Add(time.Hour)},
		Members: []clashy.RaidMember{{Tag: "#A", AttackCount: 1}},
	}
	previousRaw := jsonBytes(previous)
	if err := cache.Replace(t.Context(), "#CLAN", capitalRaidParticipantTags(previous), previousRaw, now.Add(time.Hour+10*time.Minute)); err != nil {
		t.Fatal(err)
	}
	entry := cache.entries["#CLAN"]
	if bytes.Equal(entry.compressed, previousRaw) {
		t.Fatal("cached payload was not Snappy encoded")
	}
	roundTrip, ok, err := cache.LoadRaw(t.Context(), "#CLAN")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !bytes.Equal(roundTrip, previousRaw) {
		t.Fatalf("round trip = %q, %v; want %q, true", roundTrip, ok, previousRaw)
	}

	current := previous
	current.Members[0].AttackCount = 2
	decodedPrevious, currentRaw, hasPrevious, changed, err := loadCapitalRaidCacheChange(
		t.Context(),
		cache,
		"#CLAN",
		current,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || !changed || decodedPrevious == nil || decodedPrevious.Members[0].AttackCount != 1 {
		t.Fatalf("previous response = %#v, hasPrevious %v, changed %v", decodedPrevious, hasPrevious, changed)
	}
	stillPrevious, _, err := cache.LoadRaw(t.Context(), "#CLAN")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stillPrevious, previousRaw) || bytes.Equal(stillPrevious, currentRaw) {
		t.Fatal("change detection advanced the cached response before required effects succeeded")
	}
}

func TestCapitalRaidCacheAlignsExpiryAndCleansReplacementMappings(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	cache := newMemoryCapitalRaidCache("botclans:snapshot:", func() time.Time { return now })
	expiresAt := now.Add(2*time.Hour + 10*time.Minute)
	if err := cache.Replace(t.Context(), "#CLAN", []string{"#A", "#B"}, []byte(`{"state":"ongoing"}`), expiresAt); err != nil {
		t.Fatal(err)
	}
	entry := cache.entries["#CLAN"]
	for _, playerTag := range []string{"#A", "#B"} {
		mapping, ok := cache.mappings[playerTag]
		if !ok || mapping.clanTag != "#CLAN" {
			t.Fatalf("mapping %s = %#v, %v", playerTag, mapping, ok)
		}
		if !mapping.expiresAt.Equal(entry.expiresAt) || !mapping.expiresAt.Equal(expiresAt) {
			t.Fatalf("mapping %s expiry = %v, payload expiry = %v, want %v", playerTag, mapping.expiresAt, entry.expiresAt, expiresAt)
		}
	}

	if err := cache.Replace(t.Context(), "#CLAN", []string{"#B", "#C", "#C", ""}, []byte(`{"state":"ongoing","totalAttacks":1}`), expiresAt); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.mappings["#A"]; ok {
		t.Fatal("removed participant mapping #A survived cache replacement")
	}
	if _, ok := cache.mappings["#B"]; !ok {
		t.Fatal("retained participant mapping #B was removed")
	}
	if _, ok := cache.mappings["#C"]; !ok {
		t.Fatal("new participant mapping #C was not stored")
	}

	if err := cache.Replace(t.Context(), "#OTHER", []string{"#C"}, []byte(`{"state":"ongoing"}`), expiresAt); err != nil {
		t.Fatal(err)
	}
	if err := cache.Replace(t.Context(), "#CLAN", []string{"#B"}, []byte(`{"state":"ended"}`), expiresAt); err != nil {
		t.Fatal(err)
	}
	if mapping := cache.mappings["#C"]; mapping.clanTag != "#OTHER" {
		t.Fatalf("replacement removed newer mapping ownership: %#v", mapping)
	}
	if err := cache.Delete(t.Context(), "#CLAN"); err != nil {
		t.Fatal(err)
	}
	if _, ok := cache.mappings["#B"]; ok {
		t.Fatal("explicit cache cleanup left its participant mapping behind")
	}
	if mapping := cache.mappings["#C"]; mapping.clanTag != "#OTHER" {
		t.Fatalf("cache cleanup removed another clan's mapping ownership: %#v", mapping)
	}
	if err := cache.Replace(t.Context(), "#CLAN", []string{"#B"}, []byte(`{"state":"ended"}`), expiresAt); err != nil {
		t.Fatal(err)
	}

	now = expiresAt
	if _, ok, err := cache.LoadRaw(t.Context(), "#CLAN"); err != nil || ok {
		t.Fatalf("expired payload = ok %v, err %v", ok, err)
	}
	if len(cache.mappings) != 0 {
		t.Fatalf("participant mappings survived associated payload expiry: %#v", cache.mappings)
	}
}

func TestCapitalRaidInvalidEndCleansExistingCache(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	cache := newMemoryCapitalRaidCache("botclans:snapshot:", func() time.Time { return now })
	if err := cache.Replace(t.Context(), "#CLAN", []string{"#A"}, []byte(`{"state":"ongoing"}`), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	domain := &botClansDomain{capitalRaids: cache}
	if err := domain.handleRaidChange(t.Context(), botClansTestApp(), TrackedItem[clashy.RaidLogEntry]{
		Tag:     "#CLAN",
		Current: &clashy.RaidLogEntry{},
	}); err != nil {
		t.Fatal(err)
	}
	if len(cache.entries) != 0 || len(cache.mappings) != 0 {
		t.Fatalf("invalid raid end left cache state: entries %#v mappings %#v", cache.entries, cache.mappings)
	}
}

func TestCapitalRaidUnchangedResponseRefreshesExpiryAndMappings(t *testing.T) {
	now := time.Now().UTC()
	end := now.Add(2 * time.Hour)
	raid := clashy.RaidLogEntry{
		State:   "ongoing",
		EndTime: &clashy.Timestamp{Time: end},
		Members: []clashy.RaidMember{{Tag: "#A"}},
	}
	raw := jsonBytes(raid)
	cache := newMemoryCapitalRaidCache("botclans:snapshot:", func() time.Time { return now })
	cache.entries["#CLAN"] = memoryCapitalRaidCacheEntry{
		compressed: utils.Compress(raw),
		expiresAt:  now.Add(24 * time.Hour),
		members:    make(map[string]struct{}),
	}
	domain := &botClansDomain{capitalRaids: cache}
	if err := domain.handleRaidChange(t.Context(), botClansTestApp(), TrackedItem[clashy.RaidLogEntry]{
		Tag:     "#CLAN",
		Current: &raid,
		Raw:     raw,
	}); err != nil {
		t.Fatal(err)
	}
	wantExpiry := end.Add(10 * time.Minute)
	if got := cache.entries["#CLAN"].expiresAt; !got.Equal(wantExpiry) {
		t.Fatalf("refreshed payload expiry = %v, want %v", got, wantExpiry)
	}
	if mapping, ok := cache.mappings["#A"]; !ok || mapping.clanTag != "#CLAN" || !mapping.expiresAt.Equal(wantExpiry) {
		t.Fatalf("refreshed member mapping = %#v, %v", mapping, ok)
	}
}

func TestCapitalRaidParticipantTagsAreUnique(t *testing.T) {
	raid := clashy.RaidLogEntry{Members: []clashy.RaidMember{
		{Tag: "#A"},
		{Tag: ""},
		{Tag: "#B"},
		{Tag: "#A"},
	}}
	if got, want := capitalRaidParticipantTags(raid), []string{"#A", "#B"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("participant tags = %#v, want %#v", got, want)
	}
}

func TestBotClansHasNoCapitalRaidSQLQueries(t *testing.T) {
	source, err := os.ReadFile("bot_clans.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"capital_raid_cache", "capital_raid_members"} {
		if strings.Contains(string(source), table) {
			t.Fatalf("bot_clans.go still references removed SQL table %s", table)
		}
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
