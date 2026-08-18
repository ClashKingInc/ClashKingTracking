//go:build script_internal_tests

package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestTrackedClanSnapshotDiffDoesNotAdvanceUntilStored(t *testing.T) {
	store := &memoryTrackedClanSnapshotStore{values: make(map[string][]byte)}
	clan := clashy.Clan{Tag: "#CLAN", Name: "Before"}
	prefix := "trackedclans:test:"

	_, raw, hasPrevious, changed, err := loadTrackedClanSnapshotChange(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hasPrevious || !changed {
		t.Fatalf("first snapshot = hasPrevious %v changed %v, want false/true", hasPrevious, changed)
	}
	if err := store.StoreRaw(t.Context(), trackedClanSnapshotKey(prefix, "clan", clan.Tag), raw); err != nil {
		t.Fatal(err)
	}
	_, _, hasPrevious, changed, err = loadTrackedClanSnapshotChange(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || changed {
		t.Fatalf("same snapshot = hasPrevious %v changed %v, want true/false", hasPrevious, changed)
	}
	clan.Name = "After"
	previous, _, hasPrevious, changed, err := loadTrackedClanSnapshotChange(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || !changed || previous == nil || previous.Name != "Before" {
		t.Fatalf("changed snapshot = previous %#v hasPrevious %v changed %v", previous, hasPrevious, changed)
	}
	stored, _, ok, err := loadTrackedClanSnapshot[clashy.Clan](t.Context(), store, prefix, "clan", clan.Tag)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || stored == nil || stored.Name != "Before" {
		t.Fatalf("snapshot advanced before store: %#v", stored)
	}
}

func TestClosedWarLogResponsesAreNormalAbsence(t *testing.T) {
	if !closedWarLogResponse(&clashy.Forbidden{}) {
		t.Fatal("closed war-log forbidden response should be treated as no readable war")
	}
	if !closedWarLogResponse(&clashy.NotFound{}) {
		t.Fatal("missing current-war response should be treated as no readable war")
	}
	if closedWarLogResponse(&clashy.GatewayError{}) {
		t.Fatal("gateway failure must remain visible")
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

func TestCWLDiscoveryStopsAfterSignupDeadline(t *testing.T) {
	if !cwlDiscoveryWindow(time.Date(2026, 5, 3, 23, 59, 0, 0, time.UTC)) {
		t.Fatal("the third should remain in the discovery window")
	}
	if cwlSignupClosed(time.Date(2026, 5, 3, 23, 59, 0, 0, time.UTC)) {
		t.Fatal("a clan was marked no-spin before the discovery window ended")
	}
	if cwlDiscoveryWindow(time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("an undiscovered clan cannot enter CWL after the third")
	}
	if !cwlSignupClosed(time.Date(2026, 5, 4, 0, 0, 0, 0, time.UTC)) {
		t.Fatal("a clan was not marked no-spin after the discovery window ended")
	}
}

func TestLiveCWLGroupRefreshBacksOffBeforeSignup(t *testing.T) {
	now := time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC)
	checked := now.Add(-time.Minute)
	if shouldRefreshCWLGroup(now, botCWLState{GroupState: "notInWar", GroupCheckedAt: checked}, true) {
		t.Fatal("a negative pre-signup lookup was retried before the 15-minute refresh")
	}
	if !shouldRefreshCWLGroup(now, botCWLState{GroupState: "preparation", GroupCheckedAt: checked}, true) {
		t.Fatal("an active group without war tags did not remain on the fast discovery loop")
	}
	if shouldRefreshCWLGroup(now, botCWLState{GroupState: "inWar", GroupCheckedAt: checked, BattleWarTag: "#WAR"}, true) {
		t.Fatal("a known tagged war caused an unnecessary group refresh")
	}
	if !shouldRefreshCWLGroup(now, botCWLState{GroupState: "notInWar", GroupCheckedAt: now.Add(-16 * time.Minute)}, true) {
		t.Fatal("a negative lookup was not refreshed after 15 minutes")
	}
}

func TestValidCWLWarTagRoundsKeepsOnlyRealRounds(t *testing.T) {
	group := &clashy.ClanWarLeagueGroup{Rounds: []struct {
		WarTags []string `json:"warTags,omitempty"`
	}{
		{WarTags: []string{"#WAR1", "#WAR2"}},
		{WarTags: []string{"#0", ""}},
		{WarTags: []string{"#WAR3"}},
	}}
	want := [][]string{{"#WAR1", "#WAR2"}, {"#WAR3"}}
	if got := validCWLWarTagRounds(group); !reflect.DeepEqual(got, want) {
		t.Fatalf("valid rounds = %#v, want %#v", got, want)
	}
}

func TestCWLRoundRolesAllowCurrentAndPreparationToOverlap(t *testing.T) {
	tests := []struct {
		name                         string
		rounds                       int
		latestState                  clashy.WarState
		wantCurrent, wantPreparation int
	}{
		{name: "first round preparation remains preparation", rounds: 1, latestState: clashy.WarStatePreparation, wantCurrent: -1, wantPreparation: 0},
		{name: "ongoing plus next preparation", rounds: 2, latestState: clashy.WarStatePreparation, wantCurrent: 0, wantPreparation: 1},
		{name: "new round promoted when battle begins", rounds: 2, latestState: clashy.WarStateInWar, wantCurrent: 1, wantPreparation: -1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current, preparation := cwlRoundRoleIndexes(test.rounds, test.latestState)
			if current != test.wantCurrent || preparation != test.wantPreparation {
				t.Fatalf("roles = (%d, %d), want (%d, %d)", current, preparation, test.wantCurrent, test.wantPreparation)
			}
		})
	}
}

func TestCWLPanelTargetIsExplicitInV2Event(t *testing.T) {
	war := clashy.ClanWar{
		Clan:     &clashy.WarClan{Tag: "#AAA"},
		Opponent: &clashy.WarClan{Tag: "#BBB"},
	}
	preparation := cwlWarEventBase("#AAA", "#PREP", war, json.RawMessage(`{"state":"preparation"}`), json.RawMessage(`{"state":"inWar"}`), cwlWarPreparation, false)
	if preparation["war_role"] != "preparation" || preparation["panel_target"] != false || preparation["war_tag"] != "#PREP" {
		t.Fatalf("preparation event identity = %#v", preparation)
	}
	battle := cwlWarEventBase("#AAA", "#BATTLE", war, json.RawMessage(`{"state":"inWar"}`), nil, cwlWarBattle, true)
	if battle["war_role"] != "battle" || battle["panel_target"] != true || battle["war_type"] != "cwl" {
		t.Fatalf("battle event identity = %#v", battle)
	}
}

func TestResolveClanCWLWarsFindsOngoingAndNextPreparation(t *testing.T) {
	wars := map[string]map[string]any{
		"#22222222": {"state": "inWar", "clan": map[string]any{"tag": "#Q2Q2Q2"}, "opponent": map[string]any{"tag": "#Q2Q2Q8"}},
		"#22222228": {"state": "inWar", "clan": map[string]any{"tag": "#AAA"}, "opponent": map[string]any{"tag": "#Q2Q2Q9"}},
		"#88888888": {"state": "preparation", "clan": map[string]any{"tag": "#Q8Q8Q2"}, "opponent": map[string]any{"tag": "#Q8Q8Q8"}},
		"#88888889": {"state": "preparation", "clan": map[string]any{"tag": "#Q8Q8Q9"}, "opponent": map[string]any{"tag": "#AAA"}},
	}
	var callsMu sync.Mutex
	calls := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		tag := path.Base(request.URL.Path)
		callsMu.Lock()
		calls[tag]++
		callsMu.Unlock()
		war, ok := wars[tag]
		if !ok {
			http.NotFound(response, request)
			return
		}
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(war)
	}))
	defer server.Close()

	clientConfig := clashy.DefaultClientConfig()
	clientConfig.BaseURL = server.URL + "/v1"
	clientConfig.LookupCache = false
	clientConfig.UpdateCache = false
	client, err := clashy.NewClient(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	app := trackedClansTestApp()
	app.Clash = client
	app.Availability = platform.NewAvailabilityGate(nil)
	limiter, err := newTrackingLimiter(100)
	if err != nil {
		t.Fatal(err)
	}
	group := &clashy.ClanWarLeagueGroup{State: "inWar", Rounds: []struct {
		WarTags []string `json:"warTags,omitempty"`
	}{
		{WarTags: []string{"#22222222", "#22222228"}},
		{WarTags: []string{"#88888888", "#88888889"}},
	}}
	domain := &trackedClansDomain{}
	resolved, err := domain.resolveClanCWLWars(t.Context(), app, limiter, newCWLCycleWarCache(), "#AAA", group)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.battleTag != "#22222228" || resolved.battle == nil || resolved.battle.war.State != clashy.WarStateInWar {
		callsMu.Lock()
		defer callsMu.Unlock()
		t.Fatalf("battle = %q %#v; calls = %#v", resolved.battleTag, resolved.battle, calls)
	}
	if resolved.preparationTag != "#88888889" || resolved.preparation == nil || resolved.preparation.war.State != clashy.WarStatePreparation {
		t.Fatalf("preparation = %q %#v", resolved.preparationTag, resolved.preparation)
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if calls["#88888888"] != 1 {
		t.Fatalf("latest-round probe was not reused: calls = %#v", calls)
	}
	if len(calls) != 4 {
		t.Fatalf("war-tag calls = %#v, want each distinct matchup at most once", calls)
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

func TestRaidReminderUsesV2ConfigurationFields(t *testing.T) {
	value := (raidReminder{
		ID: "reminder", ServerID: "server", ClanTag: "#CLAN", ChannelID: "channel",
		MinutesRemaining: 60, TownHalls: []int{16}, WarTypes: []string{"cwl"},
	}).eventData()
	for _, retired := range []string{"_id", "server", "clan", "channel", "time", "townhalls", "types", "attack_threshold"} {
		if _, exists := value[retired]; exists {
			t.Fatalf("raid reminder retained compatibility field %q: %#v", retired, value)
		}
	}
	for _, field := range []string{"id", "server_id", "type_name", "clan_tag", "channel_id", "minutes_remaining", "town_halls", "war_types", "trigger_threshold"} {
		if _, exists := value[field]; !exists {
			t.Fatalf("raid reminder is missing v2 field %q: %#v", field, value)
		}
	}
}

func TestCapitalRaidCacheKeys(t *testing.T) {
	const prefix = "trackedclans:snapshot:"
	if got, want := capitalRaidPayloadKey(prefix, "#CLAN"), "trackedclans:snapshot:raid:#CLAN"; got != want {
		t.Fatalf("payload key = %q, want %q", got, want)
	}
	if got, want := capitalRaidParticipantSetKey(prefix, "#CLAN"), "trackedclans:snapshot:raid-members:#CLAN"; got != want {
		t.Fatalf("participant set key = %q, want %q", got, want)
	}
	if got, want := capitalRaidMemberKey(prefix, "#PLAYER"), "trackedclans:snapshot:raid-member:#PLAYER"; got != want {
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
	cache := newMemoryCapitalRaidCache("trackedclans:snapshot:", func() time.Time { return now })
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
	cache := newMemoryCapitalRaidCache("trackedclans:snapshot:", func() time.Time { return now })
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
	cache := newMemoryCapitalRaidCache("trackedclans:snapshot:", func() time.Time { return now })
	if err := cache.Replace(t.Context(), "#CLAN", []string{"#A"}, []byte(`{"state":"ongoing"}`), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	domain := &trackedClansDomain{capitalRaids: cache}
	if err := domain.handleRaidChange(t.Context(), trackedClansTestApp(), TrackedItem[clashy.RaidLogEntry]{
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
	cache := newMemoryCapitalRaidCache("trackedclans:snapshot:", func() time.Time { return now })
	cache.entries["#CLAN"] = memoryCapitalRaidCacheEntry{
		compressed: utils.Compress(raw),
		expiresAt:  now.Add(24 * time.Hour),
		members:    make(map[string]struct{}),
	}
	domain := &trackedClansDomain{capitalRaids: cache}
	if err := domain.handleRaidChange(t.Context(), trackedClansTestApp(), TrackedItem[clashy.RaidLogEntry]{
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

func TestTrackedClansHasNoCapitalRaidSQLQueries(t *testing.T) {
	source, err := os.ReadFile("tracked_clans.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"capital_raid_cache", "capital_raid_members"} {
		if strings.Contains(string(source), table) {
			t.Fatalf("tracked_clans.go still references removed SQL table %s", table)
		}
	}
}

type capturingTrackedClanStore struct {
	memoryTrackedClanStore
	warTag string
	calls  int
}

func (s *capturingTrackedClanStore) UpsertCurrentWar(_ context.Context, sourceTag string, war clashy.ClanWar, warTag string) (string, error) {
	s.warTag = warTag
	s.calls++
	ingest, err := buildWarIngest(war, sourceTag, false, warTag, "", "")
	if err != nil || len(ingest.Schedules) == 0 {
		return "", err
	}
	return ingest.Schedules[0].ScheduleKey, nil
}

func TestLiveCWLChangeCreatesDurableTaggedSchedule(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	war := sampleWar(now.Add(-time.Hour), now, now.Add(24*time.Hour))
	store := &capturingTrackedClanStore{}
	domain := &trackedClansDomain{
		store: store,
		snapshots: &memoryTrackedClanSnapshotStore{
			values: make(map[string][]byte),
		},
		snapshotPrefix: "trackedclans:test:",
	}
	app := trackedClansTestApp()
	app.Config.MockDB = true
	if err := domain.handleCWLWarChange(t.Context(), app, "#AAA", "#CWLTAG", war, nil, nil, cwlWarBattle, false, true); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || store.warTag != "#CWLTAG" {
		t.Fatalf("durable CWL schedule calls=%d warTag=%q, want 1/#CWLTAG", store.calls, store.warTag)
	}
	if err := domain.handleCWLWarChange(t.Context(), app, "#AAA", "#CWLTAG", war, nil, nil, cwlWarBattle, false, true); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 {
		t.Fatalf("unchanged CWL snapshot created another schedule: calls=%d", store.calls)
	}
}

func TestOverlappingCWLWarsUseIndependentSnapshots(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	current := sampleWar(now.Add(-25*time.Hour), now.Add(-24*time.Hour), now)
	current.State = clashy.WarStateInWar
	preparation := sampleWar(now.Add(-time.Hour), now, now.Add(24*time.Hour))
	preparation.State = clashy.WarStatePreparation
	store := &capturingTrackedClanStore{}
	snapshots := &memoryTrackedClanSnapshotStore{values: make(map[string][]byte)}
	domain := &trackedClansDomain{store: store, snapshots: snapshots, snapshotPrefix: "trackedclans:test:"}
	app := trackedClansTestApp()
	app.Config.MockDB = true

	if err := domain.handleCWLWarChange(t.Context(), app, "#AAA", "#CURRENT", current, nil, nil, cwlWarBattle, false, true); err != nil {
		t.Fatal(err)
	}
	if err := domain.handleCWLWarChange(t.Context(), app, "#AAA", "#PREP", preparation, nil, nil, cwlWarPreparation, false, false); err != nil {
		t.Fatal(err)
	}
	if store.calls != 2 {
		t.Fatalf("schedule calls = %d, want one for each overlapping war", store.calls)
	}
	for _, key := range []string{
		trackedClanSnapshotKey(domain.snapshotPrefix, "cwlwar:CURRENT", "#AAA"),
		trackedClanSnapshotKey(domain.snapshotPrefix, "cwlwar:PREP", "#AAA"),
	} {
		if _, ok := snapshots.values[key]; !ok {
			t.Fatalf("missing independent CWL snapshot %q", key)
		}
	}
}

func trackedClansTestApp() *platform.App {
	return &platform.App{
		Config: platform.Config{
			TrackedClanRequestsPerSecond: 950,
			TrackedClanSnapshotPrefix:    "trackedclans:test:",
			TrackedClanCWLStateSnapshot:  "cwlstate",
		},
		Stats: platform.NewTracker(),
	}
}
