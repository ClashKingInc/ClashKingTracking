//go:build script_internal_tests

package scripts

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestValidateScheduledConfig(t *testing.T) {
	cfg := platform.Config{MockDB: true}
	cfg.ScheduledIntervalSeconds = 0
	if err := validateScheduledConfig(cfg); err == nil {
		t.Fatal("expected invalid interval error")
	}
	cfg.ScheduledIntervalSeconds = 60
	if err := validateScheduledConfig(cfg); err != nil {
		t.Fatalf("valid mock config rejected: %v", err)
	}
}

func TestJSONAnyNormalizesStructPayloads(t *testing.T) {
	type payload struct {
		Name  string `json:"name"`
		Score int    `json:"score"`
	}
	got := jsonAny(payload{Name: "global", Score: 10})
	want := map[string]any{"name": "global", "score": float64(10)}
	if !reflect.DeepEqual(got, want) {
		raw, _ := json.Marshal(got)
		t.Fatalf("jsonAny = %s, want %#v", raw, want)
	}
}

func TestLeaderboardLocationIDsAddsGlobal(t *testing.T) {
	got := leaderboardLocationIDs([]clashy.Location{
		{ID: 32000006, Name: "International"},
		{ID: 0, Name: "Invalid"},
		{ID: 32000007, Name: "Afghanistan"},
		{ID: 32000006, Name: "Duplicate"},
	})
	want := []string{"32000006", "32000007", "global"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("leaderboardLocationIDs = %#v, want %#v", got, want)
	}
}

func TestCurrentClanRankingPathsUseExactTypes(t *testing.T) {
	got := make([]string, 0, len(currentClanRankingPaths))
	for _, path := range currentClanRankingPaths {
		got = append(got, path.RankingType)
	}
	if want := []string{"home", "builder_base", "capital"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current clan ranking types = %#v, want %#v", got, want)
	}
}

func TestCurrentClanRankingGroupAcceptsAuthoritativeResponsesUpToTop200(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	rankings := rankedClansForCurrentTest()
	cases := []struct {
		rankingType string
		points      func(clashy.RankedClan) int
		want        int
	}{
		{rankingType: "home", points: func(row clashy.RankedClan) int { return row.Points }, want: 50001},
		{rankingType: "builder_base", points: func(row clashy.RankedClan) int { return row.BuilderBasePoints }, want: 40001},
		{rankingType: "capital", points: func(row clashy.RankedClan) int { return row.CapitalPoints }, want: 3001},
	}
	for _, tc := range cases {
		t.Run(tc.rankingType, func(t *testing.T) {
			group, err := currentClanRankingGroupFromResponse(tc.rankingType, "global", rankings, tc.points, now)
			if err != nil {
				t.Fatal(err)
			}
			if group.RankingType != tc.rankingType || group.LocationID != "global" || len(group.Rows) != currentClanRankingLimit {
				t.Fatalf("unexpected group: %#v", group)
			}
			if row := group.Rows[0]; row.ClanTag != "#CLAN001" || row.Rank != 1 || row.Points != tc.want || !row.UpdatedAt.Equal(now) {
				t.Fatalf("unexpected first row: %#v", row)
			}
		})
	}
}

func TestCurrentClanRankingGroupAcceptsShortAndEmptyAuthoritativeResponses(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	points := func(row clashy.RankedClan) int { return row.Points }
	valid := rankedClansForCurrentTest()
	for name, rankings := range map[string][]clashy.RankedClan{
		"short": valid[:37],
		"empty": {},
	} {
		t.Run(name, func(t *testing.T) {
			group, err := currentClanRankingGroupFromResponse("home", "32000006", rankings, points, now)
			if err != nil {
				t.Fatal(err)
			}
			if len(group.Rows) != len(rankings) {
				t.Fatalf("stored rows = %d, want all %d authoritative rows", len(group.Rows), len(rankings))
			}
		})
	}
}

func TestCurrentClanRankingGroupRejectsInvalidResponses(t *testing.T) {
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	points := func(row clashy.RankedClan) int { return row.Points }
	valid := rankedClansForCurrentTest()
	tests := map[string][]clashy.RankedClan{
		"oversized":       append(append([]clashy.RankedClan(nil), valid...), clashy.RankedClan{Clan: clashy.Clan{Tag: "#EXTRA", Points: 1}, Rank: 201}),
		"duplicate_tag":   append([]clashy.RankedClan(nil), valid...),
		"duplicate_rank":  append([]clashy.RankedClan(nil), valid...),
		"negative_points": append([]clashy.RankedClan(nil), valid...),
	}
	tests["duplicate_tag"][1].Tag = tests["duplicate_tag"][0].Tag
	tests["duplicate_rank"][1].Rank = tests["duplicate_rank"][0].Rank
	tests["negative_points"][0].Points = -1
	for name, rankings := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := currentClanRankingGroupFromResponse("home", "32000006", rankings, points, now); err == nil {
				t.Fatal("invalid ranking response was accepted")
			}
		})
	}
	zeroPoints := append([]clashy.RankedClan(nil), valid[:1]...)
	zeroPoints[0].Points = 0
	if _, err := currentClanRankingGroupFromResponse("home", "32000006", zeroPoints, points, now); err != nil {
		t.Fatalf("authoritative zero-point row was rejected: %v", err)
	}
	if _, err := currentClanRankingGroupFromResponse("players", "global", valid, points, now); err == nil {
		t.Fatal("unsupported player ranking type was accepted")
	}
	if _, err := currentClanRankingGroupFromResponse("home", "us", valid, points, now); err == nil {
		t.Fatal("non-numeric local location was accepted")
	}
}

func TestMemoryScheduledStoreReplacesOnlyRequestedClanRankingGroup(t *testing.T) {
	store := newMemoryScheduledStore()
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	groups := []currentClanRankingGroup{
		{RankingType: "home", LocationID: "global", Rows: []currentClanRankingRow{{ClanTag: "#OLD", Rank: 1, Points: 10, UpdatedAt: now}}},
		{RankingType: "builder_base", LocationID: "global", Rows: []currentClanRankingRow{{ClanTag: "#BUILDER", Rank: 1, Points: 20, UpdatedAt: now}}},
		{RankingType: "home", LocationID: "32000006", Rows: []currentClanRankingRow{{ClanTag: "#LOCAL", Rank: 1, Points: 30, UpdatedAt: now}}},
	}
	for _, group := range groups {
		if _, err := store.ReplaceCurrentClanRankingGroup(t.Context(), group); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.ReplaceCurrentClanRankingGroup(t.Context(), currentClanRankingGroup{
		RankingType: "home",
		LocationID:  "global",
		Rows:        []currentClanRankingRow{{ClanTag: "#NEW", Rank: 1, Points: 40, UpdatedAt: now}},
	}); err != nil {
		t.Fatal(err)
	}
	if got := store.currentClanRankings["home\x00global"]; len(got) != 1 || got["#NEW"].Points != 40 {
		t.Fatalf("home/global group was not replaced: %#v", got)
	}
	if got := store.currentClanRankings["builder_base\x00global"]; len(got) != 1 || got["#BUILDER"].Points != 20 {
		t.Fatalf("builder/global group was touched: %#v", got)
	}
	if got := store.currentClanRankings["home\x0032000006"]; len(got) != 1 || got["#LOCAL"].Points != 30 {
		t.Fatalf("home/local group was touched: %#v", got)
	}
	if _, err := store.ReplaceCurrentClanRankingGroup(t.Context(), currentClanRankingGroup{
		RankingType: "home",
		LocationID:  "global",
		Rows:        nil,
	}); err != nil {
		t.Fatal(err)
	}
	if got := store.currentClanRankings["home\x00global"]; len(got) != 0 {
		t.Fatalf("authoritative empty group did not clear stale rows: %#v", got)
	}
	if got := store.currentClanRankings["builder_base\x00global"]; len(got) != 1 || got["#BUILDER"].Points != 20 {
		t.Fatalf("empty home/global replacement touched another group: %#v", got)
	}
}

func TestCurrentClanRankingSQLScopesReplacementToOneGroup(t *testing.T) {
	if !strings.Contains(replaceCurrentClanRankingGroupSQL, "ON CONFLICT (clan_tag, ranking_type, location_id)") {
		t.Fatalf("upsert does not use the decision 10 primary key: %s", replaceCurrentClanRankingGroupSQL)
	}
	for _, predicate := range []string{"current.ranking_type = $1", "current.location_id = $2"} {
		if !strings.Contains(deleteStaleCurrentClanRankingGroupSQL, predicate) {
			t.Fatalf("stale delete missing group predicate %q: %s", predicate, deleteStaleCurrentClanRankingGroupSQL)
		}
	}
	if !strings.Contains(deleteStaleCurrentClanRankingGroupSQL, "stage.clan_tag = current.clan_tag") {
		t.Fatalf("stale delete does not preserve staged clan tags: %s", deleteStaleCurrentClanRankingGroupSQL)
	}
}

func TestLeaderboardPayloadHasItems(t *testing.T) {
	if leaderboardPayloadHasItems([]string{}) {
		t.Fatal("empty slices should be skipped")
	}
	if !leaderboardPayloadHasItems([]string{"one"}) {
		t.Fatal("non-empty slices should be stored")
	}
	if !leaderboardPayloadHasItems(struct{ Name string }{Name: "not a leaderboard slice"}) {
		t.Fatal("non-collection payloads should be stored")
	}
}

func rankedClansForCurrentTest() []clashy.RankedClan {
	out := make([]clashy.RankedClan, 0, currentClanRankingLimit)
	for rank := 1; rank <= currentClanRankingLimit; rank++ {
		out = append(out, clashy.RankedClan{
			Clan: clashy.Clan{
				Tag:               "#CLAN" + strconv.Itoa(1000 + rank)[1:],
				Points:            50000 + rank,
				BuilderBasePoints: 40000 + rank,
				CapitalPoints:     3000 + rank,
			},
			Rank: rank,
		})
	}
	return out
}

func TestShouldStoreLeaderboardKindSkipsCapitalExceptTuesday(t *testing.T) {
	monday := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	tuesday := time.Date(2026, 6, 2, 12, 0, 0, 0, time.UTC)
	if shouldStoreLeaderboardKind(leaderboardKindCapital, monday) {
		t.Fatal("capital rankings should not be stored outside Tuesday")
	}
	if !shouldStoreLeaderboardKind(leaderboardKindCapital, tuesday) {
		t.Fatal("capital rankings should be stored on Tuesday")
	}
	if !shouldStoreLeaderboardKind("player_trophies", monday) {
		t.Fatal("non-capital rankings should be stored every run")
	}
}

func TestPreviousRankedSeasonIDUsesMondayFiveUTC(t *testing.T) {
	before := time.Date(2026, 6, 22, 11, 59, 0, 0, time.UTC)
	if _, ok := previousRankedSeasonID(before); ok {
		t.Fatal("ranked sync should wait until Monday noon UTC")
	}
	after := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	seasonID, ok := previousRankedSeasonID(after)
	if !ok {
		t.Fatal("ranked sync should be ready after Monday noon UTC")
	}
	want := time.Date(2026, 6, 15, 5, 0, 0, 0, time.UTC).Unix()
	if seasonID != want {
		t.Fatalf("seasonID = %d, want %d", seasonID, want)
	}
}

func TestRankedGroupMemberRowsUseMemberOrderAsPlacement(t *testing.T) {
	group := &clashy.LeagueTierGroup{Members: []clashy.LeagueTierGroupMember{
		{PlayerTag: "#A", PlayerName: "A", ClanTag: "#C", ClanName: "Clan", LeagueTrophies: 500, AttackWinCount: 1, DefenseLoseCount: 2},
		{PlayerTag: "#B", PlayerName: "B", LeagueTrophies: 450, AttackWinCount: 3, DefenseLoseCount: 4},
	}}
	rows := rankedGroupMemberRows("#GROUP", 1781499600, 105000028, group)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].Placement != 1 || rows[1].Placement != 2 {
		t.Fatalf("placements = %d/%d, want 1/2", rows[0].Placement, rows[1].Placement)
	}
	if rows[0].GroupTag != "#GROUP" || rows[0].LeagueTierID != 105000028 || rows[0].LeagueTrophies != 500 {
		t.Fatalf("unexpected first row: %#v", rows[0])
	}
}

func TestLeaderboardSnapshotItemsExtractsQueryableRows(t *testing.T) {
	date := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	got := leaderboardSnapshotItems("clan_trophies", "global", date, []clashy.RankedClan{{
		Clan: clashy.Clan{Tag: "#2ABC", Name: "One", Points: 70000},
		Rank: 1,
	}})
	if len(got) != 1 {
		t.Fatalf("leaderboardSnapshotItems returned %d rows, want 1", len(got))
	}
	row := got[0]
	if row.Kind != "clan_trophies" || row.LocationID != "global" || !row.Date.Equal(date) || row.Tag != "#2ABC" {
		t.Fatalf("unexpected row identity: %#v", row)
	}
	if row.Name != "One" || row.Rank != 1 {
		t.Fatalf("unexpected row fields: %#v", row)
	}

	players := leaderboardSnapshotItems("player_versus_trophies", "32000006", date, []clashy.RankedPlayer{{
		Player:       clashy.Player{Tag: "#2PLY", Name: "Player", BuilderBaseTrophies: 6000},
		Rank:         3,
		PreviousRank: 4,
	}})
	if len(players) != 1 || players[0].Kind != "player_versus_trophies" || players[0].Rank != 3 {
		t.Fatalf("unexpected player row: %#v", players)
	}
}
