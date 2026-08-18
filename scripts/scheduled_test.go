//go:build script_internal_tests

package scripts

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

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

func TestLeaderboardHistoryPathsUseCanonicalKinds(t *testing.T) {
	got := make([]string, 0, len(leaderboardHistoryPaths))
	for _, path := range leaderboardHistoryPaths {
		got = append(got, path.Kind)
	}
	want := []string{
		leaderboardHistoryPlayerHomeTrophies,
		leaderboardHistoryPlayerBuilderBaseTrophies,
		leaderboardHistoryClanHomePoints,
		leaderboardHistoryClanBuilderBasePoints,
		leaderboardHistoryClanCapitalPoints,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("leaderboard history kinds = %#v, want %#v", got, want)
	}
}

func TestLeaderboardHistoryFetchReturnsSuccessfulGroupsWithErrors(t *testing.T) {
	original := leaderboardHistoryPaths
	t.Cleanup(func() { leaderboardHistoryPaths = original })
	leaderboardHistoryPaths = []struct {
		Kind string
		Load leaderboardLoader
	}{
		{
			Kind: leaderboardHistoryPlayerHomeTrophies,
			Load: func(context.Context, *clashy.Client, string) (any, error) {
				return []clashy.RankedPlayer{{
					Player: clashy.Player{Tag: "#P1", Name: "Player"},
					Rank:   1,
				}}, nil
			},
		},
		{
			Kind: leaderboardHistoryClanHomePoints,
			Load: func(context.Context, *clashy.Client, string) (any, error) {
				return nil, errors.New("official leaderboard unavailable")
			},
		},
	}
	domain := &scheduledDomain{}
	app := &platform.App{Stats: platform.NewTracker()}
	groups, err := domain.doLeaderboardHistory(
		t.Context(),
		app,
		[]string{"global"},
		time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC),
	)
	if err == nil {
		t.Fatal("failed official group did not keep the cycle due for retry")
	}
	if len(groups) != 1 || groups[0].Kind != leaderboardHistoryPlayerHomeTrophies {
		t.Fatalf("successful history groups = %#v", groups)
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

func TestLeaderboardHistoryKindValidationRejectsLegacyKinds(t *testing.T) {
	for _, kind := range []string{
		leaderboardHistoryPlayerHomeTrophies,
		leaderboardHistoryPlayerBuilderBaseTrophies,
		leaderboardHistoryClanHomePoints,
		leaderboardHistoryClanBuilderBasePoints,
		leaderboardHistoryClanCapitalPoints,
	} {
		if !validLeaderboardHistoryKind(kind) {
			t.Fatalf("canonical leaderboard history kind rejected: %s", kind)
		}
	}
	for _, kind := range []string{
		"player_trophies",
		"player_versus_trophies",
		"clan_trophies",
		"clan_versus_trophies",
		"capital",
		"league",
		"townhall",
		"trophy_buckets",
	} {
		if validLeaderboardHistoryKind(kind) {
			t.Fatalf("legacy leaderboard history kind accepted: %s", kind)
		}
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

func TestLeaderboardHistoryGroupsMapAllFiveTypedTables(t *testing.T) {
	date := time.Date(2026, 7, 26, 15, 30, 0, 0, time.UTC)
	player := clashy.RankedPlayer{
		Player: clashy.Player{
			Tag:                 "#2PLY",
			Name:                "Player",
			ExpLevel:            250,
			Trophies:            7000,
			BuilderBaseTrophies: 6000,
			VersusAttackWins:    123,
			LeagueTier:          clashy.League{ID: 29000027, Name: "Legend I"},
			BuilderBaseLeague:   &clashy.League{ID: 44000041, Name: "Diamond I"},
			Clan: &clashy.PlayerClan{
				Tag:   "#2ABC",
				Name:  "Clan",
				Level: 20,
				Badge: historyBadgeForTest("player-clan-token"),
			},
		},
		League:       clashy.League{ID: 29000022, Name: "Legend League"},
		AttackWins:   12,
		DefenseWins:  8,
		Rank:         3,
		PreviousRank: 4,
	}
	clan := clashy.RankedClan{
		Clan: clashy.Clan{
			Tag:               "#2ABC",
			Name:              "Clan",
			Level:             20,
			Points:            70000,
			BuilderBasePoints: 60000,
			CapitalPoints:     5000,
			MemberCount:       49,
			Location:          &clashy.Location{ID: 32000006, Name: "International"},
			Badge:             historyBadgeForTest("clan-token"),
		},
		Rank:         1,
		PreviousRank: 2,
	}
	tests := []struct {
		kind    string
		payload any
	}{
		{leaderboardHistoryPlayerHomeTrophies, []clashy.RankedPlayer{player}},
		{leaderboardHistoryPlayerBuilderBaseTrophies, []clashy.RankedPlayer{player}},
		{leaderboardHistoryClanHomePoints, []clashy.RankedClan{clan}},
		{leaderboardHistoryClanBuilderBasePoints, []clashy.RankedClan{clan}},
		{leaderboardHistoryClanCapitalPoints, []clashy.RankedClan{clan}},
	}
	for _, locationID := range []string{"global", "32000006"} {
		for _, tc := range tests {
			t.Run(tc.kind+"/"+locationID, func(t *testing.T) {
				group, err := leaderboardHistoryGroupFromResponse(tc.kind, locationID, date, tc.payload)
				if err != nil {
					t.Fatal(err)
				}
				if group.Kind != tc.kind || group.LocationID != locationID || !group.Date.Equal(dayStart(date)) {
					t.Fatalf("unexpected group identity: %#v", group)
				}
				switch rows := group.Rows.(type) {
				case []models.PlayerTrophyHistoryRow:
					if len(rows) != 1 {
						t.Fatalf("home player rows = %d", len(rows))
					}
					row := rows[0]
					if row.LocationID != locationID ||
						row.PlayerTag != player.Tag ||
						row.PlayerName != player.Name ||
						row.ExpLevel != player.ExpLevel ||
						row.Trophies != player.Trophies ||
						row.AttackWins != player.AttackWins ||
						row.DefenseWins != player.DefenseWins ||
						row.Rank != player.Rank ||
						row.PreviousRank == nil || *row.PreviousRank != player.PreviousRank ||
						row.ClanBadgeToken == nil || *row.ClanBadgeToken != "player-clan-token" ||
						row.LeagueID == nil || *row.LeagueID != player.LeagueTier.ID {
						t.Fatalf("home player typed row = %#v", row)
					}
				case []models.PlayerBuilderBaseTrophyHistoryRow:
					if len(rows) != 1 {
						t.Fatalf("builder player rows = %d", len(rows))
					}
					row := rows[0]
					if row.LocationID != locationID ||
						row.BuilderBaseTrophies != player.BuilderBaseTrophies ||
						row.BuilderBaseBattleWins == nil || *row.BuilderBaseBattleWins != player.VersusAttackWins ||
						row.LeagueID == nil || *row.LeagueID != player.BuilderBaseLeague.ID ||
						row.ClanBadgeToken == nil || *row.ClanBadgeToken != "player-clan-token" {
						t.Fatalf("builder player typed row = %#v", row)
					}
				case []models.ClanTrophyHistoryRow:
					if len(rows) != 1 {
						t.Fatalf("home clan rows = %d", len(rows))
					}
					row := rows[0]
					if row.LocationID != locationID ||
						row.ClanTag != clan.Tag ||
						row.ClanName != clan.Name ||
						row.ClanBadgeToken != "clan-token" ||
						row.ClanLevel != clan.Level ||
						row.ClanPoints != clan.Points ||
						row.Members != clan.MemberCount ||
						row.ClanLocationID == nil || *row.ClanLocationID != clan.Location.ID ||
						row.PreviousRank == nil || *row.PreviousRank != clan.PreviousRank {
						t.Fatalf("home clan typed row = %#v", row)
					}
				case []models.ClanBuilderBaseTrophyHistoryRow:
					if len(rows) != 1 || rows[0].BuilderBasePoints != clan.BuilderBasePoints {
						t.Fatalf("builder clan typed rows = %#v", rows)
					}
				case []models.ClanCapitalHistoryRow:
					if len(rows) != 1 || rows[0].CapitalPoints != clan.CapitalPoints {
						t.Fatalf("capital clan typed rows = %#v", rows)
					}
				default:
					t.Fatalf("unexpected typed rows %T", group.Rows)
				}
			})
		}
	}

	legacy := player
	legacy.LeagueTier = clashy.League{}
	legacy.BuilderBaseLeague = nil
	legacy.VersusAttackWins = 0
	homeRow, err := playerTrophyHistoryRow("global", dayStart(date), legacy)
	if err != nil {
		t.Fatal(err)
	}
	if homeRow.LeagueID == nil || *homeRow.LeagueID != legacy.League.ID {
		t.Fatalf("legacy home league fallback = %#v", homeRow.LeagueID)
	}
	builderRow, err := playerBuilderBaseTrophyHistoryRow("global", dayStart(date), legacy)
	if err != nil {
		t.Fatal(err)
	}
	if builderRow.LeagueID != nil || builderRow.BuilderBaseBattleWins != nil {
		t.Fatalf("nullable builder fields = %#v", builderRow)
	}
	if _, hasData := reflect.TypeOf(models.PlayerTrophyHistoryRow{}).FieldByName("Data"); hasData {
		t.Fatal("typed leaderboard row retains removed JSON data field")
	}
}

func TestMemoryScheduledStoreAuthoritativelyUpsertsLeaderboardHistory(t *testing.T) {
	store := newMemoryScheduledStore()
	date := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	initial, err := leaderboardHistoryGroupFromResponse(
		leaderboardHistoryClanHomePoints,
		"global",
		date,
		[]clashy.RankedClan{
			{Clan: historyClanForTest("#A", "A", "a-token", 100), Rank: 1},
			{Clan: historyClanForTest("#B", "B", "b-token", 90), Rank: 2},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	other, err := leaderboardHistoryGroupFromResponse(
		leaderboardHistoryPlayerBuilderBaseTrophies,
		"32000006",
		date,
		[]clashy.RankedPlayer{{
			Player: clashy.Player{Tag: "#P", Name: "Player", BuilderBaseTrophies: 6000},
			Rank:   1,
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReplaceLeaderboardHistory(t.Context(), []leaderboardHistoryGroup{initial, other}); err != nil {
		t.Fatal(err)
	}

	replacement, err := leaderboardHistoryGroupFromResponse(
		leaderboardHistoryClanHomePoints,
		"global",
		date,
		[]clashy.RankedClan{
			{Clan: historyClanForTest("#B", "B updated", "b-token", 110), Rank: 1},
			{Clan: historyClanForTest("#C", "C", "c-token", 100), Rank: 2},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReplaceLeaderboardHistory(t.Context(), []leaderboardHistoryGroup{replacement}); err != nil {
		t.Fatal(err)
	}
	key := leaderboardHistoryGroupKey(leaderboardHistoryClanHomePoints, "global", date)
	got := store.leaderboardHistory[key]
	gotB, okB := got["#B"].(models.ClanTrophyHistoryRow)
	gotC, okC := got["#C"].(models.ClanTrophyHistoryRow)
	if len(got) != 2 || !okB || !okC || gotB.ClanName != "B updated" || gotC.Rank != 2 {
		t.Fatalf("authoritative group replacement = %#v", got)
	}
	if _, exists := got["#A"]; exists {
		t.Fatalf("stale history row survived replacement: %#v", got)
	}
	otherKey := leaderboardHistoryGroupKey(leaderboardHistoryPlayerBuilderBaseTrophies, "32000006", date)
	gotOther := store.leaderboardHistory[otherKey]
	otherRow, otherOK := gotOther["#P"].(models.PlayerBuilderBaseTrophyHistoryRow)
	if len(gotOther) != 1 || !otherOK || otherRow.Rank != 1 {
		t.Fatalf("replacement touched another group: %#v", gotOther)
	}

	if _, err := store.ReplaceLeaderboardHistory(t.Context(), []leaderboardHistoryGroup{replacement}); err != nil {
		t.Fatal(err)
	}
	if got := store.leaderboardHistory[key]; len(got) != 2 {
		t.Fatalf("idempotent upsert duplicated rows: %#v", got)
	}

	empty, err := leaderboardHistoryGroupFromResponse(
		leaderboardHistoryClanHomePoints,
		"global",
		date,
		[]clashy.RankedClan{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReplaceLeaderboardHistory(t.Context(), []leaderboardHistoryGroup{empty}); err != nil {
		t.Fatal(err)
	}
	if got := store.leaderboardHistory[key]; len(got) != 0 {
		t.Fatalf("authoritative empty group retained rows: %#v", got)
	}
	if gotOther := store.leaderboardHistory[otherKey]; len(gotOther) != 1 {
		t.Fatalf("empty replacement touched another group: %#v", gotOther)
	}
}

func TestTypedLeaderboardHistorySQLScopesEveryTableReplacement(t *testing.T) {
	wantColumns := map[string][]string{
		"leaderboard_history_player_home": {
			"location_id", "date", "player_tag", "player_name", "exp_level",
			"trophies", "attack_wins", "defense_wins", "rank", "previous_rank",
			"clan_tag", "clan_name", "clan_badge_token", "league_id",
		},
		"leaderboard_history_player_builder_base": {
			"location_id", "date", "player_tag", "player_name", "exp_level",
			"builder_base_trophies", "builder_base_battle_wins", "rank", "previous_rank",
			"clan_tag", "clan_name", "clan_badge_token", "league_id",
		},
		"leaderboard_history_clan_home": {
			"location_id", "date", "clan_tag", "clan_name", "clan_badge_token",
			"clan_level", "clan_points", "members", "clan_location_id", "rank", "previous_rank",
		},
		"leaderboard_history_clan_builder_base": {
			"location_id", "date", "clan_tag", "clan_name", "clan_badge_token",
			"clan_level", "builder_base_points", "members", "clan_location_id", "rank", "previous_rank",
		},
		"leaderboard_history_clan_capital": {
			"location_id", "date", "clan_tag", "clan_name", "clan_badge_token",
			"clan_level", "capital_points", "members", "clan_location_id", "rank", "previous_rank",
		},
	}
	seenTables := make(map[string]struct{}, len(typedLeaderboardHistorySpecs))
	for _, spec := range typedLeaderboardHistorySpecs {
		expected, exists := wantColumns[spec.Table]
		if !exists {
			t.Fatalf("unexpected typed leaderboard table %q", spec.Table)
		}
		if !reflect.DeepEqual(spec.Columns, expected) {
			t.Fatalf("%s columns = %#v, want exact schema %#v", spec.Table, spec.Columns, expected)
		}
		seenTables[spec.Table] = struct{}{}
		upsert := typedLeaderboardHistoryUpsertSQL(spec)
		deleteSQL := typedLeaderboardHistoryDeleteSQL(spec)
		if !strings.Contains(upsert, `INSERT INTO "`+spec.Table+`"`) ||
			!strings.Contains(upsert, "ON CONFLICT (location_id, date, \""+spec.TagColumn+"\")") {
			t.Fatalf("%s upsert has wrong table/PK: %s", spec.Table, upsert)
		}
		for _, predicate := range []string{
			"current.location_id = groups.location_id",
			"current.date = groups.date",
			"stage.location_id = current.location_id",
			"stage.date = current.date",
			"stage.\"" + spec.TagColumn + "\" = current.\"" + spec.TagColumn + "\"",
		} {
			if !strings.Contains(deleteSQL, predicate) {
				t.Fatalf("%s delete missing %q: %s", spec.Table, predicate, deleteSQL)
			}
		}
		combined := strings.ToLower(upsert + deleteSQL + strings.Join(spec.Columns, " "))
		for _, obsolete := range []string{
			`insert into "leaderboard_history"`,
			`from "leaderboard_history"`,
			"leaderboard_snapshot_items",
			" jsonb",
			" data",
			" kind",
		} {
			if strings.Contains(combined, obsolete) {
				t.Fatalf("%s SQL/columns retain %q: %s", spec.Table, obsolete, combined)
			}
		}
	}
	if len(seenTables) != len(wantColumns) {
		t.Fatalf("typed leaderboard tables = %#v, want all %#v", seenTables, wantColumns)
	}
}

func TestValidateAndFlattenLeaderboardHistoryGroupsRejectsLegacyOrDuplicateRows(t *testing.T) {
	date := time.Date(2026, 7, 26, 0, 0, 0, 0, time.UTC)
	row := models.ClanTrophyHistoryRow{
		LocationID:     "global",
		Date:           date,
		ClanTag:        "#A",
		ClanName:       "A",
		ClanBadgeToken: "a-token",
		ClanLevel:      1,
		ClanPoints:     100,
		Members:        1,
		Rank:           1,
	}
	if _, err := validateAndFlattenLeaderboardHistoryGroups([]leaderboardHistoryGroup{{
		Kind:       "clan_trophies",
		LocationID: "global",
		Date:       date,
	}}); err == nil {
		t.Fatal("legacy history kind was accepted for storage")
	}
	if _, err := validateAndFlattenLeaderboardHistoryGroups([]leaderboardHistoryGroup{{
		Kind:       leaderboardHistoryClanHomePoints,
		LocationID: "global",
		Date:       date,
		Rows:       []models.ClanTrophyHistoryRow{row, row},
	}}); err == nil {
		t.Fatal("duplicate history tag was accepted for one group")
	}
}

func TestCapitalHistoryUsesOfficialTuesdayDateWithoutImporterRemap(t *testing.T) {
	tuesday := time.Date(2026, 7, 28, 15, 0, 0, 0, time.UTC)
	if !shouldStoreLeaderboardHistoryKind(leaderboardHistoryClanCapitalPoints, tuesday) {
		t.Fatal("capital history was not scheduled on Tuesday")
	}
	if shouldStoreLeaderboardHistoryKind(leaderboardHistoryClanCapitalPoints, tuesday.AddDate(0, 0, -1)) {
		t.Fatal("capital history was scheduled outside Tuesday")
	}
	for _, kind := range []string{
		leaderboardHistoryPlayerHomeTrophies,
		leaderboardHistoryPlayerBuilderBaseTrophies,
		leaderboardHistoryClanHomePoints,
		leaderboardHistoryClanBuilderBasePoints,
	} {
		if !shouldStoreLeaderboardHistoryKind(kind, tuesday.AddDate(0, 0, -1)) {
			t.Fatalf("daily typed history %s was incorrectly skipped", kind)
		}
	}
	group, err := leaderboardHistoryGroupFromResponse(
		leaderboardHistoryClanCapitalPoints,
		"global",
		tuesday,
		[]clashy.RankedClan{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !group.Date.Equal(dayStart(tuesday)) {
		t.Fatalf("capital history date = %s, want official Tuesday %s", group.Date, dayStart(tuesday))
	}
}

func TestMissingCompletedLegendSeasonsUsesExactOfficialIDs(t *testing.T) {
	completedSeason := "2026-05"
	window, err := officialLegendSeasonWindow(completedSeason)
	if err != nil {
		t.Fatal(err)
	}
	v2Season := "v2-2026-07-06T05:00:00Z"
	v2Window, err := officialLegendSeasonWindow(v2Season)
	if err != nil {
		t.Fatal(err)
	}
	wantV2End := time.Date(2026, 7, 6, 5, 0, 0, 0, time.UTC)
	if v2Window.SeasonID != v2Season || !v2Window.EndTime.Equal(wantV2End) {
		t.Fatalf("v2 season window = %#v, want exact ID and end %s", v2Window, wantV2End)
	}
	missing, err := missingCompletedLegendSeasons(
		[]string{"2026-04", completedSeason, v2Season, v2Season, "v2-2026-08-03T05:00:00Z"},
		map[string]struct{}{completedSeason: {}},
		wantV2End.Add(time.Hour),
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"2026-04", v2Season}; !reflect.DeepEqual(missing, want) {
		t.Fatalf("missing completed legend seasons = %#v, want %#v", missing, want)
	}
	atBoundary, err := missingCompletedLegendSeasons(
		[]string{v2Season},
		nil,
		wantV2End,
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{v2Season}; !reflect.DeepEqual(atBoundary, want) {
		t.Fatalf("season at its embedded end = %#v, want completed %#v", atBoundary, want)
	}
	for _, invalid := range []string{"2026-5", "v2-2026-07-06T05:00:00+00:00", "v2-not-a-time"} {
		if _, err := missingCompletedLegendSeasons(
			[]string{invalid},
			nil,
			window.EndTime.Add(time.Hour),
		); err == nil {
			t.Fatalf("noncanonical official season ID %q was accepted", invalid)
		}
	}
}

func TestLegendHistoryRowsStoreNormalizedTypedFieldsBeyondAPIReadCap(t *testing.T) {
	season := "v2-2026-07-06T05:00:00Z"
	rankings := legendRankingsForTest(300)
	rankings[0].Player.Clan = nil
	rankings[0].Player.LeagueTier = clashy.League{}
	rankings[299].Player.LeagueTier = clashy.League{ID: 29000027, Name: "Legend I"}
	rows, err := legendHistoryRows(season, rankings)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 300 {
		t.Fatalf("legend history rows = %d, want 300", len(rows))
	}
	row := rows[299]
	if row.Season != season ||
		row.PlayerTag != rankings[299].Player.Tag ||
		row.PlayerName != rankings[299].Player.Name ||
		row.ExpLevel != rankings[299].Player.ExpLevel ||
		row.Rank != rankings[299].Player.Rank ||
		row.Trophies != rankings[299].Player.Trophies ||
		row.AttackWins != rankings[299].Player.AttackWins ||
		row.DefenseWins != rankings[299].Player.DefenseWins {
		t.Fatalf("typed legend row = %#v", row)
	}
	if row.ClanTag == nil || *row.ClanTag != "#LEGEND" ||
		row.ClanName == nil || *row.ClanName != "Legend Clan" ||
		row.ClanBadgeToken == nil || *row.ClanBadgeToken != "legend-clan-token" {
		t.Fatalf("typed legend clan = %#v", row)
	}
	if row.LeagueTierID == nil || *row.LeagueTierID != 29000027 {
		t.Fatalf("league tier ID = %#v, want 29000027", row.LeagueTierID)
	}
	legacyRow := rows[0]
	if legacyRow.ClanTag != nil || legacyRow.ClanName != nil || legacyRow.ClanBadgeToken != nil {
		t.Fatalf("clanless legacy row stored clan values: %#v", legacyRow)
	}
	if legacyRow.LeagueTierID != nil {
		t.Fatalf("legacy no-tier row stored league tier: %#v", legacyRow.LeagueTierID)
	}
	legacyRows, err := legendHistoryRows("2026-05", rankings[:1])
	if err != nil {
		t.Fatal(err)
	}
	if len(legacyRows) != 1 || legacyRows[0].Season != "2026-05" || legacyRows[0].LeagueTierID != nil {
		t.Fatalf("legacy season/no-tier row = %#v", legacyRows)
	}
	if _, hasData := reflect.TypeOf(models.LegendHistoryRow{}).FieldByName("Data"); hasData {
		t.Fatal("LegendHistoryRow retains removed JSON data field")
	}

	endpoint, err := url.Parse(legendSeasonRankingPageURL("https://proxy.example/v1/", season, ""))
	if err != nil {
		t.Fatal(err)
	}
	wantPath := "/v1/leagues/29000022/seasons/" + season
	if endpoint.Path != wantPath {
		t.Fatalf("v2 season fetch path = %q, want opaque ID unchanged in %q", endpoint.Path, wantPath)
	}
}

func TestFetchAllLegendSeasonRankingPagesPaginatesToExhaustion(t *testing.T) {
	rankings := legendRankingsForTest(300)
	requests := make([]string, 0, 2)
	got, err := collectLegendSeasonRankingPages(
		"2026-05",
		func(after string) ([]legendRankingItem, string, error) {
			requests = append(requests, after)
			switch after {
			case "":
				return rankings[:150], "next-page", nil
			case "next-page":
				return rankings[150:], "", nil
			default:
				return nil, "", errors.New("unexpected cursor")
			}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(rankings) || !reflect.DeepEqual(requests, []string{"", "next-page"}) {
		t.Fatalf("paginated rankings = %d across cursors %#v, want %d across two pages", len(got), requests, len(rankings))
	}

	endpoint, err := url.Parse(legendSeasonRankingPageURL("https://proxy.example/v1/", "2026-05", "next-page"))
	if err != nil {
		t.Fatal(err)
	}
	if endpoint.Path != "/v1/leagues/29000022/seasons/2026-05" {
		t.Fatalf("legend season endpoint path = %s", endpoint.Path)
	}
	if got := endpoint.Query().Get("limit"); got != strconv.Itoa(legendSeasonPageSize) {
		t.Fatalf("legend season request limit = %q, want %d", got, legendSeasonPageSize)
	}
	if got := endpoint.Query().Get("after"); got != "next-page" {
		t.Fatalf("legend season request cursor = %q", got)
	}

	if _, err := collectLegendSeasonRankingPages(
		"2026-05",
		func(string) ([]legendRankingItem, string, error) {
			return nil, "still-more", nil
		},
	); err == nil {
		t.Fatal("empty partial page with continuation cursor was accepted")
	}
	if _, err := collectLegendSeasonRankingPages(
		"2026-05",
		func(string) ([]legendRankingItem, string, error) {
			return rankings[:1], "same-cursor", nil
		},
	); err == nil {
		t.Fatal("repeated continuation cursor was accepted")
	}
}

func TestMemoryScheduledStoreLegendSeasonReplacementIsIdempotent(t *testing.T) {
	store := newMemoryScheduledStore()
	rows, err := legendHistoryRows("2026-05", legendRankingsForTest(300))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReplaceLegendSeason(t.Context(), "2026-05", rows); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReplaceLegendSeason(t.Context(), "2026-05", rows); err != nil {
		t.Fatal(err)
	}
	if got := store.legendHistory["2026-05"]; len(got) != 300 {
		t.Fatalf("idempotent legend replacement stored %d rows, want 300", len(got))
	}
	completed, err := store.CompletedLegendSeasons(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := completed["2026-05"]; !exists {
		t.Fatalf("complete legend season was not detected: %#v", completed)
	}

	store.legendHistory["2026-04"] = map[string]models.LegendHistoryRow{
		"#PARTIAL": {
			Season:     "2026-04",
			PlayerTag:  "#PARTIAL",
			PlayerName: "Partial",
			ExpLevel:   100,
			Rank:       2,
			Trophies:   6000,
		},
	}
	completed, err = store.CompletedLegendSeasons(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := completed["2026-04"]; exists {
		t.Fatalf("partial legend season was marked complete: %#v", completed)
	}
}

func TestLegendHistoryPartialFailureRemainsMissingAndRetries(t *testing.T) {
	store := newMemoryScheduledStore()
	window, err := officialLegendSeasonWindow("2026-05")
	if err != nil {
		t.Fatal(err)
	}
	attempts := 0
	domain := &scheduledDomain{
		store: store,
		loadLegendSeasons: func(context.Context, *platform.App) ([]string, error) {
			return []string{"2026-05"}, nil
		},
		loadLegendRankings: func(context.Context, *platform.App, string) ([]legendRankingItem, error) {
			attempts++
			if attempts == 1 {
				return nil, errors.New("partial page failed")
			}
			return legendRankingsForTest(300), nil
		},
	}
	app := &platform.App{}
	if writes, err := domain.doLegendHistory(t.Context(), app, window.EndTime.Add(time.Hour)); err == nil || writes != 0 {
		t.Fatalf("partial fetch result = writes %d err %v, want 0/error", writes, err)
	}
	if len(store.legendHistory) != 0 {
		t.Fatalf("partial fetch persisted legend rows: %#v", store.legendHistory)
	}
	writes, err := domain.doLegendHistory(t.Context(), app, window.EndTime.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if writes != 300 || attempts != 2 || len(store.legendHistory["2026-05"]) != 300 {
		t.Fatalf("retry result = writes %d attempts %d stored %d", writes, attempts, len(store.legendHistory["2026-05"]))
	}
}

func TestLegendHistorySQLUsesFinalTableAndTransactionalSeasonReplacement(t *testing.T) {
	wantColumns := []string{
		"season",
		"player_tag",
		"player_name",
		"exp_level",
		"trophies",
		"attack_wins",
		"defense_wins",
		"rank",
		"clan_tag",
		"clan_name",
		"clan_badge_token",
		"league_tier_id",
	}
	if !reflect.DeepEqual(legendHistoryColumns, wantColumns) {
		t.Fatalf("legend CopyFrom columns = %#v, want exact typed schema %#v", legendHistoryColumns, wantColumns)
	}
	if !strings.Contains(upsertLegendHistorySQL, "INSERT INTO legend_history") ||
		!strings.Contains(upsertLegendHistorySQL, "ON CONFLICT (season, player_tag)") {
		t.Fatalf("legend upsert does not use final table and PK: %s", upsertLegendHistorySQL)
	}
	for _, predicate := range []string{
		"current.season = $1",
		"stage.season = current.season",
		"stage.player_tag = current.player_tag",
	} {
		if !strings.Contains(deleteStaleLegendHistorySQL, predicate) {
			t.Fatalf("legend replacement missing %q: %s", predicate, deleteStaleLegendHistorySQL)
		}
	}
	for _, query := range []string{upsertLegendHistorySQL, deleteStaleLegendHistorySQL} {
		if strings.Contains(query, "legend_history_snapshots") ||
			strings.Contains(query, "created_at") ||
			strings.Contains(strings.ToLower(query), "data") ||
			strings.Contains(strings.ToLower(query), "jsonb") {
			t.Fatalf("legend SQL retains obsolete schema: %s", query)
		}
	}
	for _, column := range []string{
		"player_name",
		"exp_level",
		"attack_wins",
		"defense_wins",
		"clan_tag",
		"clan_name",
		"clan_badge_token",
		"league_tier_id",
	} {
		if !strings.Contains(upsertLegendHistorySQL, column) {
			t.Fatalf("legend upsert omits typed column %q: %s", column, upsertLegendHistorySQL)
		}
	}
}

func legendRankingsForTest(count int) []legendRankingItem {
	rankings := make([]legendRankingItem, 0, count)
	for rank := 1; rank <= count; rank++ {
		player := clashy.RankedPlayer{
			Player: clashy.Player{
				Tag:      "#P" + strconv.Itoa(100000+rank),
				Name:     "Player " + strconv.Itoa(rank),
				ExpLevel: 200 + rank,
				Trophies: 6000 + rank,
				Clan: &clashy.PlayerClan{
					Tag:   "#LEGEND",
					Name:  "Legend Clan",
					Level: 25,
					Badge: clashy.Badge{
						Small:  "https://cdn.example/70/legend-clan-token.png",
						Medium: "https://cdn.example/200/legend-clan-token.png",
						Large:  "https://cdn.example/512/legend-clan-token.png",
					},
				},
			},
			League:       clashy.League{ID: legendLeagueID, Name: "Legend League"},
			AttackWins:   rank % 10,
			DefenseWins:  rank % 7,
			Rank:         rank,
			PreviousRank: rank + 1,
		}
		rankings = append(rankings, legendRankingItem{Player: player})
	}
	return rankings
}

func historyBadgeForTest(token string) clashy.Badge {
	return clashy.Badge{
		Small:  "https://cdn.example/70/" + token + ".png",
		Medium: "https://cdn.example/200/" + token + ".png",
		Large:  "https://cdn.example/512/" + token + ".png",
	}
}

func historyClanForTest(tag, name, token string, points int) clashy.Clan {
	return clashy.Clan{
		Tag:         tag,
		Name:        name,
		Level:       1,
		Points:      points,
		MemberCount: 1,
		Badge:       historyBadgeForTest(token),
	}
}
