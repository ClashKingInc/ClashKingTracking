package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestBuildLeaderboardCacheRanksFreshPlayersAndExcludesUnrankedLeague(t *testing.T) {
	now := time.Date(2026, 6, 24, 12, 0, 0, 0, time.UTC)
	nullAssetURL := "https://api-assets.clashofclans.com/null"
	candidates := []leaderboardCandidate{
		{BoardType: "league", BoardKey: "29000022", Tag: "#A"},
		{BoardType: "league", BoardKey: "105000000", Tag: "#B"},
		{BoardType: "townhall", BoardKey: "16", Tag: "#A"},
		{BoardType: "townhall", BoardKey: "6", Tag: "#D"},
	}
	players := []leaderboardPlayerRow{
		{BasicPlayerRow: models.BasicPlayerRow{Tag: "#B", Name: "B", LeagueID: unrankedPlayerLeagueID, TownHall: 16, Trophies: 7000}},
		{
			BasicPlayerRow: models.BasicPlayerRow{Tag: "#A", Name: "A", LeagueID: 29000022, ClanTag: "#CLAN", TownHall: 16, Trophies: 6500},
			League:         leaderboardLeaguePayload{ID: 29000022, Name: "Legend League", Badge: "legend.png"},
		},
		{
			BasicPlayerRow: models.BasicPlayerRow{Tag: "#C", Name: "C", LeagueID: 29000022, TownHall: 16, Trophies: 6600},
			League:         leaderboardLeaguePayload{ID: 29000022, Name: "Legend League", Badge: "legend.png"},
		},
		{
			BasicPlayerRow: models.BasicPlayerRow{Tag: "#D", Name: "D", LeagueID: 29000023, TownHall: 16, Trophies: 1000},
			League:         leaderboardLeaguePayload{ID: 29000023, Name: "Higher League", Badge: "higher.png"},
		},
	}
	clans := map[string]leaderboardClanMetadata{
		"#CLAN": {Name: "Clan", BadgeURL: "clan.png"},
	}

	cache := buildLeaderboardCache(candidates, players, clans, now, 500, nullAssetURL)
	for _, league := range cache.Leagues {
		if league == "105000000" {
			t.Fatalf("league index should exclude unranked league: %#v", cache.Leagues)
		}
	}
	board := findLeaderboardBoard(t, cache.Boards, "league", "29000022")
	if len(board.Items) != 2 {
		t.Fatalf("league board items = %d, want 2", len(board.Items))
	}
	if board.Items[0].Tag != "#C" || board.Items[0].Rank != 1 || board.Items[1].Tag != "#A" || board.Items[1].Rank != 2 {
		t.Fatalf("unexpected ranking: %#v", board.Items)
	}
	if board.Items[1].Clan == nil || board.Items[1].Clan.Tag != "#CLAN" ||
		board.Items[1].Clan.Name == nil || *board.Items[1].Clan.Name != "Clan" ||
		board.Items[1].Clan.Badge != "clan.png" {
		t.Fatalf("expected joined clan metadata: %#v", board.Items[1])
	}
	if board.Items[0].Clan != nil {
		t.Fatalf("expected missing clan fallback: %#v", board.Items[0])
	}
	if board.Items[1].League.ID != 29000022 || board.Items[1].League.Name != "Legend League" || board.Items[1].League.Badge != "legend.png" {
		t.Fatalf("expected nested league metadata: %#v", board.Items[1].League)
	}
	townhallBoard := findLeaderboardBoard(t, cache.Boards, "townhall", "16")
	if len(townhallBoard.Items) != 3 {
		t.Fatalf("townhall board items = %d, want 3", len(townhallBoard.Items))
	}
	if townhallBoard.Items[0].Tag != "#D" || townhallBoard.Items[1].Tag != "#C" || townhallBoard.Items[2].Tag != "#A" {
		t.Fatalf("townhall board should sort by league_id desc, then trophies desc: %#v", townhallBoard.Items)
	}
	raw, err := json.Marshal(board)
	if err != nil {
		t.Fatalf("marshal board: %v", err)
	}
	payload := string(raw)
	if strings.Contains(payload, `"type"`) || strings.Contains(payload, `"key"`) || !strings.Contains(payload, `"items"`) {
		t.Fatalf("unexpected board payload shape: %s", payload)
	}
	if strings.Index(payload, `"generated_at"`) < strings.Index(payload, `"items"`) {
		t.Fatalf("generated_at should be after items: %s", payload)
	}
	for _, board := range cache.Boards {
		if board.Kind == "league" && board.Key == "105000000" {
			t.Fatalf("unranked league should not be cached: %#v", board)
		}
		if board.Kind == "townhall" && board.Key == "6" {
			t.Fatalf("townhall boards should start at TH7: %#v", board)
		}
		if board.Kind == "townhall" {
			for _, item := range board.Items {
				if item.League.ID == unrankedPlayerLeagueID {
					t.Fatalf("townhall board should exclude unranked players: %#v", item)
				}
			}
		}
	}
}

func TestLeaguePayloadForPlayerUsesLeagueTierMetadata(t *testing.T) {
	playerLeague := leaderboardLeaguePayload{
		ID:    105000001,
		Name:  "Skeleton League 1",
		Badge: "tier.png",
	}

	payload := leaguePayloadForPlayer(clashy.League{
		ID:   105000001,
		Name: "stale-player-name",
	}, map[int]leaderboardLeaguePayload{105000001: playerLeague})

	if payload != playerLeague {
		t.Fatalf("expected leaguetiers payload: %#v", payload)
	}
}

func TestLeaderboardMaterializedViewRefreshSet(t *testing.T) {
	got := strings.Join(leaderboardMaterializedViewRefreshQueries[:], "\n")
	want := strings.Join([]string{
		`REFRESH MATERIALIZED VIEW CONCURRENTLY clan_leaderboards`,
		`REFRESH MATERIALIZED VIEW war_league_counts`,
		`REFRESH MATERIALIZED VIEW CONCURRENTLY townhall_counts`,
	}, "\n")

	if got != want {
		t.Fatalf("materialized view refresh queries:\n%s\nwant:\n%s", got, want)
	}
	if leaderboardMaterializedViewCount != len(leaderboardMaterializedViewRefreshQueries) {
		t.Fatalf(
			"materialized view metric count = %d, queries = %d",
			leaderboardMaterializedViewCount,
			len(leaderboardMaterializedViewRefreshQueries),
		)
	}
	if len(leaderboardMaterializedViewBootstrapQueries) != len(leaderboardMaterializedViewRefreshQueries) {
		t.Fatalf(
			"materialized view bootstrap queries = %d, refresh queries = %d",
			len(leaderboardMaterializedViewBootstrapQueries),
			len(leaderboardMaterializedViewRefreshQueries),
		)
	}
	for i, query := range leaderboardMaterializedViewBootstrapQueries {
		if strings.Contains(query, "CONCURRENTLY") {
			t.Fatalf("bootstrap query %d uses CONCURRENTLY: %s", i, query)
		}
	}
}

func TestUnpopulatedMaterializedViewErrorIsMatchedNarrowly(t *testing.T) {
	message := "CONCURRENTLY cannot be used when the materialized view is not populated"
	for _, code := range []string{"0A000", "55000"} {
		if !isUnpopulatedMaterializedViewError(&pgconn.PgError{Code: code, Message: message}) {
			t.Fatalf("unpopulated materialized view error %s was not recognized", code)
		}
	}
	if isUnpopulatedMaterializedViewError(&pgconn.PgError{Code: "0A000", Message: "another unsupported operation"}) {
		t.Fatal("unrelated feature-not-supported error was accepted as an empty materialized view")
	}
}

func TestLeaderboardMaterializedViewRefreshFailureKeepsDeadlineDue(t *testing.T) {
	now := time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC)
	refreshErr := errors.New("townhall refresh failed")
	attempts := 0
	fail := true
	domain := &leaderboardsDomain{
		refreshMaterializedViews: func(context.Context) error {
			attempts++
			if fail {
				return refreshErr
			}
			return nil
		},
	}
	app := &platform.App{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Stats:  platform.NewTracker(),
	}

	err := domain.refreshMaterializedViewsIfDue(context.Background(), app, now)
	if !errors.Is(err, refreshErr) {
		t.Fatalf("first refresh error = %v, want %v", err, refreshErr)
	}
	if !domain.nextMaterializedViewRefresh.IsZero() {
		t.Fatalf("failed refresh advanced deadline to %s", domain.nextMaterializedViewRefresh)
	}

	fail = false
	retryAt := now.Add(600 * time.Second)
	if err := domain.refreshMaterializedViewsIfDue(context.Background(), app, retryAt); err != nil {
		t.Fatalf("retry refresh: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("refresh attempts = %d, want 2", attempts)
	}
	wantDeadline := retryAt.Add(leaderboardMaterializedViewRefreshSeconds * time.Second)
	if !domain.nextMaterializedViewRefresh.Equal(wantDeadline) {
		t.Fatalf("successful refresh deadline = %s, want %s", domain.nextMaterializedViewRefresh, wantDeadline)
	}

	stats := app.Stats.Domain(leaderboardsDomainName)
	if stats.StoreBatches != 1 ||
		stats.StoreRowsRequested != leaderboardMaterializedViewCount ||
		stats.StoreRowsAffected != leaderboardMaterializedViewCount {
		t.Fatalf("refresh store metrics = %#v", stats)
	}

	if err := domain.refreshMaterializedViewsIfDue(context.Background(), app, retryAt.Add(600*time.Second)); err != nil {
		t.Fatalf("refresh before hourly deadline: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("refresh ran before hourly deadline: attempts = %d", attempts)
	}
}

func findLeaderboardBoard(t *testing.T, boards []leaderboardBoardPayload, kind, key string) leaderboardBoardPayload {
	t.Helper()
	for _, board := range boards {
		if board.Kind == kind && board.Key == key {
			return board
		}
	}
	t.Fatalf("missing board %s/%s in %#v", kind, key, boards)
	return leaderboardBoardPayload{}
}
