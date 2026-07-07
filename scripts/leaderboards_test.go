package scripts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
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
