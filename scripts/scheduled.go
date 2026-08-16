package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	scheduledDomainName = "scheduled"

	leaderboardHistoryPlayerHomeTrophies        = "player_home_trophies"
	leaderboardHistoryPlayerBuilderBaseTrophies = "player_builder_base_trophies"
	leaderboardHistoryClanHomePoints            = "clan_home_points"
	leaderboardHistoryClanBuilderBasePoints     = "clan_builder_base_points"
	leaderboardHistoryClanCapitalPoints         = "clan_capital_points"

	legendLeagueID       = 29000022
	legendSeasonPageSize = 25000
	legendSeasonV2Prefix = "v2-"
	legendSeasonV2Length = 28 * 24 * time.Hour

	currentClanRankingLimit = 200
)

type leaderboardLoader func(context.Context, *clashy.Client, string) (any, error)
type currentClanRankingLoader func(context.Context, *clashy.Client, string, clashy.PageOptions) ([]clashy.RankedClan, error)
type legendSeasonsLoader func(context.Context, *platform.App) ([]string, error)
type legendSeasonRankingsLoader func(context.Context, *platform.App, string) ([]legendRankingItem, error)

var leaderboardHistoryPaths = []struct {
	Kind string
	Load leaderboardLoader
}{
	{Kind: leaderboardHistoryPlayerHomeTrophies, Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationPlayersByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
	{Kind: leaderboardHistoryPlayerBuilderBaseTrophies, Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationPlayersBuilderBaseByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
	{Kind: leaderboardHistoryClanHomePoints, Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationClansByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
	{Kind: leaderboardHistoryClanBuilderBasePoints, Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationClansBuilderBaseByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
	{Kind: leaderboardHistoryClanCapitalPoints, Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationClansCapitalByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
}

var typedLeaderboardHistorySpecs = []typedLeaderboardHistorySpec{
	{
		Kind:      leaderboardHistoryPlayerHomeTrophies,
		Table:     "leaderboard_history_player_home",
		TagColumn: "player_tag",
		Columns: []string{
			"location_id", "date", "player_tag", "player_name", "exp_level",
			"trophies", "attack_wins", "defense_wins", "rank", "previous_rank",
			"clan_tag", "clan_name", "clan_badge_token", "league_id",
		},
		UpdateCols: []string{
			"player_name", "exp_level", "trophies", "attack_wins", "defense_wins",
			"rank", "previous_rank", "clan_tag", "clan_name", "clan_badge_token", "league_id",
		},
	},
	{
		Kind:      leaderboardHistoryPlayerBuilderBaseTrophies,
		Table:     "leaderboard_history_player_builder_base",
		TagColumn: "player_tag",
		Columns: []string{
			"location_id", "date", "player_tag", "player_name", "exp_level",
			"builder_base_trophies", "builder_base_battle_wins", "rank", "previous_rank",
			"clan_tag", "clan_name", "clan_badge_token", "league_id",
		},
		UpdateCols: []string{
			"player_name", "exp_level", "builder_base_trophies", "builder_base_battle_wins",
			"rank", "previous_rank", "clan_tag", "clan_name", "clan_badge_token", "league_id",
		},
	},
	{
		Kind:      leaderboardHistoryClanHomePoints,
		Table:     "leaderboard_history_clan_home",
		TagColumn: "clan_tag",
		Columns: []string{
			"location_id", "date", "clan_tag", "clan_name", "clan_badge_token",
			"clan_level", "clan_points", "members", "clan_location_id", "rank", "previous_rank",
		},
		UpdateCols: []string{
			"clan_name", "clan_badge_token", "clan_level", "clan_points",
			"members", "clan_location_id", "rank", "previous_rank",
		},
	},
	{
		Kind:      leaderboardHistoryClanBuilderBasePoints,
		Table:     "leaderboard_history_clan_builder_base",
		TagColumn: "clan_tag",
		Columns: []string{
			"location_id", "date", "clan_tag", "clan_name", "clan_badge_token",
			"clan_level", "builder_base_points", "members", "clan_location_id", "rank", "previous_rank",
		},
		UpdateCols: []string{
			"clan_name", "clan_badge_token", "clan_level", "builder_base_points",
			"members", "clan_location_id", "rank", "previous_rank",
		},
	},
	{
		Kind:      leaderboardHistoryClanCapitalPoints,
		Table:     "leaderboard_history_clan_capital",
		TagColumn: "clan_tag",
		Columns: []string{
			"location_id", "date", "clan_tag", "clan_name", "clan_badge_token",
			"clan_level", "capital_points", "members", "clan_location_id", "rank", "previous_rank",
		},
		UpdateCols: []string{
			"clan_name", "clan_badge_token", "clan_level", "capital_points",
			"members", "clan_location_id", "rank", "previous_rank",
		},
	},
}

var currentClanRankingPaths = []struct {
	RankingType string
	Load        currentClanRankingLoader
	Points      func(clashy.RankedClan) int
}{
	{
		RankingType: "home",
		Load: func(ctx context.Context, client *clashy.Client, locationID string, page clashy.PageOptions) ([]clashy.RankedClan, error) {
			return client.GetLocationClansByLocationID(ctx, locationID, page)
		},
		Points: func(clan clashy.RankedClan) int { return clan.Points },
	},
	{
		RankingType: "builder_base",
		Load: func(ctx context.Context, client *clashy.Client, locationID string, page clashy.PageOptions) ([]clashy.RankedClan, error) {
			return client.GetLocationClansBuilderBaseByLocationID(ctx, locationID, page)
		},
		Points: func(clan clashy.RankedClan) int { return clan.BuilderBasePoints },
	},
	{
		RankingType: "capital",
		Load: func(ctx context.Context, client *clashy.Client, locationID string, page clashy.PageOptions) ([]clashy.RankedClan, error) {
			return client.GetLocationClansCapitalByLocationID(ctx, locationID, page)
		},
		Points: func(clan clashy.RankedClan) int { return clan.CapitalPoints },
	},
}

type currentClanRankingRow struct {
	ClanTag   string
	Rank      int
	Points    int
	UpdatedAt time.Time
}

type currentClanRankingGroup struct {
	RankingType string
	LocationID  string
	Rows        []currentClanRankingRow
}

type leaderboardHistoryGroup struct {
	Kind       string
	LocationID string
	Date       time.Time
	Rows       any
}

type leaderboardHistoryScope struct {
	LocationID string
	Date       time.Time
}

type typedLeaderboardHistoryBatch struct {
	Scopes map[string][]leaderboardHistoryScope
	Rows   map[string][][]any
}

type typedLeaderboardHistorySpec struct {
	Kind       string
	Table      string
	TagColumn  string
	Columns    []string
	UpdateCols []string
}

type legendRankingItem struct {
	Player clashy.RankedPlayer
}

type scheduledDomain struct {
	store                   scheduledStore
	lastRankedGroupSeasonID int64
	loadLegendSeasons       legendSeasonsLoader
	loadLegendRankings      legendSeasonRankingsLoader
}

type scheduledStore interface {
	Close()
	ReplaceLeaderboardHistory(context.Context, []leaderboardHistoryGroup) (int, error)
	CompletedLegendSeasons(context.Context) (map[string]struct{}, error)
	ReplaceLegendSeason(context.Context, string, []models.LegendHistoryRow) (int, error)
	ReplaceCurrentClanRankingGroup(context.Context, currentClanRankingGroup) (int, error)
	ListRankedGroupTargets(context.Context) ([]string, error)
	StorePlayerProfiles(context.Context, []models.PlayerProfileIngest) (int, error)
	DeletePlayers(context.Context, []string) error
	StoreRankedLeagueGroupMembers(context.Context, []models.RankedLeagueGroupMemberRow) (int, error)
	MissingRankedGroupPlayers(context.Context, int64) ([]string, error)
}

func NewScheduledDomain() platform.Domain { return &scheduledDomain{} }

func (d *scheduledDomain) Name() string { return scheduledDomainName }

func (d *scheduledDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateScheduledConfig(app.Config); err != nil {
		return err
	}
	store, err := newScheduledStore(ctx, app)
	if err != nil {
		return err
	}
	defer store.Close()
	d.store = store

	// Current player leaderboards are another scheduled data refresh, so they
	// share this process while retaining their own cadence and readiness stats.
	leaderboardCtx, stopLeaderboards := context.WithCancel(ctx)
	leaderboardDone := make(chan error, 1)
	go func() {
		leaderboardDone <- (&leaderboardsDomain{}).Run(leaderboardCtx, app)
	}()
	defer func() {
		stopLeaderboards()
		<-leaderboardDone
	}()

	interval := time.Duration(app.Config.ScheduledIntervalSeconds) * time.Second
	for {
		start := time.Now()
		err = d.runCycle(ctx, app)
		app.Stats.RecordProcess(scheduledDomainName, time.Since(start))
		if err != nil {
			app.Stats.SetReady(scheduledDomainName, false, err.Error())
		}
		timer := time.NewTimer(interval)
		select {
		case err := <-leaderboardDone:
			if !timer.Stop() {
				<-timer.C
			}
			stopLeaderboards()
			leaderboardDone <- err
			return fmt.Errorf("scheduled leaderboard refresh stopped: %w", err)
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func validateScheduledConfig(cfg platform.Config) error {
	if cfg.ScheduledIntervalSeconds <= 0 {
		return errors.New("scheduled.interval_seconds must be greater than zero")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_* connection variables are required for scheduled")
	}
	return nil
}

func newScheduledStore(ctx context.Context, app *platform.App) (scheduledStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return newMemoryScheduledStore(), nil
	}
	return newTimescaleScheduledStore(ctx, app.Config.TimescaleURL)
}

func (d *scheduledDomain) runCycle(ctx context.Context, app *platform.App) error {
	locationIDs, err := d.loadLeaderboardLocationIDs(ctx, app)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	var cycleErrors []error
	historyGroups, historyFetchErr := d.doLeaderboardHistory(ctx, app, locationIDs, now)
	if historyWrites, err := d.store.ReplaceLeaderboardHistory(ctx, historyGroups); err != nil {
		cycleErrors = append(cycleErrors, err)
	} else {
		app.Stats.RecordWrite(scheduledDomainName, historyWrites)
	}
	if historyFetchErr != nil {
		cycleErrors = append(cycleErrors, historyFetchErr)
	}
	legendWrites, err := d.doLegendHistory(ctx, app, now)
	app.Stats.RecordWrite(scheduledDomainName, legendWrites)
	if err != nil {
		cycleErrors = append(cycleErrors, err)
	}
	currentRankingWrites, err := d.doCurrentClanRankings(ctx, app, locationIDs, now)
	app.Stats.RecordWrite(scheduledDomainName, currentRankingWrites)
	if err != nil {
		cycleErrors = append(cycleErrors, err)
	}
	groupWrites, err := d.doRankedGroupDiscovery(ctx, app, now)
	if err != nil {
		cycleErrors = append(cycleErrors, err)
	}
	app.Stats.RecordWrite(scheduledDomainName, groupWrites)
	if err := errors.Join(cycleErrors...); err != nil {
		return err
	}
	app.Stats.SetReady(scheduledDomainName, true, "")
	return nil
}

func (d *scheduledDomain) doRankedGroupDiscovery(ctx context.Context, app *platform.App, now time.Time) (int, error) {
	seasonID, ok := previousRankedSeasonID(now)
	if !ok {
		return 0, nil
	}
	if d.lastRankedGroupSeasonID == seasonID {
		return 0, nil
	}
	targets, err := d.store.ListRankedGroupTargets(ctx)
	if err != nil {
		return 0, err
	}
	pending := make(map[string]struct{}, len(targets))
	for _, tag := range targets {
		if tag != "" {
			pending[tag] = struct{}{}
		}
	}
	writes := 0
	for tag := range pending {
		delete(pending, tag)
		player, ok, err := d.fetchRankedSeedPlayer(ctx, app, tag)
		if err != nil {
			return writes, err
		}
		if !ok {
			continue
		}
		affected, err := d.store.StorePlayerProfiles(ctx, []models.PlayerProfileIngest{utils.PlayerProfileFromClashy(*player)})
		if err != nil {
			return writes, err
		}
		writes += affected
		if player.PreviousLeagueSeasonID == 0 || int64(player.PreviousLeagueSeasonID) != seasonID || player.PreviousLeagueGroupTag == "" || player.PreviousLeagueGroupTag == "#0" {
			continue
		}
		leagueTierID, found, err := d.previousLeagueTierID(ctx, app, player.Tag, seasonID)
		if err != nil {
			return writes, err
		}
		if !found {
			app.Logger.Info("ranked group skipped without matching league history", "tag", player.Tag, "season_id", seasonID, "group_tag", player.PreviousLeagueGroupTag)
			continue
		}
		members, err := d.fetchRankedGroupMembers(ctx, app, player.Tag, player.PreviousLeagueGroupTag, seasonID, leagueTierID)
		if err != nil {
			return writes, err
		}
		affected, err = d.store.StoreRankedLeagueGroupMembers(ctx, members)
		if err != nil {
			return writes, err
		}
		writes += affected
		for _, member := range members {
			delete(pending, member.PlayerTag)
		}
	}
	missingWrites, err := d.fetchAndStoreMissingRankedPlayers(ctx, app, seasonID, now)
	if err != nil {
		return writes, err
	}
	writes += missingWrites
	d.lastRankedGroupSeasonID = seasonID
	return writes, nil
}

func (d *scheduledDomain) fetchRankedSeedPlayer(ctx context.Context, app *platform.App, tag string) (*clashy.Player, bool, error) {
	player, err := platform.RetryClashFetch(ctx, func(fetchCtx context.Context) (*clashy.Player, error) {
		start := time.Now()
		player, err := app.Clash.GetPlayer(fetchCtx, tag)
		app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
		return player, err
	})
	if isClashNotFound(err) {
		if err := d.store.DeletePlayers(ctx, []string{tag}); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return player, true, nil
}

func (d *scheduledDomain) previousLeagueTierID(ctx context.Context, app *platform.App, tag string, seasonID int64) (int, bool, error) {
	entries, err := platform.RetryClashFetch(ctx, func(fetchCtx context.Context) ([]clashy.LeagueHistoryEntry, error) {
		start := time.Now()
		entries, err := app.Clash.GetPlayerLeagueHistory(fetchCtx, tag)
		app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
		return entries, err
	})
	if err != nil {
		return 0, false, err
	}
	for _, entry := range entries {
		if int64(entry.LeagueSeasonID) == seasonID && entry.LeagueTierID > 0 {
			return entry.LeagueTierID, true, nil
		}
	}
	return 0, false, nil
}

func (d *scheduledDomain) fetchRankedGroupMembers(ctx context.Context, app *platform.App, seedTag, groupTag string, seasonID int64, leagueTierID int) ([]models.RankedLeagueGroupMemberRow, error) {
	group, err := platform.RetryClashFetch(ctx, func(fetchCtx context.Context) (*clashy.LeagueTierGroup, error) {
		start := time.Now()
		group, err := app.Clash.GetPlayerLeagueGroup(fetchCtx, seedTag, groupTag, strconv.FormatInt(seasonID, 10))
		app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
		return group, err
	})
	if err != nil {
		return nil, err
	}
	return rankedGroupMemberRows(groupTag, seasonID, leagueTierID, group), nil
}

func rankedGroupMemberRows(groupTag string, seasonID int64, leagueTierID int, group *clashy.LeagueTierGroup) []models.RankedLeagueGroupMemberRow {
	if group == nil {
		return nil
	}
	rows := make([]models.RankedLeagueGroupMemberRow, 0, len(group.Members))
	for i, member := range group.Members {
		if member.PlayerTag == "" {
			continue
		}
		rows = append(rows, models.RankedLeagueGroupMemberRow{
			SeasonID:         seasonID,
			GroupTag:         groupTag,
			LeagueTierID:     leagueTierID,
			PlayerTag:        member.PlayerTag,
			PlayerName:       member.PlayerName,
			ClanTag:          member.ClanTag,
			ClanName:         member.ClanName,
			Placement:        i + 1,
			LeagueTrophies:   member.LeagueTrophies,
			AttackWinCount:   member.AttackWinCount,
			AttackLoseCount:  member.AttackLoseCount,
			DefenseWinCount:  member.DefenseWinCount,
			DefenseLoseCount: member.DefenseLoseCount,
		})
	}
	return rows
}

func (d *scheduledDomain) fetchAndStoreMissingRankedPlayers(ctx context.Context, app *platform.App, seasonID int64, _ time.Time) (int, error) {
	tags, err := d.store.MissingRankedGroupPlayers(ctx, seasonID)
	if err != nil {
		return 0, err
	}
	writes := 0
	profiles := make([]models.PlayerProfileIngest, 0, 500)
	flush := func() error {
		if len(profiles) == 0 {
			return nil
		}
		affected, err := d.store.StorePlayerProfiles(ctx, profiles)
		if err != nil {
			return err
		}
		writes += affected
		profiles = profiles[:0]
		return nil
	}
	for _, tag := range tags {
		player, ok, err := d.fetchRankedSeedPlayer(ctx, app, tag)
		if err != nil {
			return writes, err
		}
		if !ok {
			continue
		}
		profiles = append(profiles, utils.PlayerProfileFromClashy(*player))
		if len(profiles) >= 500 {
			if err := flush(); err != nil {
				return writes, err
			}
		}
	}
	return writes, flush()
}

func previousRankedSeasonID(now time.Time) (int64, bool) {
	now = now.UTC()
	weekdayOffset := (int(now.Weekday()) + 6) % 7
	currentSeasonStart := time.Date(now.Year(), now.Month(), now.Day(), 5, 0, 0, 0, time.UTC).AddDate(0, 0, -weekdayOffset)
	if now.Before(currentSeasonStart.Add(7 * time.Hour)) {
		return 0, false
	}
	return currentSeasonStart.AddDate(0, 0, -7).Unix(), true
}

func (d *scheduledDomain) loadLeaderboardLocationIDs(
	ctx context.Context,
	app *platform.App,
) ([]string, error) {
	locations, err := platform.RetryClashFetch(ctx, func(fetchCtx context.Context) ([]clashy.Location, error) {
		start := time.Now()
		locations, err := app.Clash.SearchLocations(fetchCtx, clashy.PageOptions{})
		app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
		return locations, err
	})
	if err != nil {
		return nil, err
	}
	return leaderboardLocationIDs(locations), nil
}

func (d *scheduledDomain) doLeaderboardHistory(
	ctx context.Context,
	app *platform.App,
	locationIDs []string,
	now time.Time,
) ([]leaderboardHistoryGroup, error) {
	date := dayStart(now)
	groups := make([]leaderboardHistoryGroup, 0, len(leaderboardHistoryPaths)*len(locationIDs))
	var groupErrors []error
	for _, locationID := range locationIDs {
		for _, path := range leaderboardHistoryPaths {
			if !shouldStoreLeaderboardHistoryKind(path.Kind, now) {
				continue
			}
			payload, err := platform.RetryClashFetch(ctx, func(fetchCtx context.Context) (any, error) {
				start := time.Now()
				payload, err := path.Load(fetchCtx, app.Clash, locationID)
				app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
				return payload, err
			})
			if err != nil {
				groupErrors = append(
					groupErrors,
					fmt.Errorf("fetch %s leaderboard history for %s: %w", path.Kind, locationID, err),
				)
				continue
			}
			group, err := leaderboardHistoryGroupFromResponse(path.Kind, locationID, date, payload)
			if err != nil {
				groupErrors = append(groupErrors, err)
				continue
			}
			groups = append(groups, group)
		}
	}
	return groups, errors.Join(groupErrors...)
}

func shouldStoreLeaderboardHistoryKind(kind string, now time.Time) bool {
	if kind != leaderboardHistoryClanCapitalPoints {
		return true
	}
	return now.UTC().Weekday() == time.Tuesday
}

func (d *scheduledDomain) doLegendHistory(
	ctx context.Context,
	app *platform.App,
	now time.Time,
) (int, error) {
	completed, err := d.store.CompletedLegendSeasons(ctx)
	if err != nil {
		return 0, err
	}
	loadSeasons := d.loadLegendSeasons
	if loadSeasons == nil {
		loadSeasons = loadOfficialLegendSeasons
	}
	officialSeasons, err := loadSeasons(ctx, app)
	if err != nil {
		return 0, err
	}
	missing, err := missingCompletedLegendSeasons(officialSeasons, completed, now)
	if err != nil {
		return 0, err
	}
	loadRankings := d.loadLegendRankings
	if loadRankings == nil {
		loadRankings = loadOfficialLegendSeasonRankings
	}
	writes := 0
	var seasonErrors []error
	for _, season := range missing {
		rankings, err := loadRankings(ctx, app, season)
		if err != nil {
			seasonErrors = append(seasonErrors, fmt.Errorf("fetch legend season %s: %w", season, err))
			continue
		}
		rows, err := legendHistoryRows(season, rankings)
		if err != nil {
			seasonErrors = append(seasonErrors, err)
			continue
		}
		affected, err := d.store.ReplaceLegendSeason(ctx, season, rows)
		if err != nil {
			seasonErrors = append(seasonErrors, fmt.Errorf("store legend season %s: %w", season, err))
			continue
		}
		writes += affected
	}
	return writes, errors.Join(seasonErrors...)
}

func loadOfficialLegendSeasons(ctx context.Context, app *platform.App) ([]string, error) {
	return platform.RetryClashFetch(ctx, func(fetchCtx context.Context) ([]string, error) {
		start := time.Now()
		seasons, err := app.Clash.GetSeasons(fetchCtx, legendLeagueID)
		app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
		return seasons, err
	})
}

func loadOfficialLegendSeasonRankings(
	ctx context.Context,
	app *platform.App,
	season string,
) ([]legendRankingItem, error) {
	return platform.RetryClashFetch(ctx, func(fetchCtx context.Context) ([]legendRankingItem, error) {
		return fetchAllLegendSeasonRankingPages(fetchCtx, app, season)
	})
}

func fetchAllLegendSeasonRankingPages(
	ctx context.Context,
	app *platform.App,
	season string,
) ([]legendRankingItem, error) {
	if _, err := officialLegendSeasonWindow(season); err != nil {
		return nil, err
	}
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = strings.TrimRight(app.Config.ProxyURL, "/")
	cfg.LookupCache = false
	cfg.UpdateCache = false
	httpClient := clashy.NewHTTPClient(cfg)
	defer httpClient.CloseIdleConnections()

	return collectLegendSeasonRankingPages(season, func(after string) ([]legendRankingItem, string, error) {
		endpoint := legendSeasonRankingPageURL(cfg.BaseURL, season, after)
		start := time.Now()
		response, err := httpClient.Do(
			ctx,
			http.MethodGet,
			endpoint,
			nil,
			clashy.RequestOptions{SkipAuth: true},
		)
		app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
		if err != nil {
			return nil, "", err
		}
		var page struct {
			Items  []clashy.RankedPlayer `json:"items"`
			Paging struct {
				Cursors struct {
					After string `json:"after"`
				} `json:"cursors"`
			} `json:"paging"`
		}
		if err := json.Unmarshal(response.Body, &page); err != nil {
			return nil, "", err
		}
		items := make([]legendRankingItem, 0, len(page.Items))
		for _, player := range page.Items {
			items = append(items, legendRankingItem{Player: player})
		}
		return items, page.Paging.Cursors.After, nil
	})
}

func collectLegendSeasonRankingPages(
	season string,
	fetch func(after string) ([]legendRankingItem, string, error),
) ([]legendRankingItem, error) {
	rankings := make([]legendRankingItem, 0, legendSeasonPageSize)
	after := ""
	seenCursors := make(map[string]struct{})
	for {
		items, next, err := fetch(after)
		if err != nil {
			return nil, err
		}
		rankings = append(rankings, items...)
		if next == "" {
			return rankings, nil
		}
		if len(items) == 0 {
			return nil, fmt.Errorf("legend season %s returned an empty page with continuation cursor", season)
		}
		if _, duplicate := seenCursors[next]; duplicate {
			return nil, fmt.Errorf("legend season %s repeated pagination cursor %q", season, next)
		}
		seenCursors[next] = struct{}{}
		after = next
	}
}

func legendSeasonRankingPageURL(baseURL, season, after string) string {
	query := url.Values{}
	query.Set("limit", strconv.Itoa(legendSeasonPageSize))
	if after != "" {
		query.Set("after", after)
	}
	return fmt.Sprintf(
		"%s/leagues/%d/seasons/%s?%s",
		strings.TrimRight(baseURL, "/"),
		legendLeagueID,
		url.PathEscape(season),
		query.Encode(),
	)
}

func missingCompletedLegendSeasons(
	official []string,
	completed map[string]struct{},
	now time.Time,
) ([]string, error) {
	now = now.UTC()
	missing := make([]string, 0, len(official))
	seen := make(map[string]struct{}, len(official))
	for _, season := range official {
		window, err := officialLegendSeasonWindow(season)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[season]; duplicate {
			continue
		}
		seen[season] = struct{}{}
		if window.EndTime.After(now) {
			continue
		}
		if _, exists := completed[season]; !exists {
			missing = append(missing, season)
		}
	}
	return missing, nil
}

func officialLegendSeasonWindow(season string) (clashy.SeasonWindow, error) {
	if strings.HasPrefix(season, legendSeasonV2Prefix) {
		rawEnd := strings.TrimPrefix(season, legendSeasonV2Prefix)
		end, err := time.Parse(time.RFC3339, rawEnd)
		if err != nil || end.Format(time.RFC3339) != rawEnd {
			return clashy.SeasonWindow{}, fmt.Errorf("invalid official legend season %q", season)
		}
		end = end.UTC()
		return clashy.SeasonWindow{
			SeasonID:  season,
			StartTime: end.Add(-legendSeasonV2Length),
			EndTime:   end,
		}, nil
	}
	if len(season) != len("2006-01") {
		return clashy.SeasonWindow{}, fmt.Errorf("invalid official legend season %q", season)
	}
	parsed, err := time.Parse("2006-01", season)
	if err != nil || parsed.Format("2006-01") != season {
		return clashy.SeasonWindow{}, fmt.Errorf("invalid official legend season %q", season)
	}
	window, err := clashy.GetSeasonByID(season)
	if err != nil {
		return clashy.SeasonWindow{}, fmt.Errorf("invalid official legend season %q: %w", season, err)
	}
	return window, nil
}

func legendHistoryRows(season string, rankings []legendRankingItem) ([]models.LegendHistoryRow, error) {
	if _, err := officialLegendSeasonWindow(season); err != nil {
		return nil, err
	}
	if len(rankings) == 0 {
		return nil, fmt.Errorf("legend season %s returned no final rankings", season)
	}
	rows := make([]models.LegendHistoryRow, 0, len(rankings))
	seenTags := make(map[string]struct{}, len(rankings))
	seenRanks := make(map[int]struct{}, len(rankings))
	for _, item := range rankings {
		ranking := item.Player
		if ranking.Tag == "" ||
			strings.TrimSpace(ranking.Name) == "" ||
			ranking.ExpLevel < 0 ||
			ranking.Rank <= 0 ||
			ranking.Trophies < 0 ||
			ranking.AttackWins < 0 ||
			ranking.DefenseWins < 0 {
			return nil, fmt.Errorf(
				"invalid legend season %s row: tag %q name %q rank %d trophies %d",
				season,
				ranking.Tag,
				ranking.Name,
				ranking.Rank,
				ranking.Trophies,
			)
		}
		if _, duplicate := seenTags[ranking.Tag]; duplicate {
			return nil, fmt.Errorf("duplicate legend season %s player %s", season, ranking.Tag)
		}
		if _, duplicate := seenRanks[ranking.Rank]; duplicate {
			return nil, fmt.Errorf("duplicate legend season %s rank %d", season, ranking.Rank)
		}
		row := models.LegendHistoryRow{
			Season:       season,
			PlayerTag:    ranking.Tag,
			PlayerName:   ranking.Name,
			ExpLevel:     ranking.ExpLevel,
			Trophies:     ranking.Trophies,
			AttackWins:   ranking.AttackWins,
			DefenseWins:  ranking.DefenseWins,
			Rank:         ranking.Rank,
			LeagueTierID: nil,
		}
		if ranking.Clan != nil {
			token := badgeToken(ranking.Clan.Badge)
			if strings.TrimSpace(ranking.Clan.Tag) == "" ||
				strings.TrimSpace(ranking.Clan.Name) == "" ||
				token == "" {
				return nil, fmt.Errorf(
					"invalid legend season %s player %s clan snapshot",
					season,
					ranking.Tag,
				)
			}
			clanTag := ranking.Clan.Tag
			clanName := ranking.Clan.Name
			row.ClanTag = &clanTag
			row.ClanName = &clanName
			row.ClanBadgeToken = &token
		}
		if ranking.LeagueTier.ID > 0 {
			leagueTierID := ranking.LeagueTier.ID
			row.LeagueTierID = &leagueTierID
		} else if ranking.LeagueTier.ID < 0 {
			return nil, fmt.Errorf(
				"invalid legend season %s player %s league tier %d",
				season,
				ranking.Tag,
				ranking.LeagueTier.ID,
			)
		}
		seenTags[ranking.Tag] = struct{}{}
		seenRanks[ranking.Rank] = struct{}{}
		rows = append(rows, row)
	}
	for rank := 1; rank <= len(rows); rank++ {
		if _, exists := seenRanks[rank]; !exists {
			return nil, fmt.Errorf("incomplete legend season %s rankings: missing rank %d", season, rank)
		}
	}
	return rows, nil
}

func (d *scheduledDomain) doCurrentClanRankings(
	ctx context.Context,
	app *platform.App,
	locationIDs []string,
	updatedAt time.Time,
) (int, error) {
	writes := 0
	var groupErrors []error
	for _, locationID := range locationIDs {
		for _, path := range currentClanRankingPaths {
			rankings, err := platform.RetryClashFetch(ctx, func(fetchCtx context.Context) ([]clashy.RankedClan, error) {
				start := time.Now()
				rankings, err := path.Load(fetchCtx, app.Clash, locationID, clashy.PageOptions{Limit: currentClanRankingLimit})
				app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
				return rankings, err
			})
			if err != nil {
				groupErrors = append(groupErrors, fmt.Errorf("fetch %s clan rankings for %s: %w", path.RankingType, locationID, err))
				continue
			}
			group, err := currentClanRankingGroupFromResponse(
				path.RankingType,
				locationID,
				rankings,
				path.Points,
				updatedAt,
			)
			if err != nil {
				groupErrors = append(groupErrors, err)
				continue
			}
			count, err := d.store.ReplaceCurrentClanRankingGroup(ctx, group)
			if err != nil {
				groupErrors = append(groupErrors, fmt.Errorf("replace %s clan rankings for %s: %w", path.RankingType, locationID, err))
				continue
			}
			writes += count
		}
	}
	return writes, errors.Join(groupErrors...)
}

func currentClanRankingGroupFromResponse(
	rankingType string,
	locationID string,
	rankings []clashy.RankedClan,
	points func(clashy.RankedClan) int,
	updatedAt time.Time,
) (currentClanRankingGroup, error) {
	group := currentClanRankingGroup{
		RankingType: rankingType,
		LocationID:  locationID,
		Rows:        make([]currentClanRankingRow, 0, len(rankings)),
	}
	if !validCurrentClanRankingType(rankingType) {
		return currentClanRankingGroup{}, fmt.Errorf("unsupported clan ranking type %q", rankingType)
	}
	if locationID != "global" {
		if _, err := strconv.Atoi(locationID); err != nil {
			return currentClanRankingGroup{}, fmt.Errorf("invalid clan ranking location %q", locationID)
		}
	}
	if len(rankings) > currentClanRankingLimit {
		return currentClanRankingGroup{}, fmt.Errorf(
			"oversized %s clan rankings for %s: got %d rows, limit %d",
			rankingType,
			locationID,
			len(rankings),
			currentClanRankingLimit,
		)
	}
	seenTags := make(map[string]struct{}, len(rankings))
	seenRanks := make(map[int]struct{}, len(rankings))
	for _, ranking := range rankings {
		score := points(ranking)
		if ranking.Tag == "" || ranking.Rank < 1 || ranking.Rank > currentClanRankingLimit || score < 0 {
			return currentClanRankingGroup{}, fmt.Errorf(
				"incomplete %s clan ranking row for %s: tag %q rank %d points %d",
				rankingType,
				locationID,
				ranking.Tag,
				ranking.Rank,
				score,
			)
		}
		if _, ok := seenTags[ranking.Tag]; ok {
			return currentClanRankingGroup{}, fmt.Errorf("duplicate clan %s in %s rankings for %s", ranking.Tag, rankingType, locationID)
		}
		if _, ok := seenRanks[ranking.Rank]; ok {
			return currentClanRankingGroup{}, fmt.Errorf("duplicate rank %d in %s rankings for %s", ranking.Rank, rankingType, locationID)
		}
		seenTags[ranking.Tag] = struct{}{}
		seenRanks[ranking.Rank] = struct{}{}
		group.Rows = append(group.Rows, currentClanRankingRow{
			ClanTag:   ranking.Tag,
			Rank:      ranking.Rank,
			Points:    score,
			UpdatedAt: updatedAt.UTC(),
		})
	}
	return group, nil
}

func validCurrentClanRankingType(value string) bool {
	switch value {
	case "home", "builder_base", "capital":
		return true
	default:
		return false
	}
}

func dayStart(value time.Time) time.Time {
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func leaderboardLocationIDs(locations []clashy.Location) []string {
	out := make([]string, 0, len(locations)+1)
	seen := make(map[int]struct{}, len(locations))
	for _, location := range locations {
		if location.ID == 0 {
			continue
		}
		if _, ok := seen[location.ID]; ok {
			continue
		}
		seen[location.ID] = struct{}{}
		out = append(out, strconv.Itoa(location.ID))
	}
	return append(out, "global")
}

func leaderboardHistoryGroupFromResponse(
	kind string,
	locationID string,
	date time.Time,
	payload any,
) (leaderboardHistoryGroup, error) {
	group := leaderboardHistoryGroup{
		Kind:       kind,
		LocationID: locationID,
		Date:       dayStart(date),
	}
	if !validLeaderboardHistoryKind(kind) {
		return leaderboardHistoryGroup{}, fmt.Errorf("unsupported leaderboard history kind %q", kind)
	}
	if locationID != "global" {
		location, err := strconv.Atoi(locationID)
		if err != nil || location <= 0 {
			return leaderboardHistoryGroup{}, fmt.Errorf("invalid leaderboard history location %q", locationID)
		}
	}

	switch kind {
	case leaderboardHistoryPlayerHomeTrophies:
		items, ok := payload.([]clashy.RankedPlayer)
		if !ok {
			return leaderboardHistoryGroup{}, fmt.Errorf("player leaderboard history kind %q returned %T", kind, payload)
		}
		rows := make([]models.PlayerTrophyHistoryRow, 0, len(items))
		for _, item := range items {
			row, err := playerTrophyHistoryRow(locationID, group.Date, item)
			if err != nil {
				return leaderboardHistoryGroup{}, err
			}
			rows = append(rows, row)
		}
		group.Rows = rows
	case leaderboardHistoryPlayerBuilderBaseTrophies:
		items, ok := payload.([]clashy.RankedPlayer)
		if !ok {
			return leaderboardHistoryGroup{}, fmt.Errorf("player leaderboard history kind %q returned %T", kind, payload)
		}
		rows := make([]models.PlayerBuilderBaseTrophyHistoryRow, 0, len(items))
		for _, item := range items {
			row, err := playerBuilderBaseTrophyHistoryRow(locationID, group.Date, item)
			if err != nil {
				return leaderboardHistoryGroup{}, err
			}
			rows = append(rows, row)
		}
		group.Rows = rows
	case leaderboardHistoryClanHomePoints:
		items, ok := payload.([]clashy.RankedClan)
		if !ok {
			return leaderboardHistoryGroup{}, fmt.Errorf("clan leaderboard history kind %q returned %T", kind, payload)
		}
		rows := make([]models.ClanTrophyHistoryRow, 0, len(items))
		for _, item := range items {
			row, err := clanTrophyHistoryRow(locationID, group.Date, item)
			if err != nil {
				return leaderboardHistoryGroup{}, err
			}
			rows = append(rows, row)
		}
		group.Rows = rows
	case leaderboardHistoryClanBuilderBasePoints:
		items, ok := payload.([]clashy.RankedClan)
		if !ok {
			return leaderboardHistoryGroup{}, fmt.Errorf("clan leaderboard history kind %q returned %T", kind, payload)
		}
		rows := make([]models.ClanBuilderBaseTrophyHistoryRow, 0, len(items))
		for _, item := range items {
			row, err := clanBuilderBaseTrophyHistoryRow(locationID, group.Date, item)
			if err != nil {
				return leaderboardHistoryGroup{}, err
			}
			rows = append(rows, row)
		}
		group.Rows = rows
	case leaderboardHistoryClanCapitalPoints:
		items, ok := payload.([]clashy.RankedClan)
		if !ok {
			return leaderboardHistoryGroup{}, fmt.Errorf("clan leaderboard history kind %q returned %T", kind, payload)
		}
		rows := make([]models.ClanCapitalHistoryRow, 0, len(items))
		for _, item := range items {
			row, err := clanCapitalHistoryRow(locationID, group.Date, item)
			if err != nil {
				return leaderboardHistoryGroup{}, err
			}
			rows = append(rows, row)
		}
		group.Rows = rows
	}
	return group, nil
}

func playerTrophyHistoryRow(
	locationID string,
	date time.Time,
	item clashy.RankedPlayer,
) (models.PlayerTrophyHistoryRow, error) {
	clanTag, clanName, clanBadgeToken, err := leaderboardPlayerClan(item.Clan)
	if err != nil {
		return models.PlayerTrophyHistoryRow{}, fmt.Errorf("player trophy history %s: %w", item.Tag, err)
	}
	previousRank, err := optionalHistoryPositiveInt(item.PreviousRank)
	if err != nil {
		return models.PlayerTrophyHistoryRow{}, fmt.Errorf("player trophy history %s previous rank: %w", item.Tag, err)
	}
	leagueIDValue := item.LeagueTier.ID
	if leagueIDValue <= 0 {
		leagueIDValue = item.League.ID
	}
	leagueID, err := optionalHistoryPositiveInt(leagueIDValue)
	if err != nil {
		return models.PlayerTrophyHistoryRow{}, fmt.Errorf("player trophy history %s league: %w", item.Tag, err)
	}
	row := models.PlayerTrophyHistoryRow{
		LocationID:     locationID,
		Date:           date,
		PlayerTag:      item.Tag,
		PlayerName:     item.Name,
		ExpLevel:       item.ExpLevel,
		Trophies:       item.Trophies,
		AttackWins:     item.AttackWins,
		DefenseWins:    item.DefenseWins,
		Rank:           item.Rank,
		PreviousRank:   previousRank,
		ClanTag:        clanTag,
		ClanName:       clanName,
		ClanBadgeToken: clanBadgeToken,
		LeagueID:       leagueID,
	}
	if err := validatePlayerTrophyHistoryRow(row); err != nil {
		return models.PlayerTrophyHistoryRow{}, err
	}
	return row, nil
}

func playerBuilderBaseTrophyHistoryRow(
	locationID string,
	date time.Time,
	item clashy.RankedPlayer,
) (models.PlayerBuilderBaseTrophyHistoryRow, error) {
	clanTag, clanName, clanBadgeToken, err := leaderboardPlayerClan(item.Clan)
	if err != nil {
		return models.PlayerBuilderBaseTrophyHistoryRow{}, fmt.Errorf("builder base trophy history %s: %w", item.Tag, err)
	}
	previousRank, err := optionalHistoryPositiveInt(item.PreviousRank)
	if err != nil {
		return models.PlayerBuilderBaseTrophyHistoryRow{}, fmt.Errorf("builder base trophy history %s previous rank: %w", item.Tag, err)
	}
	var leagueID *int
	if item.BuilderBaseLeague != nil {
		leagueID, err = optionalHistoryPositiveInt(item.BuilderBaseLeague.ID)
		if err != nil {
			return models.PlayerBuilderBaseTrophyHistoryRow{}, fmt.Errorf("builder base trophy history %s league: %w", item.Tag, err)
		}
	}
	builderBaseTrophies := item.BuilderBaseTrophies
	if builderBaseTrophies == 0 && item.VersusTrophies > 0 {
		builderBaseTrophies = item.VersusTrophies
	}
	builderBaseBattleWins, err := optionalHistoryPositiveInt(item.VersusAttackWins)
	if err != nil {
		return models.PlayerBuilderBaseTrophyHistoryRow{}, fmt.Errorf("builder base trophy history %s battle wins: %w", item.Tag, err)
	}
	row := models.PlayerBuilderBaseTrophyHistoryRow{
		LocationID:            locationID,
		Date:                  date,
		PlayerTag:             item.Tag,
		PlayerName:            item.Name,
		ExpLevel:              item.ExpLevel,
		BuilderBaseTrophies:   builderBaseTrophies,
		BuilderBaseBattleWins: builderBaseBattleWins,
		Rank:                  item.Rank,
		PreviousRank:          previousRank,
		ClanTag:               clanTag,
		ClanName:              clanName,
		ClanBadgeToken:        clanBadgeToken,
		LeagueID:              leagueID,
	}
	if err := validatePlayerBuilderBaseTrophyHistoryRow(row); err != nil {
		return models.PlayerBuilderBaseTrophyHistoryRow{}, err
	}
	return row, nil
}

func clanTrophyHistoryRow(
	locationID string,
	date time.Time,
	item clashy.RankedClan,
) (models.ClanTrophyHistoryRow, error) {
	common, err := leaderboardClanHistoryValues(locationID, date, item)
	if err != nil {
		return models.ClanTrophyHistoryRow{}, err
	}
	row := models.ClanTrophyHistoryRow{
		LocationID:     common.LocationID,
		Date:           common.Date,
		ClanTag:        common.ClanTag,
		ClanName:       common.ClanName,
		ClanBadgeToken: common.ClanBadgeToken,
		ClanLevel:      common.ClanLevel,
		ClanPoints:     item.Points,
		Members:        common.Members,
		ClanLocationID: common.ClanLocationID,
		Rank:           common.Rank,
		PreviousRank:   common.PreviousRank,
	}
	if err := validateClanTrophyHistoryRow(row); err != nil {
		return models.ClanTrophyHistoryRow{}, err
	}
	return row, nil
}

func clanBuilderBaseTrophyHistoryRow(
	locationID string,
	date time.Time,
	item clashy.RankedClan,
) (models.ClanBuilderBaseTrophyHistoryRow, error) {
	common, err := leaderboardClanHistoryValues(locationID, date, item)
	if err != nil {
		return models.ClanBuilderBaseTrophyHistoryRow{}, err
	}
	row := models.ClanBuilderBaseTrophyHistoryRow{
		LocationID:        common.LocationID,
		Date:              common.Date,
		ClanTag:           common.ClanTag,
		ClanName:          common.ClanName,
		ClanBadgeToken:    common.ClanBadgeToken,
		ClanLevel:         common.ClanLevel,
		BuilderBasePoints: item.BuilderBasePoints,
		Members:           common.Members,
		ClanLocationID:    common.ClanLocationID,
		Rank:              common.Rank,
		PreviousRank:      common.PreviousRank,
	}
	if err := validateClanBuilderBaseTrophyHistoryRow(row); err != nil {
		return models.ClanBuilderBaseTrophyHistoryRow{}, err
	}
	return row, nil
}

func clanCapitalHistoryRow(
	locationID string,
	date time.Time,
	item clashy.RankedClan,
) (models.ClanCapitalHistoryRow, error) {
	common, err := leaderboardClanHistoryValues(locationID, date, item)
	if err != nil {
		return models.ClanCapitalHistoryRow{}, err
	}
	row := models.ClanCapitalHistoryRow{
		LocationID:     common.LocationID,
		Date:           common.Date,
		ClanTag:        common.ClanTag,
		ClanName:       common.ClanName,
		ClanBadgeToken: common.ClanBadgeToken,
		ClanLevel:      common.ClanLevel,
		CapitalPoints:  item.CapitalPoints,
		Members:        common.Members,
		ClanLocationID: common.ClanLocationID,
		Rank:           common.Rank,
		PreviousRank:   common.PreviousRank,
	}
	if err := validateClanCapitalHistoryRow(row); err != nil {
		return models.ClanCapitalHistoryRow{}, err
	}
	return row, nil
}

type leaderboardClanHistoryCommon struct {
	LocationID     string
	Date           time.Time
	ClanTag        string
	ClanName       string
	ClanBadgeToken string
	ClanLevel      int
	Members        int
	ClanLocationID *int
	Rank           int
	PreviousRank   *int
}

func leaderboardClanHistoryValues(
	locationID string,
	date time.Time,
	item clashy.RankedClan,
) (leaderboardClanHistoryCommon, error) {
	token := badgeToken(item.Badge)
	previousRank, err := optionalHistoryPositiveInt(item.PreviousRank)
	if err != nil {
		return leaderboardClanHistoryCommon{}, fmt.Errorf("clan history %s previous rank: %w", item.Tag, err)
	}
	var clanLocationID *int
	if item.Location != nil {
		clanLocationID, err = optionalHistoryPositiveInt(item.Location.ID)
		if err != nil {
			return leaderboardClanHistoryCommon{}, fmt.Errorf("clan history %s location: %w", item.Tag, err)
		}
	}
	common := leaderboardClanHistoryCommon{
		LocationID:     locationID,
		Date:           date,
		ClanTag:        item.Tag,
		ClanName:       item.Name,
		ClanBadgeToken: token,
		ClanLevel:      item.Level,
		Members:        item.MemberCount,
		ClanLocationID: clanLocationID,
		Rank:           item.Rank,
		PreviousRank:   previousRank,
	}
	if strings.TrimSpace(common.ClanTag) == "" ||
		strings.TrimSpace(common.ClanName) == "" ||
		common.ClanBadgeToken == "" ||
		common.ClanLevel <= 0 ||
		common.Members < 0 ||
		common.Members > 50 ||
		common.Rank <= 0 {
		return leaderboardClanHistoryCommon{}, fmt.Errorf("invalid clan history row for %s", item.Tag)
	}
	return common, nil
}

func leaderboardPlayerClan(
	clan *clashy.PlayerClan,
) (*string, *string, *string, error) {
	if clan == nil {
		return nil, nil, nil, nil
	}
	token := badgeToken(clan.Badge)
	if strings.TrimSpace(clan.Tag) == "" ||
		strings.TrimSpace(clan.Name) == "" ||
		token == "" {
		return nil, nil, nil, fmt.Errorf("invalid clan snapshot")
	}
	tag := clan.Tag
	name := clan.Name
	return &tag, &name, &token, nil
}

func optionalHistoryPositiveInt(value int) (*int, error) {
	if value < 0 {
		return nil, fmt.Errorf("negative value %d", value)
	}
	if value == 0 {
		return nil, nil
	}
	result := value
	return &result, nil
}

func validatePlayerTrophyHistoryRow(row models.PlayerTrophyHistoryRow) error {
	if !validLeaderboardHistoryLocation(row.LocationID) ||
		row.Date.IsZero() ||
		!row.Date.Equal(dayStart(row.Date)) ||
		strings.TrimSpace(row.PlayerTag) == "" ||
		strings.TrimSpace(row.PlayerName) == "" ||
		row.ExpLevel < 0 ||
		row.Trophies < 0 ||
		row.AttackWins < 0 ||
		row.DefenseWins < 0 ||
		row.Rank <= 0 ||
		!validOptionalPositiveHistoryInt(row.PreviousRank) ||
		!validOptionalPositiveHistoryInt(row.LeagueID) ||
		!validLeaderboardPlayerClan(row.ClanTag, row.ClanName, row.ClanBadgeToken) {
		return fmt.Errorf("invalid player trophy history row for %s", row.PlayerTag)
	}
	return nil
}

func validatePlayerBuilderBaseTrophyHistoryRow(row models.PlayerBuilderBaseTrophyHistoryRow) error {
	if !validLeaderboardHistoryLocation(row.LocationID) ||
		row.Date.IsZero() ||
		!row.Date.Equal(dayStart(row.Date)) ||
		strings.TrimSpace(row.PlayerTag) == "" ||
		strings.TrimSpace(row.PlayerName) == "" ||
		row.ExpLevel < 0 ||
		row.BuilderBaseTrophies < 0 ||
		(row.BuilderBaseBattleWins != nil && *row.BuilderBaseBattleWins < 0) ||
		row.Rank <= 0 ||
		!validOptionalPositiveHistoryInt(row.PreviousRank) ||
		!validOptionalPositiveHistoryInt(row.LeagueID) ||
		!validLeaderboardPlayerClan(row.ClanTag, row.ClanName, row.ClanBadgeToken) {
		return fmt.Errorf("invalid builder base trophy history row for %s", row.PlayerTag)
	}
	return nil
}

func validateClanTrophyHistoryRow(row models.ClanTrophyHistoryRow) error {
	if !validLeaderboardClanHistoryValues(
		row.LocationID,
		row.Date,
		row.ClanTag,
		row.ClanName,
		row.ClanBadgeToken,
		row.ClanLevel,
		row.ClanPoints,
		row.Members,
		row.ClanLocationID,
		row.Rank,
		row.PreviousRank,
	) {
		return fmt.Errorf("invalid clan trophy history row for %s", row.ClanTag)
	}
	return nil
}

func validateClanBuilderBaseTrophyHistoryRow(row models.ClanBuilderBaseTrophyHistoryRow) error {
	if !validLeaderboardClanHistoryValues(
		row.LocationID,
		row.Date,
		row.ClanTag,
		row.ClanName,
		row.ClanBadgeToken,
		row.ClanLevel,
		row.BuilderBasePoints,
		row.Members,
		row.ClanLocationID,
		row.Rank,
		row.PreviousRank,
	) {
		return fmt.Errorf("invalid clan builder base trophy history row for %s", row.ClanTag)
	}
	return nil
}

func validateClanCapitalHistoryRow(row models.ClanCapitalHistoryRow) error {
	if !validLeaderboardClanHistoryValues(
		row.LocationID,
		row.Date,
		row.ClanTag,
		row.ClanName,
		row.ClanBadgeToken,
		row.ClanLevel,
		row.CapitalPoints,
		row.Members,
		row.ClanLocationID,
		row.Rank,
		row.PreviousRank,
	) {
		return fmt.Errorf("invalid clan capital history row for %s", row.ClanTag)
	}
	return nil
}

func validLeaderboardClanHistoryValues(
	locationID string,
	date time.Time,
	tag string,
	name string,
	badgeToken string,
	level int,
	points int,
	members int,
	clanLocationID *int,
	rank int,
	previousRank *int,
) bool {
	return validLeaderboardHistoryLocation(locationID) &&
		!date.IsZero() &&
		date.Equal(dayStart(date)) &&
		strings.TrimSpace(tag) != "" &&
		strings.TrimSpace(name) != "" &&
		strings.TrimSpace(badgeToken) != "" &&
		level > 0 &&
		points >= 0 &&
		members >= 0 &&
		members <= 50 &&
		validOptionalPositiveHistoryInt(clanLocationID) &&
		rank > 0 &&
		validOptionalPositiveHistoryInt(previousRank)
}

func validLeaderboardPlayerClan(tag, name, token *string) bool {
	if tag == nil && name == nil && token == nil {
		return true
	}
	return tag != nil &&
		name != nil &&
		token != nil &&
		strings.TrimSpace(*tag) != "" &&
		strings.TrimSpace(*name) != "" &&
		strings.TrimSpace(*token) != ""
}

func validOptionalPositiveHistoryInt(value *int) bool {
	return value == nil || *value > 0
}

func validLeaderboardHistoryLocation(locationID string) bool {
	if locationID == "global" {
		return true
	}
	location, err := strconv.Atoi(locationID)
	return err == nil && location > 0
}

func validLeaderboardHistoryKind(kind string) bool {
	return isPlayerLeaderboardHistoryKind(kind) || isClanLeaderboardHistoryKind(kind)
}

func isPlayerLeaderboardHistoryKind(kind string) bool {
	switch kind {
	case leaderboardHistoryPlayerHomeTrophies, leaderboardHistoryPlayerBuilderBaseTrophies:
		return true
	default:
		return false
	}
}

func isClanLeaderboardHistoryKind(kind string) bool {
	switch kind {
	case leaderboardHistoryClanHomePoints,
		leaderboardHistoryClanBuilderBasePoints,
		leaderboardHistoryClanCapitalPoints:
		return true
	default:
		return false
	}
}

type timescaleScheduledStore struct {
	pool *pgxpool.Pool
}

func newTimescaleScheduledStore(ctx context.Context, dsn string) (*timescaleScheduledStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleScheduledStore{pool: pool}, nil
}

func (s *timescaleScheduledStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *timescaleScheduledStore) ReplaceLeaderboardHistory(
	ctx context.Context,
	groups []leaderboardHistoryGroup,
) (int, error) {
	if len(groups) == 0 {
		return 0, nil
	}
	batch, err := validateAndFlattenLeaderboardHistoryGroups(groups)
	if err != nil {
		return 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	affected := 0
	for _, spec := range typedLeaderboardHistorySpecs {
		count, err := storeTypedLeaderboardHistoryBatch(ctx, tx, spec, batch)
		if err != nil {
			return 0, err
		}
		affected += count
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return affected, nil
}

func validateAndFlattenLeaderboardHistoryGroups(
	groups []leaderboardHistoryGroup,
) (typedLeaderboardHistoryBatch, error) {
	batch := typedLeaderboardHistoryBatch{
		Scopes: make(map[string][]leaderboardHistoryScope, len(typedLeaderboardHistorySpecs)),
		Rows:   make(map[string][][]any, len(typedLeaderboardHistorySpecs)),
	}
	groupKeys := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		if !validLeaderboardHistoryKind(group.Kind) {
			return typedLeaderboardHistoryBatch{}, fmt.Errorf("unsupported leaderboard history category %q", group.Kind)
		}
		if group.LocationID != "global" {
			location, err := strconv.Atoi(group.LocationID)
			if err != nil || location <= 0 {
				return typedLeaderboardHistoryBatch{}, fmt.Errorf("invalid leaderboard history location %q", group.LocationID)
			}
		}
		groupDate := dayStart(group.Date)
		key := leaderboardHistoryGroupKey(group.Kind, group.LocationID, groupDate)
		if _, exists := groupKeys[key]; exists {
			return typedLeaderboardHistoryBatch{}, fmt.Errorf(
				"duplicate leaderboard history group %s/%s/%s",
				group.Kind,
				group.LocationID,
				groupDate.Format(time.DateOnly),
			)
		}
		groupKeys[key] = struct{}{}
		rows, identities, err := typedLeaderboardHistoryRows(group, groupDate)
		if err != nil {
			return typedLeaderboardHistoryBatch{}, err
		}
		tags := make(map[string]struct{}, len(identities))
		ranks := make(map[int]struct{}, len(identities))
		for _, identity := range identities {
			if _, exists := tags[identity.Tag]; exists {
				return typedLeaderboardHistoryBatch{}, fmt.Errorf(
					"duplicate leaderboard history tag %s for %s/%s/%s",
					identity.Tag,
					group.Kind,
					group.LocationID,
					groupDate.Format(time.DateOnly),
				)
			}
			if _, exists := ranks[identity.Rank]; exists {
				return typedLeaderboardHistoryBatch{}, fmt.Errorf(
					"duplicate leaderboard history rank %d for %s/%s/%s",
					identity.Rank,
					group.Kind,
					group.LocationID,
					groupDate.Format(time.DateOnly),
				)
			}
			tags[identity.Tag] = struct{}{}
			ranks[identity.Rank] = struct{}{}
		}
		batch.Scopes[group.Kind] = append(batch.Scopes[group.Kind], leaderboardHistoryScope{
			LocationID: group.LocationID,
			Date:       groupDate,
		})
		batch.Rows[group.Kind] = append(batch.Rows[group.Kind], rows...)
	}
	return batch, nil
}

func leaderboardHistoryGroupKey(kind, locationID string, date time.Time) string {
	return kind + "\x00" + locationID + "\x00" + dayStart(date).Format(time.DateOnly)
}

type leaderboardHistoryRowIdentity struct {
	Tag  string
	Rank int
}

func typedLeaderboardHistoryRows(
	group leaderboardHistoryGroup,
	groupDate time.Time,
) ([][]any, []leaderboardHistoryRowIdentity, error) {
	switch group.Kind {
	case leaderboardHistoryPlayerHomeTrophies:
		items, ok := group.Rows.([]models.PlayerTrophyHistoryRow)
		if !ok {
			return nil, nil, fmt.Errorf("player trophy history group has row type %T", group.Rows)
		}
		rows := make([][]any, 0, len(items))
		identities := make([]leaderboardHistoryRowIdentity, 0, len(items))
		for _, item := range items {
			if item.LocationID != group.LocationID || !dayStart(item.Date).Equal(groupDate) {
				return nil, nil, fmt.Errorf("player trophy history row is outside its group")
			}
			if err := validatePlayerTrophyHistoryRow(item); err != nil {
				return nil, nil, err
			}
			rows = append(rows, []any{
				item.LocationID, groupDate, item.PlayerTag, item.PlayerName, item.ExpLevel,
				item.Trophies, item.AttackWins, item.DefenseWins, item.Rank, item.PreviousRank,
				item.ClanTag, item.ClanName, item.ClanBadgeToken, item.LeagueID,
			})
			identities = append(identities, leaderboardHistoryRowIdentity{Tag: item.PlayerTag, Rank: item.Rank})
		}
		return rows, identities, nil
	case leaderboardHistoryPlayerBuilderBaseTrophies:
		items, ok := group.Rows.([]models.PlayerBuilderBaseTrophyHistoryRow)
		if !ok {
			return nil, nil, fmt.Errorf("builder base trophy history group has row type %T", group.Rows)
		}
		rows := make([][]any, 0, len(items))
		identities := make([]leaderboardHistoryRowIdentity, 0, len(items))
		for _, item := range items {
			if item.LocationID != group.LocationID || !dayStart(item.Date).Equal(groupDate) {
				return nil, nil, fmt.Errorf("builder base trophy history row is outside its group")
			}
			if err := validatePlayerBuilderBaseTrophyHistoryRow(item); err != nil {
				return nil, nil, err
			}
			rows = append(rows, []any{
				item.LocationID, groupDate, item.PlayerTag, item.PlayerName, item.ExpLevel,
				item.BuilderBaseTrophies, item.BuilderBaseBattleWins, item.Rank, item.PreviousRank,
				item.ClanTag, item.ClanName, item.ClanBadgeToken, item.LeagueID,
			})
			identities = append(identities, leaderboardHistoryRowIdentity{Tag: item.PlayerTag, Rank: item.Rank})
		}
		return rows, identities, nil
	case leaderboardHistoryClanHomePoints:
		items, ok := group.Rows.([]models.ClanTrophyHistoryRow)
		if !ok {
			return nil, nil, fmt.Errorf("clan trophy history group has row type %T", group.Rows)
		}
		rows := make([][]any, 0, len(items))
		identities := make([]leaderboardHistoryRowIdentity, 0, len(items))
		for _, item := range items {
			if item.LocationID != group.LocationID || !dayStart(item.Date).Equal(groupDate) {
				return nil, nil, fmt.Errorf("clan trophy history row is outside its group")
			}
			if err := validateClanTrophyHistoryRow(item); err != nil {
				return nil, nil, err
			}
			rows = append(rows, []any{
				item.LocationID, groupDate, item.ClanTag, item.ClanName, item.ClanBadgeToken,
				item.ClanLevel, item.ClanPoints, item.Members, item.ClanLocationID, item.Rank, item.PreviousRank,
			})
			identities = append(identities, leaderboardHistoryRowIdentity{Tag: item.ClanTag, Rank: item.Rank})
		}
		return rows, identities, nil
	case leaderboardHistoryClanBuilderBasePoints:
		items, ok := group.Rows.([]models.ClanBuilderBaseTrophyHistoryRow)
		if !ok {
			return nil, nil, fmt.Errorf("clan builder base trophy history group has row type %T", group.Rows)
		}
		rows := make([][]any, 0, len(items))
		identities := make([]leaderboardHistoryRowIdentity, 0, len(items))
		for _, item := range items {
			if item.LocationID != group.LocationID || !dayStart(item.Date).Equal(groupDate) {
				return nil, nil, fmt.Errorf("clan builder base trophy history row is outside its group")
			}
			if err := validateClanBuilderBaseTrophyHistoryRow(item); err != nil {
				return nil, nil, err
			}
			rows = append(rows, []any{
				item.LocationID, groupDate, item.ClanTag, item.ClanName, item.ClanBadgeToken,
				item.ClanLevel, item.BuilderBasePoints, item.Members, item.ClanLocationID, item.Rank, item.PreviousRank,
			})
			identities = append(identities, leaderboardHistoryRowIdentity{Tag: item.ClanTag, Rank: item.Rank})
		}
		return rows, identities, nil
	case leaderboardHistoryClanCapitalPoints:
		items, ok := group.Rows.([]models.ClanCapitalHistoryRow)
		if !ok {
			return nil, nil, fmt.Errorf("clan capital history group has row type %T", group.Rows)
		}
		rows := make([][]any, 0, len(items))
		identities := make([]leaderboardHistoryRowIdentity, 0, len(items))
		for _, item := range items {
			if item.LocationID != group.LocationID || !dayStart(item.Date).Equal(groupDate) {
				return nil, nil, fmt.Errorf("clan capital history row is outside its group")
			}
			if err := validateClanCapitalHistoryRow(item); err != nil {
				return nil, nil, err
			}
			rows = append(rows, []any{
				item.LocationID, groupDate, item.ClanTag, item.ClanName, item.ClanBadgeToken,
				item.ClanLevel, item.CapitalPoints, item.Members, item.ClanLocationID, item.Rank, item.PreviousRank,
			})
			identities = append(identities, leaderboardHistoryRowIdentity{Tag: item.ClanTag, Rank: item.Rank})
		}
		return rows, identities, nil
	default:
		return nil, nil, fmt.Errorf("unsupported leaderboard history category %q", group.Kind)
	}
}

func storeTypedLeaderboardHistoryBatch(
	ctx context.Context,
	tx pgx.Tx,
	spec typedLeaderboardHistorySpec,
	batch typedLeaderboardHistoryBatch,
) (int, error) {
	scopes := batch.Scopes[spec.Kind]
	if len(scopes) == 0 {
		return 0, nil
	}
	stageTable := spec.Table + "_stage"
	groupsTable := spec.Table + "_groups_stage"
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE TEMP TABLE %s
		(LIKE %s)
		ON COMMIT DROP
	`, quoteHistoryIdentifier(stageTable), quoteHistoryIdentifier(spec.Table))); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE TEMP TABLE %s (
			location_id text NOT NULL,
			date date NOT NULL,
			PRIMARY KEY (location_id, date)
		)
		ON COMMIT DROP
	`, quoteHistoryIdentifier(groupsTable))); err != nil {
		return 0, err
	}
	if _, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{groupsTable},
		[]string{"location_id", "date"},
		pgx.CopyFromSlice(len(scopes), func(index int) ([]any, error) {
			scope := scopes[index]
			return []any{scope.LocationID, scope.Date}, nil
		}),
	); err != nil {
		return 0, err
	}
	rows := batch.Rows[spec.Kind]
	if len(rows) > 0 {
		if _, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{stageTable},
			spec.Columns,
			pgx.CopyFromRows(rows),
		); err != nil {
			return 0, err
		}
	}
	upserted, err := tx.Exec(ctx, typedLeaderboardHistoryUpsertSQL(spec))
	if err != nil {
		return 0, err
	}
	deleted, err := tx.Exec(ctx, typedLeaderboardHistoryDeleteSQL(spec))
	if err != nil {
		return 0, err
	}
	return int(upserted.RowsAffected() + deleted.RowsAffected()), nil
}

func typedLeaderboardHistoryUpsertSQL(spec typedLeaderboardHistorySpec) string {
	columns := quoteHistoryIdentifiers(spec.Columns)
	assignments := make([]string, 0, len(spec.UpdateCols))
	changes := make([]string, 0, len(spec.UpdateCols))
	target := quoteHistoryIdentifier(spec.Table)
	for _, column := range spec.UpdateCols {
		quoted := quoteHistoryIdentifier(column)
		assignments = append(assignments, quoted+" = EXCLUDED."+quoted)
		changes = append(changes, target+"."+quoted+" IS DISTINCT FROM EXCLUDED."+quoted)
	}
	return fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s FROM %s
		ON CONFLICT (location_id, date, %s) DO UPDATE SET
			%s
		WHERE %s
	`,
		target,
		strings.Join(columns, ", "),
		strings.Join(columns, ", "),
		quoteHistoryIdentifier(spec.Table+"_stage"),
		quoteHistoryIdentifier(spec.TagColumn),
		strings.Join(assignments, ", "),
		strings.Join(changes, " OR "),
	)
}

func typedLeaderboardHistoryDeleteSQL(spec typedLeaderboardHistorySpec) string {
	target := quoteHistoryIdentifier(spec.Table)
	stage := quoteHistoryIdentifier(spec.Table + "_stage")
	groups := quoteHistoryIdentifier(spec.Table + "_groups_stage")
	tag := quoteHistoryIdentifier(spec.TagColumn)
	return fmt.Sprintf(`
		DELETE FROM %s AS current
		USING %s AS groups
		WHERE current.location_id = groups.location_id
		  AND current.date = groups.date
		  AND NOT EXISTS (
			SELECT 1
			FROM %s AS stage
			WHERE stage.location_id = current.location_id
			  AND stage.date = current.date
			  AND stage.%s = current.%s
		  )
	`, target, groups, stage, tag, tag)
}

func quoteHistoryIdentifier(value string) string {
	return pgx.Identifier{value}.Sanitize()
}

func quoteHistoryIdentifiers(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, quoteHistoryIdentifier(value))
	}
	return out
}

var legendHistoryColumns = []string{
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

const upsertLegendHistorySQL = `
	INSERT INTO legend_history (
		season, player_tag, player_name, exp_level, trophies,
		attack_wins, defense_wins, rank, clan_tag, clan_name,
		clan_badge_token, league_tier_id
	)
	SELECT
		season, player_tag, player_name, exp_level, trophies,
		attack_wins, defense_wins, rank, clan_tag, clan_name,
		clan_badge_token, league_tier_id
	FROM legend_history_stage
	ON CONFLICT (season, player_tag) DO UPDATE SET
		player_name = EXCLUDED.player_name,
		exp_level = EXCLUDED.exp_level,
		trophies = EXCLUDED.trophies,
		attack_wins = EXCLUDED.attack_wins,
		defense_wins = EXCLUDED.defense_wins,
		rank = EXCLUDED.rank,
		clan_tag = EXCLUDED.clan_tag,
		clan_name = EXCLUDED.clan_name,
		clan_badge_token = EXCLUDED.clan_badge_token,
		league_tier_id = EXCLUDED.league_tier_id
	WHERE
		legend_history.player_name IS DISTINCT FROM EXCLUDED.player_name OR
		legend_history.exp_level IS DISTINCT FROM EXCLUDED.exp_level OR
		legend_history.trophies IS DISTINCT FROM EXCLUDED.trophies OR
		legend_history.attack_wins IS DISTINCT FROM EXCLUDED.attack_wins OR
		legend_history.defense_wins IS DISTINCT FROM EXCLUDED.defense_wins OR
		legend_history.rank IS DISTINCT FROM EXCLUDED.rank OR
		legend_history.clan_tag IS DISTINCT FROM EXCLUDED.clan_tag OR
		legend_history.clan_name IS DISTINCT FROM EXCLUDED.clan_name OR
		legend_history.clan_badge_token IS DISTINCT FROM EXCLUDED.clan_badge_token OR
		legend_history.league_tier_id IS DISTINCT FROM EXCLUDED.league_tier_id
`

const deleteStaleLegendHistorySQL = `
	DELETE FROM legend_history AS current
	WHERE current.season = $1
	  AND NOT EXISTS (
		SELECT 1
		FROM legend_history_stage AS stage
		WHERE stage.season = current.season
		  AND stage.player_tag = current.player_tag
	  )
`

func (s *timescaleScheduledStore) CompletedLegendSeasons(ctx context.Context) (map[string]struct{}, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT season
		FROM legend_history
		GROUP BY season
		HAVING count(*) > 0
		   AND min(rank) = 1
		   AND max(rank)::bigint = count(*)
		   AND count(DISTINCT rank) = count(*)
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	completed := make(map[string]struct{})
	for rows.Next() {
		var season string
		if err := rows.Scan(&season); err != nil {
			return nil, err
		}
		completed[season] = struct{}{}
	}
	return completed, rows.Err()
}

func (s *timescaleScheduledStore) ReplaceLegendSeason(
	ctx context.Context,
	season string,
	rows []models.LegendHistoryRow,
) (int, error) {
	if err := validateLegendHistoryRows(season, rows); err != nil {
		return 0, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE legend_history_stage
		(LIKE legend_history)
		ON COMMIT DROP
	`); err != nil {
		return 0, err
	}
	if _, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"legend_history_stage"},
		legendHistoryColumns,
		pgx.CopyFromSlice(len(rows), func(index int) ([]any, error) {
			row := rows[index]
			return []any{
				row.Season,
				row.PlayerTag,
				row.PlayerName,
				row.ExpLevel,
				row.Trophies,
				row.AttackWins,
				row.DefenseWins,
				row.Rank,
				row.ClanTag,
				row.ClanName,
				row.ClanBadgeToken,
				row.LeagueTierID,
			}, nil
		}),
	); err != nil {
		return 0, err
	}
	upserted, err := tx.Exec(ctx, upsertLegendHistorySQL)
	if err != nil {
		return 0, err
	}
	deleted, err := tx.Exec(ctx, deleteStaleLegendHistorySQL, season)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return int(upserted.RowsAffected() + deleted.RowsAffected()), nil
}

func validateLegendHistoryRows(season string, rows []models.LegendHistoryRow) error {
	if _, err := officialLegendSeasonWindow(season); err != nil {
		return err
	}
	if len(rows) == 0 {
		return fmt.Errorf("refusing empty legend season %s replacement", season)
	}
	seenTags := make(map[string]struct{}, len(rows))
	seenRanks := make(map[int]struct{}, len(rows))
	for _, row := range rows {
		if row.Season != season ||
			row.PlayerTag == "" ||
			strings.TrimSpace(row.PlayerName) == "" ||
			row.ExpLevel < 0 ||
			row.Trophies < 0 ||
			row.AttackWins < 0 ||
			row.DefenseWins < 0 ||
			row.Rank <= 0 ||
			!validLegendHistoryClan(row) ||
			(row.LeagueTierID != nil && *row.LeagueTierID <= 0) {
			return fmt.Errorf(
				"invalid legend history row for season %s: player %q name %q rank %d trophies %d",
				season,
				row.PlayerTag,
				row.PlayerName,
				row.Rank,
				row.Trophies,
			)
		}
		if _, duplicate := seenTags[row.PlayerTag]; duplicate {
			return fmt.Errorf("duplicate legend season %s player %s", season, row.PlayerTag)
		}
		if _, duplicate := seenRanks[row.Rank]; duplicate {
			return fmt.Errorf("duplicate legend season %s rank %d", season, row.Rank)
		}
		seenTags[row.PlayerTag] = struct{}{}
		seenRanks[row.Rank] = struct{}{}
	}
	for rank := 1; rank <= len(rows); rank++ {
		if _, exists := seenRanks[rank]; !exists {
			return fmt.Errorf("incomplete legend season %s rows: missing rank %d", season, rank)
		}
	}
	return nil
}

func validLegendHistoryClan(row models.LegendHistoryRow) bool {
	if row.ClanTag == nil && row.ClanName == nil && row.ClanBadgeToken == nil {
		return true
	}
	return row.ClanTag != nil &&
		row.ClanName != nil &&
		row.ClanBadgeToken != nil &&
		strings.TrimSpace(*row.ClanTag) != "" &&
		strings.TrimSpace(*row.ClanName) != "" &&
		strings.TrimSpace(*row.ClanBadgeToken) != ""
}

const replaceCurrentClanRankingGroupSQL = `
	INSERT INTO clan_rankings_current (
		clan_tag, ranking_type, location_id, rank, points, updated_at
	)
	SELECT clan_tag, $1, $2, rank, points, updated_at
	FROM clan_rankings_current_stage
	ON CONFLICT (clan_tag, ranking_type, location_id) DO UPDATE SET
		rank = EXCLUDED.rank,
		points = EXCLUDED.points,
		updated_at = EXCLUDED.updated_at
`

const deleteStaleCurrentClanRankingGroupSQL = `
	DELETE FROM clan_rankings_current AS current
	WHERE current.ranking_type = $1
	  AND current.location_id = $2
	  AND NOT EXISTS (
		SELECT 1
		FROM clan_rankings_current_stage AS stage
		WHERE stage.clan_tag = current.clan_tag
	  )
`

func (s *timescaleScheduledStore) ReplaceCurrentClanRankingGroup(
	ctx context.Context,
	group currentClanRankingGroup,
) (int, error) {
	if len(group.Rows) > currentClanRankingLimit {
		return 0, fmt.Errorf(
			"refusing oversized %s clan ranking replacement for %s: got %d rows, limit %d",
			group.RankingType,
			group.LocationID,
			len(group.Rows),
			currentClanRankingLimit,
		)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE clan_rankings_current_stage (
			clan_tag text NOT NULL,
			rank integer NOT NULL,
			points integer NOT NULL,
			updated_at timestamp with time zone NOT NULL
		) ON COMMIT DROP
	`); err != nil {
		return 0, err
	}
	if len(group.Rows) > 0 {
		if _, err := tx.CopyFrom(
			ctx,
			pgx.Identifier{"clan_rankings_current_stage"},
			[]string{"clan_tag", "rank", "points", "updated_at"},
			pgx.CopyFromSlice(len(group.Rows), func(index int) ([]any, error) {
				row := group.Rows[index]
				return []any{row.ClanTag, row.Rank, row.Points, row.UpdatedAt}, nil
			}),
		); err != nil {
			return 0, err
		}
	}
	if _, err := tx.Exec(ctx, replaceCurrentClanRankingGroupSQL, group.RankingType, group.LocationID); err != nil {
		return 0, err
	}
	commandTag, err := tx.Exec(ctx, deleteStaleCurrentClanRankingGroupSQL, group.RankingType, group.LocationID)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(group.Rows) + int(commandTag.RowsAffected()), nil
}

func (s *timescaleScheduledStore) ListRankedGroupTargets(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT tag
		FROM basic_player
		WHERE tag <> ''
		ORDER BY tag
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tags := make([]string, 0)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		if tag != "" {
			tags = append(tags, tag)
		}
	}
	return tags, rows.Err()
}

func (s *timescaleScheduledStore) StorePlayerProfiles(ctx context.Context, profiles []models.PlayerProfileIngest) (int, error) {
	if len(profiles) == 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	affected, err := utils.UpsertPlayerProfiles(ctx, tx, profiles, scheduledDomainName)
	if err != nil {
		return affected, err
	}
	if err := tx.Commit(ctx); err != nil {
		return affected, err
	}
	return affected, nil
}

func (s *timescaleScheduledStore) DeletePlayers(ctx context.Context, tags []string) error {
	if len(tags) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := utils.DeletePlayers(ctx, tx, tags); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *timescaleScheduledStore) StoreRankedLeagueGroupMembers(ctx context.Context, rows []models.RankedLeagueGroupMemberRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO ranked_league_group_members (
				season_id, group_tag, league_tier_id,
				player_tag, player_name, clan_tag, clan_name,
				placement, league_trophies,
				attack_win_count, attack_lose_count,
				defense_win_count, defense_lose_count
			)
			VALUES (
				$1, $2, $3,
				$4, $5, NULLIF($6, ''), NULLIF($7, ''),
				$8, $9,
				$10, $11,
				$12, $13
			)
			ON CONFLICT (season_id, group_tag, player_tag) DO UPDATE SET
				league_tier_id = EXCLUDED.league_tier_id,
				player_name = EXCLUDED.player_name,
				clan_tag = EXCLUDED.clan_tag,
				clan_name = EXCLUDED.clan_name,
				placement = EXCLUDED.placement,
				league_trophies = EXCLUDED.league_trophies,
				attack_win_count = EXCLUDED.attack_win_count,
				attack_lose_count = EXCLUDED.attack_lose_count,
				defense_win_count = EXCLUDED.defense_win_count,
				defense_lose_count = EXCLUDED.defense_lose_count
			WHERE
				ranked_league_group_members.league_tier_id IS DISTINCT FROM EXCLUDED.league_tier_id OR
				ranked_league_group_members.player_name IS DISTINCT FROM EXCLUDED.player_name OR
				ranked_league_group_members.clan_tag IS DISTINCT FROM EXCLUDED.clan_tag OR
				ranked_league_group_members.clan_name IS DISTINCT FROM EXCLUDED.clan_name OR
				ranked_league_group_members.placement IS DISTINCT FROM EXCLUDED.placement OR
				ranked_league_group_members.league_trophies IS DISTINCT FROM EXCLUDED.league_trophies OR
				ranked_league_group_members.attack_win_count IS DISTINCT FROM EXCLUDED.attack_win_count OR
				ranked_league_group_members.attack_lose_count IS DISTINCT FROM EXCLUDED.attack_lose_count OR
				ranked_league_group_members.defense_win_count IS DISTINCT FROM EXCLUDED.defense_win_count OR
				ranked_league_group_members.defense_lose_count IS DISTINCT FROM EXCLUDED.defense_lose_count
		`,
			row.SeasonID, row.GroupTag, row.LeagueTierID,
			row.PlayerTag, row.PlayerName, row.ClanTag, row.ClanName,
			row.Placement, row.LeagueTrophies,
			row.AttackWinCount, row.AttackLoseCount,
			row.DefenseWinCount, row.DefenseLoseCount,
		)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	affected, err := utils.SendBatchCount(ctx, tx, batch)
	if err != nil {
		return affected, err
	}
	if err := tx.Commit(ctx); err != nil {
		return affected, err
	}
	return affected, nil
}

func (s *timescaleScheduledStore) MissingRankedGroupPlayers(ctx context.Context, seasonID int64) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT m.player_tag
		FROM ranked_league_group_members m
		LEFT JOIN basic_player p ON p.tag = m.player_tag
		WHERE m.season_id = $1
		  AND p.tag IS NULL
		ORDER BY m.player_tag
	`, seasonID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tags := make([]string, 0)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		if tag != "" {
			tags = append(tags, tag)
		}
	}
	return tags, rows.Err()
}

type memoryScheduledStore struct {
	currentClanRankings map[string]map[string]currentClanRankingRow
	leaderboardHistory  map[string]map[string]any
	legendHistory       map[string]map[string]models.LegendHistoryRow
}

func newMemoryScheduledStore() *memoryScheduledStore {
	return &memoryScheduledStore{
		currentClanRankings: make(map[string]map[string]currentClanRankingRow),
		leaderboardHistory:  make(map[string]map[string]any),
		legendHistory:       make(map[string]map[string]models.LegendHistoryRow),
	}
}

func (*memoryScheduledStore) Close() {}

func (s *memoryScheduledStore) ReplaceLeaderboardHistory(
	_ context.Context,
	groups []leaderboardHistoryGroup,
) (int, error) {
	if _, err := validateAndFlattenLeaderboardHistoryGroups(groups); err != nil {
		return 0, err
	}
	affected := 0
	for _, group := range groups {
		key := leaderboardHistoryGroupKey(group.Kind, group.LocationID, group.Date)
		replacement, err := memoryLeaderboardHistoryReplacement(group)
		if err != nil {
			return 0, err
		}
		for tag := range s.leaderboardHistory[key] {
			if _, exists := replacement[tag]; !exists {
				affected++
			}
		}
		affected += len(replacement)
		s.leaderboardHistory[key] = replacement
	}
	return affected, nil
}

func memoryLeaderboardHistoryReplacement(group leaderboardHistoryGroup) (map[string]any, error) {
	replacement := make(map[string]any)
	switch rows := group.Rows.(type) {
	case []models.PlayerTrophyHistoryRow:
		for _, row := range rows {
			replacement[row.PlayerTag] = row
		}
	case []models.PlayerBuilderBaseTrophyHistoryRow:
		for _, row := range rows {
			replacement[row.PlayerTag] = row
		}
	case []models.ClanTrophyHistoryRow:
		for _, row := range rows {
			replacement[row.ClanTag] = row
		}
	case []models.ClanBuilderBaseTrophyHistoryRow:
		for _, row := range rows {
			replacement[row.ClanTag] = row
		}
	case []models.ClanCapitalHistoryRow:
		for _, row := range rows {
			replacement[row.ClanTag] = row
		}
	default:
		return nil, fmt.Errorf("unsupported memory leaderboard history rows %T", group.Rows)
	}
	return replacement, nil
}

func (s *memoryScheduledStore) CompletedLegendSeasons(context.Context) (map[string]struct{}, error) {
	completed := make(map[string]struct{})
	for season, stored := range s.legendHistory {
		rows := make([]models.LegendHistoryRow, 0, len(stored))
		for _, row := range stored {
			rows = append(rows, row)
		}
		if validateLegendHistoryRows(season, rows) == nil {
			completed[season] = struct{}{}
		}
	}
	return completed, nil
}

func (s *memoryScheduledStore) ReplaceLegendSeason(
	_ context.Context,
	season string,
	rows []models.LegendHistoryRow,
) (int, error) {
	if err := validateLegendHistoryRows(season, rows); err != nil {
		return 0, err
	}
	replacement := make(map[string]models.LegendHistoryRow, len(rows))
	for _, row := range rows {
		replacement[row.PlayerTag] = row
	}
	affected := len(replacement)
	for playerTag := range s.legendHistory[season] {
		if _, exists := replacement[playerTag]; !exists {
			affected++
		}
	}
	s.legendHistory[season] = replacement
	return affected, nil
}

func (s *memoryScheduledStore) ReplaceCurrentClanRankingGroup(
	_ context.Context,
	group currentClanRankingGroup,
) (int, error) {
	key := group.RankingType + "\x00" + group.LocationID
	replacement := make(map[string]currentClanRankingRow, len(group.Rows))
	for _, row := range group.Rows {
		replacement[row.ClanTag] = row
	}
	stale := 0
	for clanTag := range s.currentClanRankings[key] {
		if _, ok := replacement[clanTag]; !ok {
			stale++
		}
	}
	s.currentClanRankings[key] = replacement
	return len(replacement) + stale, nil
}

func (*memoryScheduledStore) ListRankedGroupTargets(context.Context) ([]string, error) {
	return nil, nil
}

func (*memoryScheduledStore) StorePlayerProfiles(context.Context, []models.PlayerProfileIngest) (int, error) {
	return 0, nil
}

func (*memoryScheduledStore) DeletePlayers(context.Context, []string) error {
	return nil
}

func (*memoryScheduledStore) StoreRankedLeagueGroupMembers(context.Context, []models.RankedLeagueGroupMemberRow) (int, error) {
	return 0, nil
}

func (*memoryScheduledStore) MissingRankedGroupPlayers(context.Context, int64) ([]string, error) {
	return nil, nil
}
