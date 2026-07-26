package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const scheduledDomainName = "scheduled"
const leaderboardKindCapital = "capital"
const currentClanRankingLimit = 200

type leaderboardLoader func(context.Context, *clashy.Client, string) (any, error)
type currentClanRankingLoader func(context.Context, *clashy.Client, string, clashy.PageOptions) ([]clashy.RankedClan, error)

var leaderboardPaths = []struct {
	Kind string
	Load leaderboardLoader
}{
	{Kind: "clan_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationClansByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
	{Kind: "clan_versus_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationClansBuilderBaseByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
	{Kind: leaderboardKindCapital, Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationClansCapitalByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
	{Kind: "player_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationPlayersByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
	{Kind: "player_versus_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID string) (any, error) {
		return client.GetLocationPlayersBuilderBaseByLocationID(ctx, locationID, clashy.PageOptions{})
	}},
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

type scheduledDomain struct {
	store                   scheduledStore
	lastRankedGroupSeasonID int64
}

type scheduledStore interface {
	Close()
	StoreSnapshots(context.Context, []models.LeaderboardSnapshotItemRow) error
	ReplaceCurrentClanRankingGroup(context.Context, currentClanRankingGroup) (int, error)
	ListRankedGroupTargets(context.Context) ([]string, error)
	StorePlayerProfiles(context.Context, []models.PlayerProfileIngest, *time.Time) (int, error)
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

	interval := time.Duration(app.Config.ScheduledIntervalSeconds) * time.Second
	for {
		start := time.Now()
		err = d.runCycle(ctx, app)
		app.Stats.RecordProcess(scheduledDomainName, time.Since(start))
		if err != nil {
			app.Stats.SetReady(scheduledDomainName, false, err.Error())
		}
		if err := sleepOrDone(ctx, interval); err != nil {
			return err
		}
	}
}

func validateScheduledConfig(cfg platform.Config) error {
	if cfg.ScheduledIntervalSeconds <= 0 {
		return errors.New("scheduled.interval_seconds must be greater than zero")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required for scheduled")
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
	items, err := d.doLeaderboardSnapshots(ctx, app, locationIDs, now)
	if err != nil {
		cycleErrors = append(cycleErrors, err)
	} else if err := d.store.StoreSnapshots(ctx, items); err != nil {
		cycleErrors = append(cycleErrors, err)
	} else {
		app.Stats.RecordWrite(scheduledDomainName, len(items))
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
		affected, err := d.store.StorePlayerProfiles(ctx, []models.PlayerProfileIngest{utils.PlayerProfileFromClashy(*player)}, nil)
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

func (d *scheduledDomain) fetchAndStoreMissingRankedPlayers(ctx context.Context, app *platform.App, seasonID int64, activityAt time.Time) (int, error) {
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
		affected, err := d.store.StorePlayerProfiles(ctx, profiles, &activityAt)
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

func (d *scheduledDomain) doLeaderboardSnapshots(
	ctx context.Context,
	app *platform.App,
	locationIDs []string,
	now time.Time,
) ([]models.LeaderboardSnapshotItemRow, error) {
	date := dayStart(now)
	itemRows := make([]models.LeaderboardSnapshotItemRow, 0, len(leaderboardPaths)*len(locationIDs)*200)
	for _, locationID := range locationIDs {
		for _, item := range leaderboardPaths {
			if !shouldStoreLeaderboardKind(item.Kind, now) {
				continue
			}
			payload, err := platform.RetryClashFetch(ctx, func(fetchCtx context.Context) (any, error) {
				start := time.Now()
				payload, err := item.Load(fetchCtx, app.Clash, locationID)
				app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
				return payload, err
			})
			if err != nil {
				continue
			}
			if !leaderboardPayloadHasItems(payload) {
				continue
			}
			itemRows = append(itemRows, leaderboardSnapshotItems(item.Kind, locationID, date, payload)...)
		}
	}
	return itemRows, nil
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

func leaderboardPayloadHasItems(payload any) bool {
	value := reflect.ValueOf(payload)
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return false
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice:
		return value.Len() > 0
	default:
		return true
	}
}

func shouldStoreLeaderboardKind(kind string, now time.Time) bool {
	if kind != leaderboardKindCapital {
		return true
	}
	return now.UTC().Weekday() == time.Tuesday
}

func leaderboardSnapshotItems(
	kind string,
	locationID string,
	date time.Time,
	payload any,
) []models.LeaderboardSnapshotItemRow {
	switch items := payload.(type) {
	case []clashy.RankedClan:
		out := make([]models.LeaderboardSnapshotItemRow, 0, len(items))
		for _, item := range items {
			out = append(out, models.LeaderboardSnapshotItemRow{
				Kind:       kind,
				LocationID: locationID,
				Date:       date,
				Tag:        item.Tag,
				Name:       item.Name,
				Rank:       item.Rank,
				Data:       jsonAny(item),
			})
		}
		return out
	case []clashy.RankedPlayer:
		out := make([]models.LeaderboardSnapshotItemRow, 0, len(items))
		for _, item := range items {
			out = append(out, models.LeaderboardSnapshotItemRow{
				Kind:       kind,
				LocationID: locationID,
				Date:       date,
				Tag:        item.Tag,
				Name:       item.Name,
				Rank:       item.Rank,
				Data:       jsonAny(item),
			})
		}
		return out
	default:
		return nil
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

func (s *timescaleScheduledStore) StoreSnapshots(
	ctx context.Context,
	items []models.LeaderboardSnapshotItemRow,
) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE leaderboard_snapshot_items_stage
		(LIKE leaderboard_snapshot_items)
		ON COMMIT DROP
	`); err != nil {
		return err
	}
	_, err = tx.CopyFrom(
		ctx,
		pgx.Identifier{"leaderboard_snapshot_items_stage"},
		[]string{"kind", "location_id", "date", "tag", "name", "rank", "data"},
		pgx.CopyFromSlice(len(items), func(index int) ([]any, error) {
			item := items[index]
			raw, err := json.Marshal(item.Data)
			if err != nil {
				return nil, err
			}
			return []any{item.Kind, item.LocationID, item.Date, item.Tag, item.Name, item.Rank, string(raw)}, nil
		}),
	)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO leaderboard_snapshot_items (
			kind, location_id, date, tag, name, rank, data
		)
		SELECT kind, location_id, date, tag, name, rank, data
		FROM leaderboard_snapshot_items_stage
		ON CONFLICT (kind, location_id, date, tag) DO UPDATE SET
			name = EXCLUDED.name,
			rank = EXCLUDED.rank,
			data = EXCLUDED.data
		WHERE
			leaderboard_snapshot_items.name IS DISTINCT FROM EXCLUDED.name OR
			leaderboard_snapshot_items.rank IS DISTINCT FROM EXCLUDED.rank OR
			leaderboard_snapshot_items.data IS DISTINCT FROM EXCLUDED.data
	`); err != nil {
		return err
	}
	return tx.Commit(ctx)
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

func (s *timescaleScheduledStore) StorePlayerProfiles(ctx context.Context, profiles []models.PlayerProfileIngest, activityAt *time.Time) (int, error) {
	if len(profiles) == 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	affected, err := utils.UpsertPlayerProfiles(ctx, tx, profiles, scheduledDomainName, activityAt)
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
}

func newMemoryScheduledStore() *memoryScheduledStore {
	return &memoryScheduledStore{currentClanRankings: make(map[string]map[string]currentClanRankingRow)}
}

func (*memoryScheduledStore) Close() {}

func (*memoryScheduledStore) StoreSnapshots(
	context.Context,
	[]models.LeaderboardSnapshotItemRow,
) error {
	return nil
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

func (*memoryScheduledStore) StorePlayerProfiles(context.Context, []models.PlayerProfileIngest, *time.Time) (int, error) {
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

func jsonAny(value any) any {
	raw, _ := json.Marshal(value)
	var out any
	_ = json.Unmarshal(raw, &out)
	return out
}
