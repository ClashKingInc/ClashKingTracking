package scripts

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const (
	leaderboardsDomainName                    = "leaderboards"
	unrankedPlayerLeagueID                    = 105000000
	leaderboardIndexKey                       = "leaderboards:index"
	leaderboardLeagueKeyPrefix                = "leaderboards:league:"
	leaderboardTHKeyPrefix                    = "leaderboards:townhall:"
	leaderboardUpdateRetries                  = 3
	leaderboardMaterializedViewRefreshSeconds = 3600
)

var leaderboardCacheScript = valkey.NewLuaScript(`
	for i = 1, #KEYS do
		redis.call('SET', KEYS[i], ARGV[i])
	end
	return #KEYS
`)

const leaderboardCandidateSQL = `
	WITH league_ids AS (
		SELECT generate_series(105000001, 105000036) AS league_id
	),
	townhall_levels AS (
		SELECT generate_series(7, 18) AS townhall_level
	),
	league_candidates AS (
		SELECT
			'league'::text AS board_type,
			league_ids.league_id::text AS board_key,
			players.tag
		FROM league_ids
		CROSS JOIN LATERAL (
			SELECT tag
			FROM basic_player
			WHERE league_id = league_ids.league_id
			  AND league_id IS NOT NULL
			  AND league_id <> 105000000
			  AND trophies > 0
			ORDER BY trophies DESC
			LIMIT $1
		) players
	),
	townhall_candidates AS (
		SELECT
			'townhall'::text AS board_type,
			townhall_levels.townhall_level::text AS board_key,
			players.tag
		FROM townhall_levels
		CROSS JOIN LATERAL (
			SELECT tag
			FROM basic_player
			WHERE townhall_level = townhall_levels.townhall_level
			  AND townhall_level >= 7
			  AND league_id IS NOT NULL
			  AND league_id <> 105000000
			  AND trophies > 0
			ORDER BY league_id DESC, trophies DESC
			LIMIT $1
		) players
	)
	SELECT board_type, board_key, tag
	FROM league_candidates
	UNION ALL
	SELECT board_type, board_key, tag
	FROM townhall_candidates
	ORDER BY board_type, board_key, tag
`

type leaderboardsDomain struct {
	store                       *timescaleLeaderboardStore
	limit                       int
	nullAssetURL                string
	nextMaterializedViewRefresh time.Time
}

type leaderboardCandidate struct {
	BoardType string
	BoardKey  string
	Tag       string
}

type timescaleLeaderboardStore struct {
	pool   *pgxpool.Pool
	valkey valkey.Client
}

type leaderboardCacheSet struct {
	Leagues     []string                  `json:"leagues"`
	TownHalls   []string                  `json:"townhalls"`
	Boards      []leaderboardBoardPayload `json:"-"`
	GeneratedAt time.Time                 `json:"generated_at"`
}

type leaderboardBoardPayload struct {
	Kind        string                     `json:"-"`
	Key         string                     `json:"-"`
	Items       []leaderboardPlayerPayload `json:"items"`
	GeneratedAt time.Time                  `json:"generated_at"`
}

type leaderboardPlayerPayload struct {
	Rank     int                      `json:"rank"`
	Tag      string                   `json:"tag"`
	Name     string                   `json:"name"`
	Clan     *leaderboardClanPayload  `json:"clan"`
	League   leaderboardLeaguePayload `json:"league"`
	TownHall int                      `json:"townhall_level"`
	Trophies int                      `json:"trophies"`
}

type leaderboardLeaguePayload struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Badge string `json:"badge"`
}

type leaderboardClanMetadata struct {
	Name     string
	BadgeURL string
}

type leaderboardClanPayload struct {
	Tag   string  `json:"tag"`
	Name  *string `json:"name"`
	Badge string  `json:"badge"`
}

type leaderboardPlayerRow struct {
	models.BasicPlayerRow
	League leaderboardLeaguePayload
}

func NewLeaderboardsDomain() platform.Domain { return &leaderboardsDomain{} }

func (d *leaderboardsDomain) Name() string { return leaderboardsDomainName }

func (d *leaderboardsDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateLeaderboardsConfig(app.Config); err != nil {
		return err
	}
	d.limit = app.Config.LeaderboardLimit
	d.nullAssetURL = app.Config.LeaderboardNullAssetURL
	store, err := d.openStore(ctx, app)
	if err != nil {
		return err
	}
	d.store = store
	defer store.Close()

	interval := time.Duration(app.Config.LeaderboardIntervalSeconds) * time.Second
	for {
		start := time.Now()
		err := d.runCycle(ctx, app)
		if refreshErr := d.refreshMaterializedViewsIfDue(ctx, app, time.Now().UTC()); refreshErr != nil {
			err = errors.Join(err, refreshErr)
		}
		app.Stats.RecordProcess(leaderboardsDomainName, time.Since(start))
		if err != nil {
			app.Logger.Error("leaderboards cycle failed", "err", err)
			app.Stats.SetReady(leaderboardsDomainName, false, err.Error())
		} else {
			app.Stats.SetReady(leaderboardsDomainName, true, "")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

func validateLeaderboardsConfig(cfg platform.Config) error {
	if cfg.LeaderboardRequestsPerSecond <= 0 {
		return errors.New("leaderboards.requests_per_second must be greater than zero")
	}
	if cfg.LeaderboardIntervalSeconds <= 0 {
		return errors.New("leaderboards.interval_seconds must be greater than zero")
	}
	if cfg.LeaderboardLimit <= 0 {
		return errors.New("leaderboards.limit must be greater than zero")
	}
	if cfg.LeaderboardNullAssetURL == "" {
		return errors.New("leaderboards.null_asset_url is required")
	}
	if cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required for leaderboards")
	}
	if cfg.ValkeyAddr == "" {
		return errors.New("valkey_addr is required for leaderboards")
	}
	return nil
}

func (d *leaderboardsDomain) openStore(ctx context.Context, app *platform.App) (*timescaleLeaderboardStore, error) {
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return nil, err
	}
	return &timescaleLeaderboardStore{pool: pool, valkey: app.Valkey}, nil
}

func (d *leaderboardsDomain) runCycle(ctx context.Context, app *platform.App) error {
	limiter, err := newTrackingLimiter(app.Config.LeaderboardRequestsPerSecond)
	if err != nil {
		return err
	}
	candidates, err := d.store.LoadCandidates(ctx, d.limit)
	if err != nil {
		return err
	}
	tags := leaderboardCandidateTags(candidates)
	if len(tags) == 0 {
		cache := buildLeaderboardCache(candidates, nil, nil, time.Now().UTC(), d.limit, d.nullAssetURL)
		return d.store.CacheBoards(ctx, cache)
	}
	leagues, err := d.fetchLeagues(ctx, app, limiter)
	if err != nil {
		return err
	}
	players, deletedTags, err := d.fetchPlayers(ctx, app, limiter, tags, leagues)
	if err != nil {
		return err
	}
	basicPlayers := leaderboardBasicPlayerRows(players)
	deleted, err := d.store.DeletePlayers(ctx, deletedTags)
	if err != nil {
		return err
	}
	if err := d.store.UpdatePlayers(ctx, basicPlayers); err != nil {
		return err
	}
	clans, err := d.store.LoadClanMetadata(ctx, leaderboardPlayerClanTags(players))
	if err != nil {
		return err
	}
	cache := buildLeaderboardCache(candidates, players, clans, time.Now().UTC(), d.limit, d.nullAssetURL)
	err = d.store.CacheBoards(ctx, cache)
	if err == nil {
		app.Stats.RecordWrite(leaderboardsDomainName, len(basicPlayers)+deleted+len(cache.Boards)+1)
	}
	return err
}

func (d *leaderboardsDomain) refreshMaterializedViewsIfDue(ctx context.Context, app *platform.App, now time.Time) error {
	if !d.nextMaterializedViewRefresh.IsZero() && now.Before(d.nextMaterializedViewRefresh) {
		return nil
	}
	start := time.Now()
	if err := d.store.RefreshMaterializedViews(ctx); err != nil {
		return fmt.Errorf("refresh leaderboard materialized views: %w", err)
	}
	duration := time.Since(start)
	d.nextMaterializedViewRefresh = now.Add(time.Duration(leaderboardMaterializedViewRefreshSeconds) * time.Second)
	app.Logger.Info("leaderboards materialized views refreshed", "duration", duration)
	app.Stats.RecordStore(leaderboardsDomainName, duration, 2, 2)
	return nil
}

func (d *leaderboardsDomain) fetchLeagues(ctx context.Context, app *platform.App, limiter *clashy.Limiter) (map[int]leaderboardLeaguePayload, error) {
	leagues, err := retryLimitedClashFetch(ctx, limiter, func(fetchCtx context.Context) ([]clashy.League, error) {
		start := time.Now()
		leagues, err := app.Clash.SearchLeagues(fetchCtx, clashy.PageOptions{Limit: 100})
		app.Stats.RecordRequest(leaderboardsDomainName, time.Since(start), err)
		return leagues, err
	})
	if err != nil {
		return nil, err
	}
	out := make(map[int]leaderboardLeaguePayload, len(leagues))
	for _, league := range leagues {
		out[league.ID] = leaguePayload(league)
	}
	return out, nil
}

func (d *leaderboardsDomain) fetchPlayers(ctx context.Context, app *platform.App, limiter *clashy.Limiter, tags []string, leagues map[int]leaderboardLeaguePayload) ([]leaderboardPlayerRow, []string, error) {
	var mu sync.Mutex
	players := make([]leaderboardPlayerRow, 0, len(tags))
	deleteTags := make([]string, 0)
	skipped := 0
	notFound := 0
	err := runBounded(ctx, platform.RequestConcurrency(app.Config.LeaderboardRequestsPerSecond), tags, func(workerCtx context.Context, tag string) error {
		player, err := retryLimitedClashFetch(workerCtx, limiter, func(fetchCtx context.Context) (*clashy.Player, error) {
			start := time.Now()
			player, err := app.Clash.GetPlayer(fetchCtx, tag)
			app.Stats.RecordRequest(leaderboardsDomainName, time.Since(start), err)
			return player, err
		})
		if err != nil {
			var missing *clashy.NotFound
			mu.Lock()
			skipped++
			if errors.As(err, &missing) {
				notFound++
				deleteTags = append(deleteTags, tag)
			}
			mu.Unlock()
			return nil
		}
		row, ok := basicPlayerRowFromPlayer(player, leagues)
		if !ok {
			return nil
		}
		mu.Lock()
		players = append(players, row)
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if len(players) == 0 && len(tags) > 0 {
		return nil, nil, fmt.Errorf("leaderboards player refresh returned no usable players from %d candidates", len(tags))
	}
	if skipped > 0 {
		app.Logger.Warn("leaderboards player refresh skipped players", "skipped", skipped, "not_found", notFound, "candidates", len(tags))
	}
	sort.Slice(players, func(i, j int) bool { return players[i].Tag < players[j].Tag })
	sort.Strings(deleteTags)
	return players, deleteTags, nil
}

func basicPlayerRowFromPlayer(player *clashy.Player, leagues map[int]leaderboardLeaguePayload) (leaderboardPlayerRow, bool) {
	if player == nil || player.Tag == "" || player.Name == "" || player.TownHall <= 0 {
		return leaderboardPlayerRow{}, false
	}
	clanTag := ""
	if player.Clan != nil {
		clanTag = player.Clan.Tag
	}
	return leaderboardPlayerRow{
		BasicPlayerRow: models.BasicPlayerRow{
			Tag:          player.Tag,
			Name:         player.Name,
			LeagueID:     player.LeagueTier.ID,
			ClanTag:      clanTag,
			ClanTagKnown: true,
			TownHall:     player.TownHall,
			Trophies:     player.Trophies,
		},
		League: leaguePayloadForPlayer(player.LeagueTier, leagues),
	}, true
}

func leaguePayloadForPlayer(league clashy.League, leagues map[int]leaderboardLeaguePayload) leaderboardLeaguePayload {
	if payload, ok := leagues[league.ID]; ok {
		return payload
	}
	return leaguePayload(league)
}

func leaderboardBasicPlayerRows(players []leaderboardPlayerRow) []models.BasicPlayerRow {
	out := make([]models.BasicPlayerRow, 0, len(players))
	for _, player := range players {
		out = append(out, player.BasicPlayerRow)
	}
	return out
}

func leaderboardPlayerClanTags(players []leaderboardPlayerRow) []string {
	seen := map[string]struct{}{}
	for _, player := range players {
		if player.ClanTag == "" {
			continue
		}
		seen[player.ClanTag] = struct{}{}
	}
	tags := make([]string, 0, len(seen))
	for tag := range seen {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	return tags
}

func leaguePayload(league clashy.League) leaderboardLeaguePayload {
	out := leaderboardLeaguePayload{ID: league.ID, Name: league.Name}
	if league.Icon != nil {
		out.Badge = firstNonEmptyString(league.Icon.Medium, league.Icon.Small, league.Icon.Tiny)
	}
	return out
}

func leaderboardCandidateTags(candidates []leaderboardCandidate) []string {
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate.Tag == "" {
			continue
		}
		seen[candidate.Tag] = struct{}{}
	}
	tags := make([]string, 0, len(seen))
	for tag := range seen {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	return tags
}

func buildLeaderboardCache(
	candidates []leaderboardCandidate,
	players []leaderboardPlayerRow,
	clans map[string]leaderboardClanMetadata,
	now time.Time,
	limit int,
	nullAssetURL string,
) leaderboardCacheSet {
	leagueKeys := map[string]struct{}{}
	townhallKeys := map[string]struct{}{}
	for _, candidate := range candidates {
		switch candidate.BoardType {
		case "league":
			if candidate.BoardKey != strconv.Itoa(unrankedPlayerLeagueID) {
				leagueKeys[candidate.BoardKey] = struct{}{}
			}
		case "townhall":
			if townhall, err := strconv.Atoi(candidate.BoardKey); err == nil && townhall >= 7 {
				townhallKeys[candidate.BoardKey] = struct{}{}
			}
		}
	}
	leagueBoards := map[string][]leaderboardPlayerRow{}
	townhallBoards := map[string][]leaderboardPlayerRow{}
	for _, player := range players {
		if player.LeagueID != 0 && player.LeagueID != unrankedPlayerLeagueID && player.Trophies > 0 {
			key := strconv.Itoa(player.LeagueID)
			leagueKeys[key] = struct{}{}
			leagueBoards[key] = append(leagueBoards[key], player)
		}
		if player.TownHall >= 7 && player.LeagueID != 0 && player.LeagueID != unrankedPlayerLeagueID && player.Trophies > 0 {
			key := strconv.Itoa(player.TownHall)
			townhallKeys[key] = struct{}{}
			townhallBoards[key] = append(townhallBoards[key], player)
		}
	}

	cache := leaderboardCacheSet{GeneratedAt: now}
	cache.Leagues = sortedKeys(leagueKeys)
	cache.TownHalls = sortedNumericKeys(townhallKeys)
	for _, key := range cache.Leagues {
		cache.Boards = append(cache.Boards, leaderboardBoard("league", key, leagueBoards[key], clans, now, limit, nullAssetURL))
	}
	for _, key := range cache.TownHalls {
		cache.Boards = append(cache.Boards, leaderboardBoard("townhall", key, townhallBoards[key], clans, now, limit, nullAssetURL))
	}
	return cache
}

func leaderboardBoard(kind, key string, players []leaderboardPlayerRow, clans map[string]leaderboardClanMetadata, now time.Time, limit int, nullAssetURL string) leaderboardBoardPayload {
	sort.Slice(players, func(i, j int) bool {
		if kind == "townhall" && players[i].LeagueID != players[j].LeagueID {
			return players[i].LeagueID > players[j].LeagueID
		}
		if players[i].Trophies == players[j].Trophies {
			return players[i].Tag < players[j].Tag
		}
		return players[i].Trophies > players[j].Trophies
	})
	if len(players) > limit {
		players = players[:limit]
	}
	payload := leaderboardBoardPayload{
		Kind:        kind,
		Key:         key,
		Items:       make([]leaderboardPlayerPayload, 0, len(players)),
		GeneratedAt: now,
	}
	for i, player := range players {
		payload.Items = append(payload.Items, leaderboardPlayerPayload{
			Rank:     i + 1,
			Tag:      player.Tag,
			Name:     player.Name,
			Clan:     leaderboardClanPayloadForPlayer(player.ClanTag, clans, nullAssetURL),
			League:   player.League,
			TownHall: player.TownHall,
			Trophies: player.Trophies,
		})
	}
	return payload
}

func leaderboardClanPayloadForPlayer(clanTag string, clans map[string]leaderboardClanMetadata, nullAssetURL string) *leaderboardClanPayload {
	if clanTag == "" {
		return nil
	}
	payload := &leaderboardClanPayload{Tag: clanTag, Badge: nullAssetURL}
	clan, ok := clans[clanTag]
	if !ok {
		return payload
	}
	payload.Name = &clan.Name
	payload.Badge = firstNonEmptyString(clan.BadgeURL, nullAssetURL)
	return payload
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func sortedNumericKeys(values map[string]struct{}) []string {
	out := sortedKeys(values)
	sort.Slice(out, func(i, j int) bool {
		left, _ := strconv.Atoi(out[i])
		right, _ := strconv.Atoi(out[j])
		return left < right
	})
	return out
}

func (s *timescaleLeaderboardStore) LoadCandidates(ctx context.Context, limit int) ([]leaderboardCandidate, error) {
	rows, err := s.pool.Query(ctx, leaderboardCandidateSQL, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []leaderboardCandidate
	for rows.Next() {
		var row leaderboardCandidate
		if err := rows.Scan(&row.BoardType, &row.BoardKey, &row.Tag); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *timescaleLeaderboardStore) UpdatePlayers(ctx context.Context, players []models.BasicPlayerRow) error {
	if len(players) == 0 {
		return nil
	}
	var err error
	for attempt := 0; attempt <= leaderboardUpdateRetries; attempt++ {
		err = s.updatePlayersOnce(ctx, players)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "40P01" {
			return err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 250 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

func (s *timescaleLeaderboardStore) updatePlayersOnce(ctx context.Context, players []models.BasicPlayerRow) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := utils.UpsertBasicPlayers(ctx, tx, players, leaderboardsDomainName); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *timescaleLeaderboardStore) DeletePlayers(ctx context.Context, tags []string) (int, error) {
	if len(tags) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM basic_player WHERE tag = ANY($1::text[])`, tags)
	return int(tag.RowsAffected()), err
}

func (s *timescaleLeaderboardStore) LoadClanMetadata(ctx context.Context, clanTags []string) (map[string]leaderboardClanMetadata, error) {
	if len(clanTags) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT requested.tag, c.name, c.badge_token
		FROM unnest($1::text[]) AS requested(tag)
		LEFT JOIN basic_clan c ON c.tag = requested.tag
	`, clanTags)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]leaderboardClanMetadata, len(clanTags))
	for rows.Next() {
		var tag string
		var name, badge sql.NullString
		if err := rows.Scan(&tag, &name, &badge); err != nil {
			return nil, err
		}
		if name.Valid {
			out[tag] = leaderboardClanMetadata{
				Name:     name.String,
				BadgeURL: badge.String,
			}
		}
	}
	return out, rows.Err()
}

func (s *timescaleLeaderboardStore) CacheBoards(ctx context.Context, cache leaderboardCacheSet) error {
	rawIndex, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(cache.Boards)+1)
	values := make([]string, 0, len(cache.Boards)+1)
	for _, board := range cache.Boards {
		raw, err := json.Marshal(board)
		if err != nil {
			return err
		}
		keys = append(keys, leaderboardCacheKey(board.Kind, board.Key))
		values = append(values, string(raw))
	}
	keys = append(keys, leaderboardIndexKey)
	values = append(values, string(rawIndex))
	return leaderboardCacheScript.Exec(ctx, s.valkey, keys, values).Error()
}

func (s *timescaleLeaderboardStore) RefreshMaterializedViews(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `REFRESH MATERIALIZED VIEW CONCURRENTLY clan_leaderboards`); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55000" {
			return err
		}
		// A new database starts this view WITH NO DATA, which PostgreSQL requires
		// us to populate once before concurrent refreshes are allowed.
		if _, err := s.pool.Exec(ctx, `REFRESH MATERIALIZED VIEW clan_leaderboards`); err != nil {
			return err
		}
	}
	if _, err := s.pool.Exec(ctx, `REFRESH MATERIALIZED VIEW war_league_counts`); err != nil {
		return err
	}
	return nil
}

func leaderboardCacheKey(kind, key string) string {
	if kind == "league" {
		return leaderboardLeagueKeyPrefix + key
	}
	return leaderboardTHKeyPrefix + key
}

func (s *timescaleLeaderboardStore) Close() error {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
	return nil
}
