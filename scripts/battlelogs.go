package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	"github.com/cespare/xxhash/v2"
	"github.com/clashkinginc/clashy.go"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel/attribute"
)

const battlelogsDomainName = "battlelogs"

const (
	battlelogTargetCursorKey = "battlelogs:cursor:active_players"
)

const activeBattlelogTargetSQL = `
	SELECT tag, name, COALESCE(league_id, 0), townhall_level
	FROM basic_player
	WHERE last_activity >= now() - interval '10 days'
	  AND tag > $1
	ORDER BY tag
	LIMIT $2
`

// Battlelog army columns use a compact prefix+ID shape. The prefix keeps the
// source section of the army link visible:
// d = siege machines, i = spells, s = super troops, u = troops,
// h = heroes, p = pets, e = hero equipment.
var battlelogColumnPattern = regexp.MustCompile(`^[disuhpe]_[0-9]+$`)

type battlelogsDomain struct {
	sink       battlelogStore
	checkpoint battlelogTimestampCache
}

type battlelogTargetBatch struct {
	Players []models.BasicPlayerRow
	Cursor  string
}

type battlelogStore interface {
	NextTargetBatch(context.Context, int) (battlelogTargetBatch, error)
	CommitTargetBatch(context.Context, battlelogTargetBatch) error
	LoadBasicPlayers(context.Context, []string) (map[string]models.BasicPlayerRow, error)
	Store(context.Context, models.BattlelogIngest) error
	Close() error
}

type timescaleBattlelogStore struct {
	pool               *pgxpool.Pool
	valkey             valkey.Client
	rollupFlushAttacks int

	// Rollups are buffered across inserts and flushed in larger batches. The raw
	// battlelog rows are committed first; aggregate writes are additive and can
	// happen afterward without changing the raw event history.
	rollupMu sync.Mutex
	rollups  itemRollupBuffer
}

type itemRollupBuffer struct {
	Usage          map[models.ItemRollupKey]int64
	Hitrate        map[models.ItemRollupKey]models.ItemHitrateCounts
	PendingAttacks int
}

func NewBattlelogsDomain() platform.Domain {
	return &battlelogsDomain{}
}

func (d *battlelogsDomain) Name() string { return battlelogsDomainName }

func (d *battlelogsDomain) Run(ctx context.Context, app *platform.App) error {
	if app.Config.BattlelogRequestsPerSecond <= 0 {
		return errors.New("battlelogs.requests_per_second must be greater than zero when battlelogs is enabled")
	}
	if app.Config.TargetPageMultiplier <= 0 {
		return errors.New("target_page_multiplier must be greater than zero when battlelogs is enabled")
	}
	if app.Config.BattlelogCheckpointTTLDays <= 0 {
		return errors.New("battlelogs.checkpoint_ttl_days must be greater than zero when battlelogs is enabled")
	}
	if app.Config.BattlelogFirstSeenLookbackDays <= 0 {
		return errors.New("battlelogs.first_seen_lookback_days must be greater than zero when battlelogs is enabled")
	}
	if !app.Config.DryRun && !app.Config.MockDB && app.Config.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required when battlelogs is enabled")
	}
	if !app.Config.DryRun && !app.Config.MockDB && app.Config.ValkeyAddr == "" {
		return errors.New("valkey_addr is required for battlelogs checkpoint persistence")
	}
	d.checkpoint = battlelogTimestampCache{
		client: app.Valkey,
		ttl:    time.Duration(app.Config.BattlelogCheckpointTTLDays) * 24 * time.Hour,
	}

	if app.Config.TimescaleURL != "" && !app.Config.DryRun && !app.Config.MockDB {
		store, err := newTimescaleBattlelogStore(ctx, app.Config.TimescaleURL, app.Config.BattlelogRollupFlushAttacks, app.Valkey)
		if err != nil {
			return err
		}
		d.sink = store
		defer store.Close()
	}

	limiter := platform.NewRequestLimiter(app.Config.BattlelogRequestsPerSecond)
	targetPageSize := app.Config.BattlelogRequestsPerSecond * app.Config.TargetPageMultiplier
	for {
		start := time.Now()
		var batch battlelogTargetBatch
		var err error
		if d.sink != nil {
			batch, err = d.sink.NextTargetBatch(ctx, targetPageSize)
		}
		if err == nil && len(batch.Players) > 0 {
			err = d.processTargets(ctx, app, limiter, batch.Players, app.Config.BattlelogRequestsPerSecond)
		}
		if err == nil && d.sink != nil {
			// Advance the scan cursor only after the fetched players have been processed.
			err = d.sink.CommitTargetBatch(ctx, batch)
		}
		if err != nil {
			app.Stats.SetReady(battlelogsDomainName, false, err.Error())
			return err
		}
		app.Stats.SetReady(battlelogsDomainName, true, "")
		app.Stats.RecordProcess(battlelogsDomainName, time.Since(start))
		if app.Config.RunOnce {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (d *battlelogsDomain) processTargets(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter, targets []models.BasicPlayerRow, maxInFlight int) error {
	// The target scan can read several seconds of players, while maxInFlight keeps the
	// active Clash calls bounded to the configured requests-per-second value.
	slots := make(chan struct{}, maxInt(maxInFlight, 1))
	errCh := make(chan error, len(targets))
	var wg sync.WaitGroup
	for _, target := range targets {
		target := target
		if target.Tag == "" || target.Name == "" || target.TownHall <= 0 {
			continue
		}
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			ingest, err := d.do(ctx, app, limiter, target)
			if err == nil {
				err = d.store(ctx, app, ingest)
			}
			if err != nil {
				app.Logger.Error("battlelog processing failed", "tag", target.Tag, "err", err)
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *battlelogsDomain) do(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter, player models.BasicPlayerRow) (models.BattlelogIngest, error) {
	ctx, span := platform.StartSpan(ctx, "battlelogs.do",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "do"),
	)
	defer span.End()

	entries, err := d.fetchBattleLog(ctx, app, limiter, player.Tag)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(attribute.Int("rows.count", len(entries)), platform.SpanErrorStatus(err))
		return models.BattlelogIngest{}, err
	}

	now := time.Now().UTC()
	players := map[string]models.BasicPlayerRow{player.Tag: player}
	checkpoint, err := d.loadBattlelogCheckpoint(ctx, player.Tag)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return models.BattlelogIngest{}, err
	}
	if len(entries) == 0 {
		ingest := models.BattlelogIngest{
			Players: basicPlayersFromMap(players),
		}
		span.SetAttributes(attribute.Int("rows.count", 0), platform.SpanErrorStatus(nil))
		return ingest, nil
	}

	after := checkpoint.Timestamp
	if after.IsZero() {
		// First-seen players only backfill a bounded window so old accounts do not fan out
		// into unbounded historical ingestion on their first poll.
		after = now.Add(-time.Duration(app.Config.BattlelogFirstSeenLookbackDays) * 24 * time.Hour)
	}
	newEntries := entriesAfterTimestamp(entries, after)
	if len(newEntries) == 0 {
		ingest := models.BattlelogIngest{
			Players: basicPlayersFromMap(players),
		}
		span.SetAttributes(attribute.Int("rows.count", 0), platform.SpanErrorStatus(nil))
		return ingest, nil
	}
	sort.Slice(newEntries, func(i, j int) bool {
		if newEntries[i].Timestamp.Equal(newEntries[j].Timestamp) {
			return newEntries[i].OpponentPlayerTag < newEntries[j].OpponentPlayerTag
		}
		return newEntries[i].Timestamp.Before(newEntries[j].Timestamp)
	})

	opponentTags := opponentTagsFromEntries(newEntries)
	opponents, err := d.loadBasicPlayers(ctx, opponentTags)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return models.BattlelogIngest{}, err
	}
	if opponents == nil {
		opponents = make(map[string]models.BasicPlayerRow)
	}
	rows := make([]models.BattlelogRow, 0, len(newEntries))
	var checkpointTime time.Time
	for _, entry := range newEntries {
		opponent, ok := opponents[entry.OpponentPlayerTag]
		if entry.OpponentPlayerTag != "" && (!ok || opponent.TownHall == 0) {
			// Battlelog rows need defender TH for analytics; fetch missing opponents lazily.
			player, fetchErr := d.fetchPlayer(ctx, app, limiter, entry.OpponentPlayerTag, "opponent_metadata")
			if fetchErr == nil && player != nil {
				opponent = basicPlayerFromPlayer(player)
				opponents[opponent.Tag] = opponent
				players[opponent.Tag] = opponent
				ok = true
			}
		}
		if !ok || opponent.TownHall <= 0 {
			// Do not checkpoint past a gap; later rows will be retried once this
			// opponent can be resolved.
			break
		}
		rows = append(rows, battlelogRowFromEntry(player.Tag, players[player.Tag], entry, opponent))
		if entry.Timestamp.After(checkpointTime) {
			checkpointTime = entry.Timestamp.UTC()
		}
	}

	ingest := models.BattlelogIngest{
		Rows:    rows,
		Players: basicPlayersFromMap(players),
	}
	if !checkpointTime.IsZero() {
		ingest.Checkpoints = []models.BattlelogCheckpoint{{Tag: player.Tag, Timestamp: checkpointTime}}
	}
	span.SetAttributes(attribute.Int("rows.count", len(rows)), platform.SpanErrorStatus(err))
	return ingest, nil
}

func (d *battlelogsDomain) store(ctx context.Context, app *platform.App, ingest models.BattlelogIngest) error {
	ctx, span := platform.StartSpan(ctx, "battlelogs.store",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "store"),
		attribute.Int("rows.count", len(ingest.Rows)),
		attribute.Int("write.count", len(ingest.Rows)+len(ingest.Players)+len(ingest.Checkpoints)),
	)
	defer span.End()
	if d.sink != nil {
		if err := d.sink.Store(ctx, ingest); err != nil {
			platform.RecordSpanError(span, err)
			span.SetAttributes(platform.SpanErrorStatus(err))
			return err
		}
	}
	if !app.Config.DryRun {
		// Checkpoints move only after durable rows write successfully.
		if err := d.checkpoint.UpdateMany(ctx, ingest.Checkpoints); err != nil {
			platform.RecordSpanError(span, err)
			span.SetAttributes(platform.SpanErrorStatus(err))
			return err
		}
	}
	app.Stats.RecordWrite(battlelogsDomainName, len(ingest.Rows)+len(ingest.Players)+len(ingest.Checkpoints))
	app.Stats.SetReady(battlelogsDomainName, true, "")
	span.SetAttributes(platform.SpanErrorStatus(nil))
	return nil
}

func (d *battlelogsDomain) fetchBattleLog(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter, tag string) ([]clashy.BattleLogEntry, error) {
	start := time.Now()
	fetchCtx, fetchSpan := platform.StartSpan(ctx, "clash.fetch",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "battlelog"),
	)
	release, err := limiter.Acquire(fetchCtx)
	if err != nil {
		platform.RecordSpanError(fetchSpan, err)
		fetchSpan.SetAttributes(platform.SpanErrorStatus(err))
		fetchSpan.End()
		return nil, err
	}
	entries, err := app.Clash.GetBattleLog(fetchCtx, tag)
	release()
	platform.RecordSpanError(fetchSpan, err)
	fetchSpan.SetAttributes(platform.SpanErrorStatus(err))
	fetchSpan.End()
	app.Stats.RecordRequest(battlelogsDomainName, time.Since(start), err)
	return entries, err
}

func (d *battlelogsDomain) fetchPlayer(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter, tag string, operation string) (*clashy.Player, error) {
	start := time.Now()
	fetchCtx, fetchSpan := platform.StartSpan(ctx, "clash.fetch",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", operation),
	)
	release, err := limiter.Acquire(fetchCtx)
	if err != nil {
		platform.RecordSpanError(fetchSpan, err)
		fetchSpan.SetAttributes(platform.SpanErrorStatus(err))
		fetchSpan.End()
		return nil, err
	}
	player, err := app.Clash.GetPlayer(fetchCtx, tag)
	release()
	platform.RecordSpanError(fetchSpan, err)
	fetchSpan.SetAttributes(platform.SpanErrorStatus(err))
	fetchSpan.End()
	app.Stats.RecordRequest(battlelogsDomainName, time.Since(start), err)
	return player, err
}

func (d *battlelogsDomain) loadBasicPlayers(ctx context.Context, tags []string) (map[string]models.BasicPlayerRow, error) {
	if d.sink == nil {
		return map[string]models.BasicPlayerRow{}, nil
	}
	return d.sink.LoadBasicPlayers(ctx, tags)
}

func (d *battlelogsDomain) loadBattlelogCheckpoint(ctx context.Context, tag string) (models.BattlelogCheckpoint, error) {
	return d.checkpoint.Get(ctx, tag)
}

func basicPlayersFromMap(values map[string]models.BasicPlayerRow) []models.BasicPlayerRow {
	players := make([]models.BasicPlayerRow, 0, len(values))
	for tag, player := range values {
		if tag == "" || player.Tag == "" || player.Name == "" || player.TownHall <= 0 {
			continue
		}
		players = append(players, player)
	}
	sort.Slice(players, func(i, j int) bool { return players[i].Tag < players[j].Tag })
	return players
}

func basicPlayerFromPlayer(player *clashy.Player) models.BasicPlayerRow {
	if player == nil {
		return models.BasicPlayerRow{}
	}
	return models.BasicPlayerRow{
		Tag:      player.Tag,
		Name:     player.Name,
		LeagueID: player.LeagueTier.ID,
		TownHall: player.TownHall,
	}
}

func opponentTagsFromEntries(entries []clashy.BattleLogEntry) []string {
	seen := make(map[string]struct{}, len(entries))
	tags := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.OpponentPlayerTag == "" {
			continue
		}
		if _, ok := seen[entry.OpponentPlayerTag]; ok {
			continue
		}
		seen[entry.OpponentPlayerTag] = struct{}{}
		tags = append(tags, entry.OpponentPlayerTag)
	}
	sort.Strings(tags)
	return tags
}

func newTimescaleBattlelogStore(ctx context.Context, dsn string, rollupFlushAttacks int, client valkey.Client) (*timescaleBattlelogStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleBattlelogStore{
		pool:               pool,
		valkey:             client,
		rollupFlushAttacks: maxInt(rollupFlushAttacks, 1),
		rollups:            newItemRollupBuffer(),
	}, nil
}

func (s *timescaleBattlelogStore) Close() error {
	if s == nil || s.pool == nil {
		return nil
	}
	// Flush aggregate deltas on shutdown so low-volume runs do not leave rollups
	// stranded below the normal threshold.
	if err := s.flushItemRollups(context.Background()); err != nil {
		s.pool.Close()
		return err
	}
	s.pool.Close()
	return nil
}

func (s *timescaleBattlelogStore) NextTargetBatch(ctx context.Context, batchSize int) (battlelogTargetBatch, error) {
	if batchSize <= 0 {
		return battlelogTargetBatch{}, nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.basic_player.targets",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "scan_basic_player_targets"),
		attribute.Int("batch.size", batchSize),
	)
	defer span.End()

	cursor, err := s.getTargetCursor(ctx)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return battlelogTargetBatch{}, err
	}
	players, nextCursor, err := s.scanActivePlayers(ctx, cursor, batchSize)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return battlelogTargetBatch{}, err
	}
	batch := battlelogTargetBatch{Players: players, Cursor: nextCursor}
	span.SetAttributes(attribute.Int("target.count", len(batch.Players)), platform.SpanErrorStatus(nil))
	return batch, nil
}

func (s *timescaleBattlelogStore) scanActivePlayers(ctx context.Context, cursor string, limit int) ([]models.BasicPlayerRow, string, error) {
	rows, err := s.pool.Query(ctx, activeBattlelogTargetSQL, cursor, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	players := make([]models.BasicPlayerRow, 0, limit+1)
	for rows.Next() {
		var player models.BasicPlayerRow
		if err := rows.Scan(&player.Tag, &player.Name, &player.LeagueID, &player.TownHall); err != nil {
			return nil, "", err
		}
		if player.Tag == "" || player.Name == "" || player.TownHall <= 0 {
			continue
		}
		players = append(players, player)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(players) == 0 {
		return nil, "", nil
	}
	if len(players) > limit {
		// Read one extra row to distinguish "more rows exist" from "wrapped the scan".
		nextCursor := players[limit-1].Tag
		return players[:limit], nextCursor, nil
	}
	return players, "", nil
}

func (s *timescaleBattlelogStore) getTargetCursor(ctx context.Context) (string, error) {
	if s.valkey == nil {
		return "", nil
	}
	ctx, span := platform.StartSpan(ctx, "valkey.battlelog_target_cursor.get",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "get"),
	)
	defer span.End()
	value, err := s.valkey.Do(ctx, s.valkey.B().Get().Key(battlelogTargetCursorKey).Build()).ToString()
	if valkey.IsValkeyNil(err) {
		span.SetAttributes(attribute.Int("rows.count", 0), platform.SpanErrorStatus(nil))
		return "", nil
	}
	platform.RecordSpanError(span, err)
	span.SetAttributes(attribute.Int("rows.count", 1), platform.SpanErrorStatus(err))
	return value, err
}

func (s *timescaleBattlelogStore) CommitTargetBatch(ctx context.Context, batch battlelogTargetBatch) error {
	if s.valkey == nil {
		return nil
	}
	// Empty cursor deletes the checkpoint, causing the next scan to wrap to the beginning.
	command := cursorCommand(s.valkey, battlelogTargetCursorKey, batch.Cursor)
	ctx, span := platform.StartSpan(ctx, "valkey.battlelog_target_cursor.set",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "set"),
		attribute.Int("write.count", 1),
	)
	defer span.End()
	err := s.valkey.Do(ctx, command).Error()
	platform.RecordSpanError(span, err)
	span.SetAttributes(platform.SpanErrorStatus(err))
	return err
}

func cursorCommand(client valkey.Client, key, cursor string) valkey.Completed {
	if cursor == "" {
		return client.B().Del().Key(key).Build()
	}
	return client.B().Set().Key(key).Value(cursor).Build()
}

func (s *timescaleBattlelogStore) Store(ctx context.Context, ingest models.BattlelogIngest) error {
	if len(ingest.Rows) == 0 && len(ingest.Players) == 0 && len(ingest.Checkpoints) == 0 {
		return nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.battlelogs.store",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "store_battlelogs"),
		attribute.Int("rows.count", len(ingest.Rows)),
		attribute.Int("write.count", len(ingest.Rows)+len(ingest.Players)+len(ingest.Checkpoints)),
	)
	defer span.End()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	defer tx.Rollback(ctx)

	// Persist profile rows before battlelog rows because the insert joins basic_player.
	if err := utils.UpsertBasicPlayers(ctx, tx, ingest.Players, battlelogsDomainName); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	insertedRows, err := s.insertBattlelogRows(ctx, tx, ingest.Rows)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	err = s.bufferItemRollups(ctx, insertedRows)
	platform.RecordSpanError(span, err)
	span.SetAttributes(platform.SpanErrorStatus(err))
	return err
}

func (s *timescaleBattlelogStore) insertBattlelogRows(ctx context.Context, tx pgx.Tx, rows []models.BattlelogRow) ([]models.BattlelogRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.battlelogs.insert",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "insert_battlelogs"),
		attribute.Int("rows.count", len(rows)),
	)
	defer span.End()
	batch := &pgx.Batch{}
	for _, row := range rows {
		armyItems, armyCounts, err := armyItemsAndCounts(row.ArmyColumns)
		if err != nil {
			platform.RecordSpanError(span, err)
			span.SetAttributes(platform.SpanErrorStatus(err))
			return nil, err
		}
		batch.Queue(`
			INSERT INTO battlelogs (
				battle_id, army_hash, player_tag, player_th,
				opponent_tag, opponent_th, league_id, battle_type, attack,
				stars, destruction_percentage, gold, elixir, dark_elixir, battle_time,
				army_items, army_counts
			)
			SELECT
				$1, $2::numeric, p.tag, p.townhall_level,
				o.tag, o.townhall_level, COALESCE(p.league_id, 0), $5, $6,
				$7, $8, $9, $10, $11, $12,
				$13, $14::jsonb
			FROM basic_player p
			JOIN basic_player o ON o.tag = $4
			WHERE p.tag = $3
			ON CONFLICT (battle_id, battle_time) DO NOTHING
		`,
			row.BattleID, strconv.FormatUint(row.ArmyHash, 10), row.PlayerTag, row.OpponentTag,
			row.BattleType, row.Attack,
			int16(row.Stars), int16(row.DestructionPercentage), int32(row.Gold),
			int32(row.Elixir), int32(row.DarkElixir), row.Timestamp,
			armyItems, armyCounts,
		)
	}
	results := tx.SendBatch(ctx, batch)
	inserted := make([]models.BattlelogRow, 0, len(rows))
	var err error
	for i := 0; i < batch.Len(); i++ {
		var tag pgconn.CommandTag
		tag, err = results.Exec()
		if err != nil {
			break
		}
		if tag.RowsAffected() > 0 {
			inserted = append(inserted, rows[i])
		}
	}
	closeErr := results.Close()
	if err == nil {
		err = closeErr
	}
	platform.RecordSpanError(span, err)
	span.SetAttributes(attribute.Int("rows.inserted", len(inserted)), platform.SpanErrorStatus(err))
	return inserted, err
}

func (s *timescaleBattlelogStore) LoadBasicPlayers(ctx context.Context, tags []string) (map[string]models.BasicPlayerRow, error) {
	out := make(map[string]models.BasicPlayerRow)
	if len(tags) == 0 {
		return out, nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.basic_player.load",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "load_basic_players"),
		attribute.Int("target.count", len(tags)),
	)
	defer span.End()
	rows, err := s.pool.Query(ctx, `
		SELECT tag, name, COALESCE(league_id, 0), townhall_level
		FROM basic_player
		WHERE tag = ANY($1)
	`, tags)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row models.BasicPlayerRow
		if err := rows.Scan(&row.Tag, &row.Name, &row.LeagueID, &row.TownHall); err != nil {
			platform.RecordSpanError(span, err)
			span.SetAttributes(platform.SpanErrorStatus(err))
			return nil, err
		}
		out[row.Tag] = row
	}
	err = rows.Err()
	platform.RecordSpanError(span, err)
	span.SetAttributes(attribute.Int("rows.count", len(out)), platform.SpanErrorStatus(err))
	return out, err
}

func (s *timescaleBattlelogStore) bufferItemRollups(ctx context.Context, rows []models.BattlelogRow) error {
	s.rollupMu.Lock()
	defer s.rollupMu.Unlock()

	accumulateItemRollups(rows, &s.rollups)
	if s.rollups.PendingAttacks < s.rollupFlushAttacks {
		return nil
	}
	// Only one goroutine may flush because the map is reset after a successful
	// commit.
	return s.flushItemRollupsLocked(ctx)
}

func (s *timescaleBattlelogStore) flushItemRollups(ctx context.Context) error {
	s.rollupMu.Lock()
	defer s.rollupMu.Unlock()
	return s.flushItemRollupsLocked(ctx)
}

func (s *timescaleBattlelogStore) flushItemRollupsLocked(ctx context.Context) error {
	if len(s.rollups.Usage) == 0 && len(s.rollups.Hitrate) == 0 {
		s.rollups.PendingAttacks = 0
		return nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.rollups.flush",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "flush_rollups"),
		attribute.Int("rows.count", len(s.rollups.Usage)+len(s.rollups.Hitrate)),
	)
	defer span.End()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	defer tx.Rollback(ctx)

	// Usage and hitrate rollups share a transaction so the daily aggregates move
	// forward together for a given flush.
	if err := s.insertItemUsageRollups(ctx, tx, s.rollups.Usage); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	if err := s.insertItemHitrateRollups(ctx, tx, s.rollups.Hitrate); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	// Reset only after commit; on failure the buffered deltas remain in memory and
	// can be retried by a later flush.
	s.rollups = newItemRollupBuffer()
	span.SetAttributes(platform.SpanErrorStatus(nil))
	return nil
}

func (s *timescaleBattlelogStore) insertItemUsageRollups(ctx context.Context, tx pgx.Tx, usage map[models.ItemRollupKey]int64) error {
	if len(usage) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for key, uses := range usage {
		batch.Queue(`
			INSERT INTO item_usage_daily (
				day_start, player_th, league_id, battle_type, item_key, uses
			)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (day_start, player_th, league_id, battle_type, item_key)
			DO UPDATE SET uses = item_usage_daily.uses + EXCLUDED.uses
		`, key.DayStart, key.PlayerTH, key.LeagueID, key.BattleType, key.ItemKey, uses)
	}
	return utils.SendBatch(ctx, tx, batch)
}

func (s *timescaleBattlelogStore) insertItemHitrateRollups(ctx context.Context, tx pgx.Tx, hitrate map[models.ItemRollupKey]models.ItemHitrateCounts) error {
	if len(hitrate) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for key, counts := range hitrate {
		batch.Queue(`
			INSERT INTO item_hitrate_daily (
				day_start, player_th, league_id, battle_type, item_key,
				attacks, zero_stars, one_stars, two_stars, three_stars
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (day_start, player_th, league_id, battle_type, item_key)
			DO UPDATE SET
				attacks = item_hitrate_daily.attacks + EXCLUDED.attacks,
				zero_stars = item_hitrate_daily.zero_stars + EXCLUDED.zero_stars,
				one_stars = item_hitrate_daily.one_stars + EXCLUDED.one_stars,
				two_stars = item_hitrate_daily.two_stars + EXCLUDED.two_stars,
				three_stars = item_hitrate_daily.three_stars + EXCLUDED.three_stars
		`,
			key.DayStart, key.PlayerTH, key.LeagueID, key.BattleType, key.ItemKey,
			counts.Attacks, counts.ZeroStars, counts.OneStars, counts.TwoStars, counts.ThreeStars,
		)
	}
	return utils.SendBatch(ctx, tx, batch)
}

func armyItemsAndCounts(columns map[string]uint16) ([]string, string, error) {
	// Derive both stored army representations from the same sorted key list so
	// row inserts are deterministic even though maps iterate randomly.
	items := sortedArmyColumnKeys(columns)
	counts := make(map[string]uint16, len(items))
	for _, item := range items {
		counts[item] = columns[item]
	}
	raw, err := json.Marshal(counts)
	if err != nil {
		return nil, "", err
	}
	return items, string(raw), nil
}

func newItemRollupBuffer() itemRollupBuffer {
	return itemRollupBuffer{
		Usage:   make(map[models.ItemRollupKey]int64),
		Hitrate: make(map[models.ItemRollupKey]models.ItemHitrateCounts),
	}
}

func accumulateItemRollups(rows []models.BattlelogRow, buffer *itemRollupBuffer) {
	// Usage stats count every ranked/legend attack with parsed army items,
	// regardless of defender town hall.
	for _, row := range attackRows(rows) {
		if !rowHasArmyItems(row) {
			continue
		}
		buffer.PendingAttacks++
		for _, item := range sortedArmyColumnKeys(row.ArmyColumns) {
			usageKey := models.ItemRollupKey{
				DayStart:   dayStart(row.Timestamp),
				PlayerTH:   int16(row.PlayerTH),
				LeagueID:   int32(row.LeagueID),
				BattleType: row.BattleType,
				ItemKey:    item,
			}
			buffer.Usage[usageKey]++
		}
	}

	// Hitrate stats are narrower: only same-TH ranked/legend attacks are used so
	// star buckets compare armies in roughly equivalent matchups.
	for _, row := range qualifyingDynamicSearchRows(rows) {
		if !rowHasArmyItems(row) {
			continue
		}
		zeroStars, oneStars, twoStars, threeStars := starBuckets(row)
		for _, item := range sortedArmyColumnKeys(row.ArmyColumns) {
			hitrateKey := models.ItemRollupKey{
				DayStart:   dayStart(row.Timestamp),
				PlayerTH:   int16(row.PlayerTH),
				LeagueID:   int32(row.LeagueID),
				BattleType: row.BattleType,
				ItemKey:    item,
			}
			counts := buffer.Hitrate[hitrateKey]
			counts.Attacks++
			counts.ZeroStars += int64(zeroStars)
			counts.OneStars += int64(oneStars)
			counts.TwoStars += int64(twoStars)
			counts.ThreeStars += int64(threeStars)
			buffer.Hitrate[hitrateKey] = counts
		}
	}
}

func qualifyingDynamicSearchRows(rows []models.BattlelogRow) []models.BattlelogRow {
	// "Dynamic search" stats are meant to answer how an army performs at the
	// player's own TH, so mismatched TH hits are excluded from hitrate rollups.
	out := make([]models.BattlelogRow, 0, len(rows))
	for _, row := range rows {
		if row.Attack && row.PlayerTH == row.OpponentTH && isBattlelogStatsType(row.BattleType) {
			out = append(out, row)
		}
	}
	return out
}

func attackRows(rows []models.BattlelogRow) []models.BattlelogRow {
	// Rollups ignore defenses and non-stats battle types; the army columns belong
	// to the attacking player.
	out := make([]models.BattlelogRow, 0, len(rows))
	for _, row := range rows {
		if row.Attack && isBattlelogStatsType(row.BattleType) {
			out = append(out, row)
		}
	}
	return out
}

func isBattlelogStatsType(value string) bool {
	// Friendly/challenge-style entries are deliberately excluded from usage and
	// hitrate aggregates because they do not reflect normal matchmaking.
	switch strings.ToLower(value) {
	case "ranked", "legend":
		return true
	default:
		return false
	}
}

func starBuckets(row models.BattlelogRow) (uint64, uint64, uint64, uint64) {
	switch row.Stars {
	case 0:
		return 1, 0, 0, 0
	case 1:
		return 0, 1, 0, 0
	case 2:
		return 0, 0, 1, 0
	case 3:
		return 0, 0, 0, 1
	default:
		return 0, 0, 0, 0
	}
}

func dayStart(value time.Time) time.Time {
	// Normalize all aggregate buckets to UTC so workers in different local
	// timezones write to the same daily row.
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func sortedArmyColumnKeys(columns map[string]uint16) []string {
	// Filter through the column regex before sorting so malformed parser output
	// or future share-code fields do not leak into persisted stats.
	keys := make([]string, 0, len(columns))
	for key, value := range columns {
		if value > 0 && battlelogColumnPattern.MatchString(key) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		leftSection, leftID := splitArmyColumn(keys[i])
		rightSection, rightID := splitArmyColumn(keys[j])
		if leftSection != rightSection {
			return leftSection < rightSection
		}
		return leftID < rightID
	})
	return keys
}

func rowHasArmyItems(row models.BattlelogRow) bool {
	for key, value := range row.ArmyColumns {
		if value > 0 && battlelogColumnPattern.MatchString(key) {
			return true
		}
	}
	return false
}

type battlelogTimestampCache struct {
	client valkey.Client
	ttl    time.Duration
}

func (c battlelogTimestampCache) Get(ctx context.Context, tag string) (models.BattlelogCheckpoint, error) {
	if c.client == nil || tag == "" {
		return models.BattlelogCheckpoint{Tag: tag}, nil
	}
	ctx, span := platform.StartSpan(ctx, "valkey.battlelog_checkpoint.get",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "get"),
	)
	defer span.End()
	value, err := c.client.Do(ctx, c.client.B().Get().Key(battlelogCheckpointKey(tag)).Build()).ToString()
	if valkey.IsValkeyNil(err) {
		span.SetAttributes(attribute.Int("rows.count", 0), platform.SpanErrorStatus(nil))
		return models.BattlelogCheckpoint{Tag: tag}, nil
	}
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return models.BattlelogCheckpoint{}, err
	}
	timestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return models.BattlelogCheckpoint{}, err
	}
	span.SetAttributes(attribute.Int("rows.count", 1), platform.SpanErrorStatus(nil))
	return models.BattlelogCheckpoint{Tag: tag, Timestamp: timestamp.UTC()}, nil
}

func (c battlelogTimestampCache) UpdateMany(ctx context.Context, checkpoints []models.BattlelogCheckpoint) error {
	if c.client == nil || len(checkpoints) == 0 {
		return nil
	}
	commands := make([]valkey.Completed, 0, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if checkpoint.Tag == "" || checkpoint.Timestamp.IsZero() {
			continue
		}
		commands = append(commands, c.client.B().Set().
			Key(battlelogCheckpointKey(checkpoint.Tag)).
			Value(checkpoint.Timestamp.UTC().Format(time.RFC3339Nano)).
			Ex(c.ttl).
			Build())
	}
	if len(commands) == 0 {
		return nil
	}
	ctx, span := platform.StartSpan(ctx, "valkey.battlelog_checkpoint.set",
		attribute.String("domain", battlelogsDomainName),
		attribute.String("operation", "set"),
		attribute.Int("write.count", len(commands)),
	)
	defer span.End()
	results := c.client.DoMulti(ctx, commands...)
	for _, result := range results {
		if err := result.Error(); err != nil {
			platform.RecordSpanError(span, err)
			span.SetAttributes(platform.SpanErrorStatus(err))
			return err
		}
	}
	span.SetAttributes(platform.SpanErrorStatus(nil))
	return nil
}

func battlelogCheckpointKey(tag string) string {
	return "bl:" + tag
}

func entriesAfterTimestamp(entries []clashy.BattleLogEntry, after time.Time) []clashy.BattleLogEntry {
	out := make([]clashy.BattleLogEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Timestamp.After(after) {
			out = append(out, entry)
		}
	}
	return out
}

func latestBattlelogTimestamp(entries []clashy.BattleLogEntry) time.Time {
	var latest time.Time
	for _, entry := range entries {
		if entry.Timestamp.After(latest) {
			latest = entry.Timestamp
		}
	}
	return latest.UTC()
}

func battlelogRowFromEntry(playerTag string, player models.BasicPlayerRow, entry clashy.BattleLogEntry, opponent models.BasicPlayerRow) models.BattlelogRow {
	gold, elixir, darkElixir := lootedResourceColumns(entry.LootedResources)
	armyColumns := parseArmyColumns(entry.ArmyShareCode)
	return models.BattlelogRow{
		BattleID:              battlelogBattleID(playerTag, entry),
		ArmyHash:              armyHash(armyColumns),
		PlayerTag:             playerTag,
		PlayerTH:              uint8(player.TownHall),
		OpponentTag:           entry.OpponentPlayerTag,
		OpponentTH:            uint8(opponent.TownHall),
		LeagueID:              uint32(player.LeagueID),
		BattleType:            entry.BattleType,
		Attack:                entry.Attack,
		Stars:                 uint8(entry.Stars),
		DestructionPercentage: uint8(entry.DestructionPercentage),
		Gold:                  uint32(gold),
		Elixir:                uint32(elixir),
		DarkElixir:            uint32(darkElixir),
		Timestamp:             entry.Timestamp.UTC(),
		ArmyColumns:           armyColumns,
	}
}

func battlelogBattleID(playerTag string, entry clashy.BattleLogEntry) uuid.UUID {
	key := strings.Join([]string{
		playerTag,
		entry.OpponentPlayerTag,
		entry.Timestamp.UTC().Format(time.RFC3339Nano),
		entry.BattleType,
		strconv.FormatBool(entry.Attack),
	}, "|")
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(key))
}

func lootedResourceColumns(resources []clashy.Resource) (gold, elixir, darkElixir int) {
	for _, resource := range resources {
		switch resource.Name {
		case "Gold":
			gold += resource.Amount
		case "Elixir":
			elixir += resource.Amount
		case "DarkElixir":
			darkElixir += resource.Amount
		}
	}
	return gold, elixir, darkElixir
}

func parseArmyColumns(link string) map[string]uint16 {
	// Army share links encode units/spells/heroes in compact sections. The parser
	// flattens them into uniform prefix_ID columns so troops, spells, heroes,
	// pets, equipment, and siege machines can share rollup code.
	payload := extractArmySharePayload(link)
	columns := make(map[string]uint16)
	for _, section := range splitArmyShareSections(payload) {
		if len(section) < 2 {
			continue
		}
		switch section[0] {
		case 'u', 's', 'i', 'd':
			parseArmyItemSection(section[0], section[1:], columns)
		case 'h':
			parseHeroSection(section[1:], columns)
		}
	}
	return columns
}

func extractArmySharePayload(link string) string {
	// Clash share links usually put the army code in the "army" query parameter,
	// but tests and callers may pass the raw payload directly.
	parsed, err := url.Parse(link)
	if err == nil {
		if army := parsed.Query().Get("army"); army != "" {
			return army
		}
	}
	return link
}

func splitArmyShareSections(payload string) []string {
	// Sections start with one of the known marker bytes. The payload format does
	// not use a global delimiter, so split by detecting the next marker.
	var sections []string
	start := -1
	for i := 0; i < len(payload); i++ {
		switch payload[i] {
		case 'h', 'i', 'd', 'u', 's':
			if start >= 0 {
				sections = append(sections, payload[start:i])
			}
			start = i
		}
	}
	if start >= 0 {
		sections = append(sections, payload[start:])
	}
	return sections
}

func parseArmyItemSection(marker byte, payload string, columns map[string]uint16) {
	// Normal sections look like "2x123-1x456": quantity first, then the Clash
	// item ID. Duplicate IDs are summed.
	for _, part := range strings.Split(payload, "-") {
		qtyText, idText, ok := strings.Cut(part, "x")
		if !ok {
			continue
		}
		qty, err1 := strconv.Atoi(qtyText)
		id, err2 := strconv.Atoi(idText)
		if err1 != nil || err2 != nil || qty <= 0 || id < 0 {
			continue
		}
		columns[fmt.Sprintf("%c_%d", marker, id)] += uint16(qty)
	}
}

func parseHeroSection(payload string, columns map[string]uint16) {
	// Hero sections include the hero ID followed by optional pets/equipment. Pets
	// use a "p" marker; equipment can appear as "e<ID>" or as a bare "_<ID>"
	// continuation after a pet/equipment token.
	for _, part := range strings.Split(payload, "-") {
		if part == "" {
			continue
		}
		heroID, rest := leadingInt(part)
		if heroID >= 0 {
			columns[fmt.Sprintf("h_%d", heroID)]++
		}
		for rest != "" {
			marker := rest[0]
			if marker != 'p' && marker != 'e' {
				if marker == '_' {
					rest = rest[1:]
					continue
				}
				_, rest = leadingInt(rest[1:])
				continue
			}
			value, next := leadingInt(rest[1:])
			if value >= 0 {
				columns[fmt.Sprintf("%c_%d", marker, value)]++
			}
			rest = next
			if strings.HasPrefix(rest, "_") {
				value, next = leadingInt(rest[1:])
				if value >= 0 {
					columns[fmt.Sprintf("e_%d", value)]++
				}
				rest = next
			}
		}
	}
}

func leadingInt(value string) (int, string) {
	// Return the parsed integer and the unconsumed suffix. A -1 sentinel means the
	// string did not start with a digit.
	if value == "" || value[0] < '0' || value[0] > '9' {
		return -1, value
	}
	i := 0
	for i < len(value) && value[i] >= '0' && value[i] <= '9' {
		i++
	}
	parsed, _ := strconv.Atoi(value[:i])
	return parsed, value[i:]
}

func armyHash(columns map[string]uint16) uint64 {
	// Hash the canonical army shape so equal armies get the same compact ID even
	// if the original share-link order differed.
	canonical := canonicalArmy(columns)
	if canonical == "" {
		return 0
	}
	return xxhash.Sum64String(canonical)
}

func canonicalArmy(columns map[string]uint16) string {
	// Build a deterministic representation of the army. This is what makes the
	// xxhash value stable despite Go's randomized map iteration order.
	keys := make([]string, 0, len(columns))
	for key, value := range columns {
		if value > 0 {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		leftSection, leftID := splitArmyColumn(keys[i])
		rightSection, rightID := splitArmyColumn(keys[j])
		if leftSection != rightSection {
			return leftSection < rightSection
		}
		return leftID < rightID
	})
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		section, id := splitArmyColumn(key)
		parts = append(parts, fmt.Sprintf("%s:%d=%d", section, id, columns[key]))
	}
	return strings.Join(parts, ";")
}

func splitArmyColumn(column string) (string, int) {
	section, idText, _ := strings.Cut(column, "_")
	id, _ := strconv.Atoi(idText)
	return section, id
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
