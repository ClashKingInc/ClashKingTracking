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
	"clashking_tracking/models"

	"github.com/clashkinginc/clashy.go"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
)

const battlelogsDomainName = "battlelogs"

const (
	// A first-seen player can contribute an entire battle-log page, so one
	// queued value is much larger than a normal profile update. Keep both the
	// SQL batch and the pending queue deliberately small; backpressure is safer
	// than retaining thousands of decoded battle logs while Postgres catches up.
	battlelogAsyncWriteBatchSize     = 100
	battlelogAsyncWriteQueueSize     = 200
	battlelogAsyncWriteFlushInterval = 500 * time.Millisecond
	// Target tags and checkpoint timestamps are small. A larger page amortizes
	// SQL/MGET work and, more importantly, avoids waiting at a page barrier for
	// a handful of retrying 504s every few seconds. Decoded response memory is
	// bounded independently by battlelogRequestConcurrency and the write queue.
	battlelogTargetPageSize = 20000
)

// Battlelog army columns use a compact prefix+ID shape. The prefix keeps the
// source section of the army link visible:
// d = siege machines, i = spells, s = super troops, u = troops,
// h = heroes, p = pets, e = hero equipment.
var battlelogColumnPattern = regexp.MustCompile(`^[disuhpe]_[0-9]+$`)

type battlelogsDomain struct {
	sink       battlelogStore
	checkpoint battlelogTimestampCache
}

type battlelogStore interface {
	LoadTargets(context.Context, string) ([]string, error)
	FilterStandardTargets(context.Context, []string) ([]string, error)
	Store(context.Context, models.BattlelogIngest) (int, error)
	Close() error
}

type battlelogTargetJob struct {
	Tag        string
	Checkpoint models.BattlelogCheckpoint
}

type timescaleBattlelogStore struct {
	pool *pgxpool.Pool
}

func NewBattlelogsDomain() platform.Domain {
	return &battlelogsDomain{}
}

func (d *battlelogsDomain) Name() string { return battlelogsDomainName }

func (d *battlelogsDomain) Run(ctx context.Context, app *platform.App) error {
	if app.Config.BattlelogRequestsPerSecond <= 0 {
		return errors.New("battlelogs.requests_per_second must be greater than zero when battlelogs is enabled")
	}
	if app.Config.BattlelogPriorityRequestsPerSecond < 0 || app.Config.BattlelogPriorityRequestsPerSecond > app.Config.BattlelogRequestsPerSecond {
		return errors.New("battlelogs.priority_requests_per_second must be between zero and battlelogs.requests_per_second")
	}
	if app.Config.BattlelogCheckpointTTLDays <= 0 {
		return errors.New("battlelogs.checkpoint_ttl_days must be greater than zero when battlelogs is enabled")
	}
	if app.Config.BattlelogFirstSeenLookbackDays <= 0 {
		return errors.New("battlelogs.first_seen_lookback_days must be greater than zero when battlelogs is enabled")
	}
	if !app.Config.DryRun && !app.Config.MockDB && app.Config.TimescaleURL == "" {
		return errors.New("TIMESCALE_* connection variables are required when battlelogs is enabled")
	}
	if !app.Config.DryRun && !app.Config.MockDB && app.Config.ValkeyAddr == "" {
		return errors.New("valkey_addr is required for battlelogs checkpoint persistence")
	}
	d.checkpoint = battlelogTimestampCache{
		client: app.Valkey,
		ttl:    time.Duration(app.Config.BattlelogCheckpointTTLDays) * 24 * time.Hour,
	}

	if app.Config.TimescaleURL != "" && !app.Config.DryRun && !app.Config.MockDB {
		store, err := newTimescaleBattlelogStore(ctx, app.Config.TimescaleURL)
		if err != nil {
			return err
		}
		d.sink = store
		defer store.Close()
	}

	if d.sink == nil {
		app.Stats.SetReady(battlelogsDomainName, true, "")
		return nil
	}

	writer := platform.NewAsyncBatchWriter[models.BattlelogIngest](
		app,
		platform.AsyncBatchWriterConfig[models.BattlelogIngest]{
			Domain:        battlelogsDomainName,
			BatchSize:     battlelogAsyncWriteBatchSize,
			QueueSize:     battlelogAsyncWriteQueueSize,
			FlushInterval: battlelogAsyncWriteFlushInterval,
			WriteBatch: func(writeCtx context.Context, values []models.BattlelogIngest) error {
				start := time.Now()
				ingest := mergeBattlelogIngests(values)
				if err := d.store(writeCtx, app, ingest); err != nil {
					return err
				}
				app.Stats.SetReady(battlelogsDomainName, true, "")
				app.Stats.RecordProcess(battlelogsDomainName, time.Since(start))
				return nil
			},
		},
	)
	writerCtx, stopWriter := context.WithCancel(ctx)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		writer.Run(writerCtx)
	}()
	defer func() {
		stopWriter()
		<-writerDone
	}()

	legendRPS := app.Config.BattlelogPriorityRequestsPerSecond
	standardRPS := app.Config.BattlelogRequestsPerSecond - legendRPS
	trackerCtx, stopTrackers := context.WithCancel(ctx)
	defer stopTrackers()
	errCh := make(chan error, 2)
	runners := 0
	if legendRPS > 0 {
		runners++
		go func() {
			errCh <- d.runTracker(trackerCtx, app, writer, "legend", legendRPS)
		}()
	}
	if standardRPS > 0 {
		runners++
		go func() {
			errCh <- d.runTracker(trackerCtx, app, writer, "standard", standardRPS)
		}()
	}
	if runners == 0 {
		return errors.New("battlelogs has no positive request budget after priority split")
	}
	var firstErr error
	for range runners {
		if err := <-errCh; err != nil && firstErr == nil {
			firstErr = err
			stopTrackers()
		}
	}
	return firstErr
}

func (d *battlelogsDomain) runTracker(
	ctx context.Context,
	app *platform.App,
	writer *platform.AsyncBatchWriter[models.BattlelogIngest],
	group string,
	requestsPerSecond int,
) error {
	statsName := trackingProgressName(battlelogsDomainName, group)
	limiter, err := newTrackingLimiter(requestsPerSecond)
	if err != nil {
		return err
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	jobs := make(chan battlelogTargetJob)
	errCh := make(chan error, 1)
	var workers sync.WaitGroup
	reportError := func(err error) {
		select {
		case errCh <- err:
		default:
		}
		stopWorkers()
	}
	for range battlelogRequestConcurrency(requestsPerSecond) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}
					ingest, err := retryLimitedClashFetch(workerCtx, app, limiter, func(fetchCtx context.Context) (models.BattlelogIngest, error) {
						return d.do(fetchCtx, app, statsName, job.Tag, job.Checkpoint)
					})
					if err != nil {
						if workerCtx.Err() != nil {
							return
						}
						// One unavailable player must not stop either target pool. The
						// tag will naturally be seen again on the next pass, while SQL,
						// Valkey, and writer failures still terminate the process.
						app.Logger.Error("battlelog processing failed", "tag", job.Tag, "err", err)
						app.Stats.SetReady(statsName, false, err.Error())
						continue
					}
					if len(ingest.Rows) > 0 || len(ingest.Checkpoints) > 0 {
						if err := writer.Enqueue(workerCtx, ingest); err != nil {
							reportError(err)
							return
						}
					}
					// A request may have recovered after a transient proxy timeout.
					app.Stats.SetReady(statsName, true, "")
					app.Stats.RecordTrackedTarget(statsName)
				}
			}
		}()
	}
	defer func() {
		stopWorkers()
		close(jobs)
		workers.Wait()
	}()
	processTags := func(tags []string) error {
		checkpoints, err := d.checkpoint.GetMany(workerCtx, tags)
		if err != nil {
			return err
		}
		for _, tag := range tags {
			select {
			case jobs <- battlelogTargetJob{Tag: tag, Checkpoint: checkpoints[tag]}:
			case err := <-errCh:
				return err
			case <-workerCtx.Done():
				select {
				case err := <-errCh:
					return err
				default:
					return workerCtx.Err()
				}
			}
		}
		return nil
	}
	for {
		tags, err := d.sink.LoadTargets(ctx, group)
		if err != nil {
			return err
		}
		if group == "standard" {
			verified, err := activeVerifiedPlayerTags(ctx, app.Valkey)
			if err != nil {
				return err
			}
			verified, err = d.sink.FilterStandardTargets(ctx, verified)
			if err != nil {
				return err
			}
			tags = mergeUniqueTags(tags, verified)
		}
		app.Stats.SetTrackingTargets(statsName, len(tags))
		for start := 0; start < len(tags); start += battlelogTargetPageSize {
			end := min(start+battlelogTargetPageSize, len(tags))
			if err := processTags(tags[start:end]); err != nil {
				return err
			}
		}
		if err := sleepOrDone(ctx, time.Second); err != nil {
			return err
		}
	}
}

func mergeUniqueTags(groups ...[]string) []string {
	seen := make(map[string]struct{})
	for _, group := range groups {
		for _, tag := range group {
			if tag != "" {
				seen[tag] = struct{}{}
			}
		}
	}
	tags := make([]string, 0, len(seen))
	for tag := range seen {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	return tags
}

func battlelogRequestConcurrency(requestsPerSecond int) int {
	if requestsPerSecond <= 0 {
		return 0
	}
	// Battle-log responses are much larger than clan/player summaries. A worker
	// blocked on SQL backpressure retains its decoded response, so the generic
	// three-seconds-of-RPS concurrency would allow thousands of full logs to
	// remain live at once. At most one second of the configured request budget,
	// capped at 1000 per target pool, covers the observed upstream latency while
	// keeping retained responses bounded when Postgres becomes the slower side.
	return min(requestsPerSecond, 1000)
}

func mergeBattlelogIngests(values []models.BattlelogIngest) models.BattlelogIngest {
	var totalRows, totalCheckpoints int
	for _, value := range values {
		totalRows += len(value.Rows)
		totalCheckpoints += len(value.Checkpoints)
	}
	out := models.BattlelogIngest{
		Rows:        make([]models.BattlelogRow, 0, totalRows),
		Checkpoints: make([]models.BattlelogCheckpoint, 0, totalCheckpoints),
	}
	checkpoints := make(map[string]models.BattlelogCheckpoint, totalCheckpoints)
	for _, value := range values {
		out.Rows = append(out.Rows, value.Rows...)
		for _, checkpoint := range value.Checkpoints {
			if checkpoint.Tag == "" || checkpoint.Timestamp.IsZero() {
				continue
			}
			current, ok := checkpoints[checkpoint.Tag]
			if !ok || checkpoint.Timestamp.After(current.Timestamp) {
				checkpoints[checkpoint.Tag] = checkpoint
			}
		}
	}
	if len(checkpoints) > 0 {
		tags := make([]string, 0, len(checkpoints))
		for tag := range checkpoints {
			tags = append(tags, tag)
		}
		sort.Strings(tags)
		out.Checkpoints = out.Checkpoints[:0]
		for _, tag := range tags {
			out.Checkpoints = append(out.Checkpoints, checkpoints[tag])
		}
	}
	return out
}

func (d *battlelogsDomain) do(
	ctx context.Context,
	app *platform.App,
	statsName string,
	playerTag string,
	checkpoint models.BattlelogCheckpoint,
) (models.BattlelogIngest, error) {
	entries, err := d.fetchBattleLog(ctx, app, statsName, playerTag)
	if err != nil {
		return models.BattlelogIngest{}, err
	}

	now := time.Now().UTC()
	if len(entries) == 0 {
		return models.BattlelogIngest{}, nil
	}

	after := checkpoint.Timestamp
	if after.IsZero() {
		// First-seen players only backfill a bounded window so old accounts do not fan out
		// into unbounded historical ingestion on their first poll.
		after = now.Add(-time.Duration(app.Config.BattlelogFirstSeenLookbackDays) * 24 * time.Hour)
	}
	newEntries := entriesAfterTimestamp(entries, after)
	if len(newEntries) == 0 {
		return models.BattlelogIngest{}, nil
	}
	sort.Slice(newEntries, func(i, j int) bool {
		leftTime := battlelogEntryTimestamp(newEntries[i])
		rightTime := battlelogEntryTimestamp(newEntries[j])
		if leftTime.Equal(rightTime) {
			return newEntries[i].OpponentPlayerTag < newEntries[j].OpponentPlayerTag
		}
		return leftTime.Before(rightTime)
	})

	rows := make([]models.BattlelogRow, 0, len(newEntries))
	var checkpointTime time.Time
	for _, entry := range newEntries {
		timestamp := battlelogEntryTimestamp(entry)
		if timestamp.IsZero() || entry.OpponentPlayerTag == "" || entry.OpponentName == "" || entry.OpponentTownHallLevel < 0 {
			// Do not checkpoint past incomplete rows; later polls can retry after
			// the API returns the required battle timestamp and opponent metadata.
			break
		}
		rows = append(rows, battlelogRowFromEntry(playerTag, entry))
		if timestamp.After(checkpointTime) {
			checkpointTime = timestamp.UTC()
		}
	}
	if len(rows) == 0 {
		return models.BattlelogIngest{}, nil
	}

	ingest := models.BattlelogIngest{
		Rows: rows,
	}
	if !checkpointTime.IsZero() {
		ingest.Checkpoints = []models.BattlelogCheckpoint{{Tag: playerTag, Timestamp: checkpointTime}}
	}
	return ingest, nil
}

func (d *battlelogsDomain) store(ctx context.Context, app *platform.App, ingest models.BattlelogIngest) error {
	start := time.Now()
	insertedRows := 0
	if d.sink != nil {
		var err error
		insertedRows, err = d.sink.Store(ctx, ingest)
		if err != nil {
			return err
		}
	}
	if !app.Config.DryRun {
		// Checkpoints move only after durable rows write successfully.
		if err := d.checkpoint.UpdateMany(ctx, ingest.Checkpoints); err != nil {
			return err
		}
	}
	app.Stats.RecordWrite(battlelogsDomainName, len(ingest.Rows)+len(ingest.Checkpoints))
	app.Stats.RecordStore(battlelogsDomainName, time.Since(start), len(ingest.Rows), insertedRows)
	app.Stats.SetReady(battlelogsDomainName, true, "")
	return nil
}

func (d *battlelogsDomain) fetchBattleLog(ctx context.Context, app *platform.App, statsName string, tag string) ([]clashy.BattleLogEntry, error) {
	start := time.Now()
	entries, err := app.Clash.GetBattleLog(ctx, tag)
	app.Stats.RecordRequest(statsName, time.Since(start), err)
	return entries, err
}

func newTimescaleBattlelogStore(ctx context.Context, dsn string) (*timescaleBattlelogStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleBattlelogStore{pool: pool}, nil
}

func (s *timescaleBattlelogStore) Close() error {
	if s == nil || s.pool == nil {
		return nil
	}
	s.pool.Close()
	return nil
}

func (s *timescaleBattlelogStore) LoadTargets(ctx context.Context, group string) ([]string, error) {
	query := `
		SELECT DISTINCT player.tag
		FROM (` + trackedPlayerTargetSetSQL + `) target
		JOIN basic_player player ON player.tag = target.tag
		WHERE player.league_id IS DISTINCT FROM 105000036
		ORDER BY player.tag
	`
	if group == "legend" {
		query = `
			SELECT tag
			FROM basic_player
			WHERE league_id = 105000036
			ORDER BY tag
		`
	}
	rows, err := s.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tags []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		if tag == "" {
			continue
		}
		tags = append(tags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tags, nil
}

func (s *timescaleBattlelogStore) FilterStandardTargets(ctx context.Context, tags []string) ([]string, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT tag
		FROM basic_player
		WHERE tag = ANY($1)
		  AND league_id IS DISTINCT FROM 105000036
		ORDER BY tag
	`, tags)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	filtered := make([]string, 0, len(tags))
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		filtered = append(filtered, tag)
	}
	return filtered, rows.Err()
}

func (s *timescaleBattlelogStore) Store(ctx context.Context, ingest models.BattlelogIngest) (int, error) {
	if len(ingest.Rows) == 0 && len(ingest.Checkpoints) == 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	insertedRows, err := s.insertBattlelogRows(ctx, tx, ingest.Rows)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return insertedRows, nil
}

func (s *timescaleBattlelogStore) insertBattlelogRows(ctx context.Context, tx pgx.Tx, rows []models.BattlelogRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE battlelog_ingest_stage (
			battle_id uuid NOT NULL,
			army_share_code text NOT NULL,
			player_tag text NOT NULL,
			opponent_tag text NOT NULL,
			opponent_name text NOT NULL,
			opponent_th smallint NOT NULL,
			battle_type text NOT NULL,
			attack boolean NOT NULL,
			stars smallint NOT NULL,
			destruction_percentage smallint NOT NULL,
			gold integer NOT NULL,
			elixir integer NOT NULL,
			dark_elixir integer NOT NULL,
			duration integer NOT NULL,
			battle_time timestamp with time zone NOT NULL,
			army_items text[] NOT NULL,
			army_counts text NOT NULL
	) ON COMMIT DROP
	`); err != nil {
		return 0, err
	}

	copyRows := make([][]any, 0, len(rows))
	for _, row := range rows {
		armyItems, armyCounts, err := armyItemsAndCounts(row.ArmyColumns)
		if err != nil {
			return 0, err
		}
		copyRows = append(copyRows, []any{
			row.BattleID, row.ArmyShareCode, row.PlayerTag,
			row.OpponentTag, row.OpponentName, int16(row.OpponentTH),
			row.BattleType, row.Attack,
			int16(row.Stars), int16(row.DestructionPercentage), int32(row.Gold),
			int32(row.Elixir), int32(row.DarkElixir), int32(row.Duration), row.Timestamp,
			armyItems, armyCounts,
		})
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"battlelog_ingest_stage"}, []string{
		"battle_id",
		"army_share_code",
		"player_tag",
		"opponent_tag",
		"opponent_name",
		"opponent_th",
		"battle_type",
		"attack",
		"stars",
		"destruction_percentage",
		"gold",
		"elixir",
		"dark_elixir",
		"duration",
		"battle_time",
		"army_items",
		"army_counts",
	}, pgx.CopyFromRows(copyRows))
	if err != nil {
		return 0, err
	}

	tag, err := tx.Exec(ctx, `
		INSERT INTO battlelogs (
			battle_id, army_share_code, player_tag, player_name, player_th,
			opponent_tag, opponent_name, opponent_th, battle_type, attack,
			stars, destruction_percentage, gold, elixir, dark_elixir, duration, timestamp,
			army_items, army_counts
		)
		SELECT
			stage.battle_id,
			stage.army_share_code,
			stage.player_tag,
			basic_player.name,
			basic_player.townhall_level,
			stage.opponent_tag,
			stage.opponent_name,
			stage.opponent_th,
			stage.battle_type,
			stage.attack,
			stage.stars,
			stage.destruction_percentage,
			stage.gold,
			stage.elixir,
			stage.dark_elixir,
			stage.duration,
			stage.battle_time,
			stage.army_items,
			stage.army_counts::jsonb
		FROM battlelog_ingest_stage AS stage
		JOIN basic_player ON basic_player.tag = stage.player_tag
		ON CONFLICT (battle_id, timestamp) DO NOTHING
	`)
	inserted := int(tag.RowsAffected())
	return inserted, err
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

type battlelogTimestampCache struct {
	client valkey.Client
	ttl    time.Duration
}

func (c battlelogTimestampCache) GetMany(ctx context.Context, tags []string) (map[string]models.BattlelogCheckpoint, error) {
	out := make(map[string]models.BattlelogCheckpoint, len(tags))
	if c.client == nil || len(tags) == 0 {
		return out, nil
	}
	keys := make([]string, 0, len(tags))
	for _, tag := range tags {
		keys = append(keys, battlelogCheckpointKey(tag))
	}
	values, err := c.client.Do(ctx, c.client.B().Mget().Key(keys...).Build()).ToArray()
	if err != nil {
		return nil, err
	}
	for i, value := range values {
		if i >= len(tags) {
			break
		}
		raw, err := value.ToString()
		if valkey.IsValkeyNil(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		timestamp, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return nil, err
		}
		out[tags[i]] = models.BattlelogCheckpoint{Tag: tags[i], Timestamp: timestamp.UTC()}
	}
	return out, nil
}

func (c battlelogTimestampCache) Get(ctx context.Context, tag string) (models.BattlelogCheckpoint, error) {
	if c.client == nil || tag == "" {
		return models.BattlelogCheckpoint{Tag: tag}, nil
	}
	value, err := c.client.Do(ctx, c.client.B().Get().Key(battlelogCheckpointKey(tag)).Build()).ToString()
	if valkey.IsValkeyNil(err) {
		return models.BattlelogCheckpoint{Tag: tag}, nil
	}
	if err != nil {
		return models.BattlelogCheckpoint{}, err
	}
	timestamp, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return models.BattlelogCheckpoint{}, err
	}
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
	results := c.client.DoMulti(ctx, commands...)
	for _, result := range results {
		if err := result.Error(); err != nil {
			return err
		}
	}
	return nil
}

func battlelogCheckpointKey(tag string) string {
	return "bl:" + tag
}

func entriesAfterTimestamp(entries []clashy.BattleLogEntry, after time.Time) []clashy.BattleLogEntry {
	out := make([]clashy.BattleLogEntry, 0, len(entries))
	for _, entry := range entries {
		timestamp := battlelogEntryTimestamp(entry)
		if !timestamp.IsZero() && timestamp.After(after) {
			out = append(out, entry)
		}
	}
	return out
}

func latestBattlelogTimestamp(entries []clashy.BattleLogEntry) time.Time {
	var latest time.Time
	for _, entry := range entries {
		timestamp := battlelogEntryTimestamp(entry)
		if timestamp.After(latest) {
			latest = timestamp
		}
	}
	return latest.UTC()
}

func battlelogEntryTimestamp(entry clashy.BattleLogEntry) time.Time {
	if entry.Timestamp == "" {
		return time.Time{}
	}
	timestamp, err := clashy.FromTimestamp(entry.Timestamp)
	if err != nil {
		return time.Time{}
	}
	return timestamp.UTC()
}

func battlelogRowFromEntry(playerTag string, entry clashy.BattleLogEntry) models.BattlelogRow {
	gold, elixir, darkElixir := lootedResourceColumns(entry.LootedResources)
	armyColumns := parseArmyColumns(entry.ArmyShareCode)
	timestamp := battlelogEntryTimestamp(entry)
	return models.BattlelogRow{
		BattleID:              battlelogBattleID(playerTag, entry),
		ArmyShareCode:         normalizeArmyShareCode(entry.ArmyShareCode),
		PlayerTag:             playerTag,
		OpponentTag:           entry.OpponentPlayerTag,
		OpponentName:          entry.OpponentName,
		OpponentTH:            uint8(entry.OpponentTownHallLevel + 1),
		BattleType:            string(entry.BattleType),
		Attack:                entry.Attack,
		Stars:                 uint8(entry.Stars),
		DestructionPercentage: uint8(entry.DestructionPercentage),
		Gold:                  uint32(gold),
		Elixir:                uint32(elixir),
		DarkElixir:            uint32(darkElixir),
		Duration:              uint16(entry.Duration),
		Timestamp:             timestamp,
		ArmyColumns:           armyColumns,
	}
}

func battlelogBattleID(playerTag string, entry clashy.BattleLogEntry) uuid.UUID {
	leftTag, rightTag := orderedBattlelogTags(playerTag, entry.OpponentPlayerTag)
	timestamp := battlelogEntryTimestamp(entry)
	key := strings.Join([]string{
		leftTag,
		rightTag,
		timestamp.Format(time.RFC3339Nano),
	}, "|")
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(key))
}

func orderedBattlelogTags(left, right string) (string, string) {
	if right < left {
		return right, left
	}
	return left, right
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
	// pets, equipment, and siege machines share one storage shape.
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

func normalizeArmyShareCode(link string) string {
	payload := extractArmySharePayload(link)
	sections := splitArmyShareSections(payload)
	if len(sections) == 0 {
		return ""
	}
	heroes := make([]armyHeroLoadout, 0)
	itemSections := map[byte]map[int]uint16{
		'i': {},
		'd': {},
		'u': {},
		's': {},
	}
	for _, section := range sections {
		if len(section) < 2 {
			continue
		}
		switch section[0] {
		case 'h':
			heroes = append(heroes, parseArmyHeroLoadouts(section[1:])...)
		case 'i', 'd', 'u', 's':
			parseArmyItemCountsSection(section[1:], itemSections[section[0]])
		}
	}

	var parts []string
	if encoded := encodeArmyHeroSection(heroes); encoded != "" {
		parts = append(parts, encoded)
	}
	for _, marker := range []byte{'i', 'd', 'u', 's'} {
		if encoded := encodeArmyItemSection(marker, itemSections[marker]); encoded != "" {
			parts = append(parts, encoded)
		}
	}
	return strings.Join(parts, "")
}

type armyHeroLoadout struct {
	HeroID    int
	PetID     int
	Equipment []int
}

func parseArmyHeroLoadouts(payload string) []armyHeroLoadout {
	var heroes []armyHeroLoadout
	for _, part := range strings.Split(payload, "-") {
		if part == "" {
			continue
		}
		heroID, rest := leadingInt(part)
		if heroID < 0 {
			continue
		}
		loadout := armyHeroLoadout{HeroID: heroID}
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
				if marker == 'p' {
					loadout.PetID = value
				} else {
					loadout.Equipment = append(loadout.Equipment, value)
				}
			}
			rest = next
			if strings.HasPrefix(rest, "_") {
				value, next = leadingInt(rest[1:])
				if value >= 0 {
					loadout.Equipment = append(loadout.Equipment, value)
				}
				rest = next
			}
		}
		sort.Ints(loadout.Equipment)
		heroes = append(heroes, loadout)
	}
	sort.Slice(heroes, func(i, j int) bool {
		if heroes[i].HeroID != heroes[j].HeroID {
			return heroes[i].HeroID < heroes[j].HeroID
		}
		if heroes[i].PetID != heroes[j].PetID {
			return heroes[i].PetID < heroes[j].PetID
		}
		left := intsKey(heroes[i].Equipment)
		right := intsKey(heroes[j].Equipment)
		return left < right
	})
	return heroes
}

func encodeArmyHeroSection(heroes []armyHeroLoadout) string {
	if len(heroes) == 0 {
		return ""
	}
	parts := make([]string, 0, len(heroes))
	for _, hero := range heroes {
		part := strconv.Itoa(hero.HeroID)
		if hero.PetID > 0 {
			part += "p" + strconv.Itoa(hero.PetID)
		}
		if len(hero.Equipment) > 0 {
			equipment := make([]string, 0, len(hero.Equipment))
			for _, id := range hero.Equipment {
				if id >= 0 {
					equipment = append(equipment, strconv.Itoa(id))
				}
			}
			if len(equipment) > 0 {
				part += "e" + strings.Join(equipment, "_")
			}
		}
		parts = append(parts, part)
	}
	return "h" + strings.Join(parts, "-")
}

func encodeArmyItemSection(marker byte, items map[int]uint16) string {
	if len(items) == 0 {
		return ""
	}
	ids := make([]int, 0, len(items))
	for id, qty := range items {
		if qty > 0 {
			ids = append(ids, id)
		}
	}
	sort.Ints(ids)
	if len(ids) == 0 {
		return ""
	}
	parts := make([]string, 0, len(ids))
	for _, id := range ids {
		parts = append(parts, fmt.Sprintf("%dx%d", items[id], id))
	}
	return string(marker) + strings.Join(parts, "-")
}

func parseArmyItemCountsSection(payload string, counts map[int]uint16) {
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
		counts[id] += uint16(qty)
	}
}

func intsKey(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, "_")
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
