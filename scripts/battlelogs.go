package scripts

import (
	"context"
	"crypto/sha256"
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
	battlelogAsyncWriteBatchSize     = 250
	battlelogAsyncWriteQueueSize     = 500
	battlelogAsyncWriteFlushInterval = 500 * time.Millisecond
	// Target tags and checkpoint timestamps are small. A larger page amortizes
	// SQL/MGET work and, more importantly, avoids waiting at a page barrier for
	// a handful of retrying 504s every few seconds. Decoded response memory is
	// bounded independently by battlelogRequestConcurrency and the write queue.
	battlelogTargetPageSize = 20000
	// Ranked attacks that begin before the weekly reset can complete a few
	// minutes afterward. Keep that narrow completion tail in the prior season.
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

func battlelogRequestConcurrency(requestsPerSecond int) int {
	if requestsPerSecond <= 0 {
		return 0
	}
	// Battle-log responses are much larger than clan/player summaries. A worker
	// blocked on SQL backpressure retains its decoded response, so keep the hard
	// cap. Within that bound, allow the same three seconds of latency headroom as
	// the other trackers. A five-second experiment improved full-population
	// throughput by only two percent while increasing peak allocation by half.
	return min(platform.RequestConcurrency(requestsPerSecond), 1000)
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
	return battlelogIngestFromEntries(entries, playerTag, checkpoint, time.Now().UTC(), app.Config.BattlelogFirstSeenLookbackDays)
}

func battlelogIngestFromEntries(entries []clashy.BattleLogEntry, playerTag string, checkpoint models.BattlelogCheckpoint, now time.Time, firstSeenLookbackDays int) (models.BattlelogIngest, error) {
	if len(entries) == 0 {
		return models.BattlelogIngest{}, nil
	}

	after := checkpoint.Timestamp
	if after.IsZero() {
		// First-seen players only backfill a bounded window so old accounts do not fan out
		// into unbounded historical ingestion on their first poll.
		after = now.Add(-time.Duration(firstSeenLookbackDays) * 24 * time.Hour)
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
		if timestamp.IsZero() {
			// Do not checkpoint past incomplete rows; later polls can retry after
			// the API returns the required battle timestamp.
			break
		}
		include := false
		switch battlelogStorageMode(entry.BattleType) {
		case "farming":
			// Farming history is intentionally attack-only and omits all opponent
			// metadata. Home-village defenses can safely advance the checkpoint.
			include = entry.Attack
		case "ranked", "legend":
			if entry.OpponentPlayerTag == "" || entry.OpponentTownHallLevel <= 0 {
				// A real battle needs both perspectives, so retry incomplete rows.
				return models.BattlelogIngest{}, nil
			}
			include = true
		default:
			// Unknown modes are outside this persistence contract.
		}
		if include {
			row := battlelogRowFromEntry(playerTag, entry)
			if row.ArmyShareCode == "" {
				return models.BattlelogIngest{}, nil
			}
			rows = append(rows, row)
		}
		if timestamp.After(checkpointTime) {
			checkpointTime = timestamp.UTC()
		}
	}
	if checkpointTime.IsZero() {
		return models.BattlelogIngest{}, nil
	}

	ingest := models.BattlelogIngest{
		Rows: rows,
	}
	ingest.Checkpoints = []models.BattlelogCheckpoint{{Tag: playerTag, Timestamp: checkpointTime}}
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

func (s *timescaleBattlelogStore) Store(ctx context.Context, ingest models.BattlelogIngest) (int, error) {
	if len(ingest.Rows) == 0 {
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
		CREATE TEMP TABLE IF NOT EXISTS battlelog_ingest_stage (
			requested_tag text NOT NULL,
			opponent_tag text NOT NULL,
			opponent_th smallint NOT NULL,
			mode text NOT NULL,
			requested_attack boolean NOT NULL,
			stars smallint NOT NULL,
			destruction_percentage smallint NOT NULL,
			looted_resources jsonb NOT NULL,
			duration_seconds integer NOT NULL,
			battle_time timestamp with time zone NOT NULL,
			army_hash bytea NOT NULL,
			army_share_code text NOT NULL
		) ON COMMIT DELETE ROWS
	`); err != nil {
		return 0, err
	}

	copyRows := make([][]any, 0, len(rows))
	compositions := make(map[[sha256.Size]byte]models.BattlelogRow)
	for _, row := range rows {
		playerTag := clashy.CorrectTag(row.PlayerTag)
		if playerTag == "" {
			return 0, errors.New("battlelog player tag is empty")
		}
		opponentTag := clashy.CorrectTag(row.OpponentTag)
		if opponentTag == "" {
			opponentTag = "#0"
		}
		mode := battlelogStorageMode(clashy.BattleType(row.BattleType))
		if mode == "" {
			return 0, fmt.Errorf("unsupported battle type %q", row.BattleType)
		}
		loot, err := json.Marshal(map[string]uint32{
			"gold": row.Gold, "elixir": row.Elixir, "darkElixir": row.DarkElixir,
		})
		if err != nil {
			return 0, err
		}
		copyRows = append(copyRows, []any{
			playerTag, opponentTag, int16(row.OpponentTH), mode, row.Attack,
			int16(row.Stars), int16(row.DestructionPercentage), loot,
			int32(row.Duration), row.Timestamp, row.ArmyHash[:], row.ArmyShareCode,
		})
		// Detailed army analytics cover Legend battles only. Keep the share code
		// and hash in every raw battle without materializing Ranked compositions.
		if mode == "legend" {
			compositions[row.ArmyHash] = row
		}
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"battlelog_ingest_stage"}, []string{
		"requested_tag", "opponent_tag", "opponent_th", "mode", "requested_attack",
		"stars", "destruction_percentage", "looted_resources", "duration_seconds",
		"battle_time", "army_hash", "army_share_code",
	}, pgx.CopyFromRows(copyRows)); err != nil {
		return 0, err
	}
	if err := insertArmyCompositions(ctx, tx, compositions); err != nil {
		return 0, err
	}

	farmingTag, err := tx.Exec(ctx, `
		INSERT INTO battles_farming (
			player_tag, battle_time, stars, destruction_percentage, duration_seconds,
			looted_resources, share_code
		)
		SELECT requested_tag, battle_time, stars, destruction_percentage,
			NULLIF(duration_seconds, 0), looted_resources, NULLIF(army_share_code, '')
		FROM battlelog_ingest_stage
		WHERE mode = 'farming' AND requested_attack
		ON CONFLICT (player_tag, battle_time) DO NOTHING
	`)
	if err != nil {
		return 0, err
	}

	var rankedInserted int
	err = tx.QueryRow(ctx, `
		WITH observations AS (
			SELECT stage.*, requested.townhall_level AS requested_th
			FROM battlelog_ingest_stage stage
			JOIN basic_player requested ON requested.tag = stage.requested_tag
			WHERE stage.mode IN ('ranked', 'legend')
			  AND requested.townhall_level BETWEEN 1 AND 20
		), battles AS (
			SELECT observations.*,
				CASE WHEN requested_attack THEN requested_tag ELSE opponent_tag END AS attacker_tag,
				CASE WHEN requested_attack THEN opponent_tag ELSE requested_tag END AS defender_tag,
				CASE WHEN requested_attack THEN requested_th ELSE opponent_th END AS attacker_th,
				CASE WHEN requested_attack THEN opponent_th ELSE requested_th END AS defender_th
			FROM observations
		), perspectives AS (
			SELECT perspective.player_tag, perspective.opponent_tag, perspective.direction,
				perspective.player_th, perspective.opponent_th, battles.battle_time,
				battles.mode, battles.army_hash, battles.stars, battles.destruction_percentage,
				battles.duration_seconds, battles.looted_resources, battles.army_share_code
			FROM battles
			CROSS JOIN LATERAL (VALUES
				(attacker_tag, defender_tag, 'attack'::text, attacker_th, defender_th),
				(defender_tag, attacker_tag, 'defense'::text, defender_th, attacker_th)
			) perspective(player_tag, opponent_tag, direction, player_th, opponent_th)
		), inserted AS (
			INSERT INTO battles_ranked (
				player_tag, battle_time, direction, opponent_tag, battle_mode,
				player_town_hall, opponent_town_hall, stars, destruction_percentage,
				duration_seconds, looted_resources, share_code, army_hash
			)
			SELECT player_tag, battle_time, direction, opponent_tag, mode,
				player_th, opponent_th, stars, destruction_percentage,
				NULLIF(duration_seconds, 0), looted_resources,
				NULLIF(army_share_code, ''), army_hash
			FROM perspectives
			ON CONFLICT (player_tag, battle_time, battle_mode, direction, opponent_tag) DO NOTHING
			RETURNING 1
		)
		SELECT count(*)::integer FROM inserted
	`).Scan(&rankedInserted)
	if err != nil {
		return 0, err
	}
	return int(farmingTag.RowsAffected()) + rankedInserted, nil
}

type armyQuantity struct {
	ID       int `json:"id"`
	Quantity int `json:"quantity"`
}

type armySpellQuantity struct {
	ID         int  `json:"id"`
	Quantity   int  `json:"quantity"`
	ClanCastle bool `json:"clanCastle"`
}

type armyHeroEquipment struct {
	HeroID      int `json:"heroId"`
	EquipmentID int `json:"equipmentId"`
}

type armyPetAssignment struct {
	HeroID int `json:"heroId"`
	PetID  int `json:"petId"`
}

type armyCompositionRecord struct {
	MainTroops       []armyQuantity
	ClanCastleTroops []armyQuantity
	Spells           []armySpellQuantity
	Heroes           []int32
	Equipment        []armyHeroEquipment
	PetAssignments   []armyPetAssignment
	SiegeMachineID   *int32
}

func insertArmyCompositions(ctx context.Context, tx pgx.Tx, compositions map[[sha256.Size]byte]models.BattlelogRow) error {
	if len(compositions) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS battlelog_army_composition_stage (
			army_hash bytea NOT NULL,
			normalized_share_code text NOT NULL,
			main_troops jsonb NOT NULL,
			clan_castle_troops jsonb NOT NULL,
			spells jsonb NOT NULL,
			heroes integer[] NOT NULL,
			equipment jsonb NOT NULL,
			pet_assignments jsonb NOT NULL,
			siege_machine_id integer
		) ON COMMIT DELETE ROWS
	`); err != nil {
		return err
	}

	hashes := make([][sha256.Size]byte, 0, len(compositions))
	shareCodeHashes := make(map[string][sha256.Size]byte, len(compositions))
	for hash, row := range compositions {
		if existing, ok := shareCodeHashes[row.ArmyShareCode]; ok && existing != hash {
			return fmt.Errorf("normalized army share code maps to multiple composition hashes: %q (%x, %x)", row.ArmyShareCode, existing, hash)
		}
		shareCodeHashes[row.ArmyShareCode] = hash
		hashes = append(hashes, hash)
	}
	sort.Slice(hashes, func(i, j int) bool { return strings.Compare(string(hashes[i][:]), string(hashes[j][:])) < 0 })
	static, err := clashy.LoadStaticData()
	if err != nil {
		return fmt.Errorf("load static data for army composition: %w", err)
	}
	compositionRows := make([][]any, 0, len(hashes))
	for _, hash := range hashes {
		row := compositions[hash]
		record := armyCompositionFromColumns(static, row.ArmyShareCode, row.ArmyColumns)
		mainTroops, _ := json.Marshal(record.MainTroops)
		clanCastleTroops, _ := json.Marshal(record.ClanCastleTroops)
		spells, _ := json.Marshal(record.Spells)
		equipment, _ := json.Marshal(record.Equipment)
		petAssignments, _ := json.Marshal(record.PetAssignments)
		compositionRows = append(compositionRows, []any{
			hash[:], row.ArmyShareCode, json.RawMessage(mainTroops), json.RawMessage(clanCastleTroops),
			json.RawMessage(spells), record.Heroes, json.RawMessage(equipment),
			json.RawMessage(petAssignments), record.SiegeMachineID,
		})
	}
	columns := []string{
		"army_hash", "normalized_share_code", "main_troops", "clan_castle_troops",
		"spells", "heroes", "equipment", "pet_assignments", "siege_machine_id",
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"battlelog_army_composition_stage"}, columns, pgx.CopyFromRows(compositionRows)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO army_compositions (
			army_hash, normalized_share_code, main_troops, clan_castle_troops,
			spells, heroes, equipment, pet_assignments, siege_machine_id
		)
		SELECT army_hash, normalized_share_code, main_troops, clan_castle_troops,
			spells, heroes, equipment, pet_assignments, siege_machine_id
		FROM battlelog_army_composition_stage
		ON CONFLICT (army_hash) DO NOTHING
	`); err != nil {
		return err
	}
	var mismatchCount int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)::integer
		FROM battlelog_army_composition_stage staged
		JOIN army_compositions stored USING (army_hash)
		WHERE staged.normalized_share_code <> stored.normalized_share_code
		   OR staged.main_troops <> stored.main_troops
		   OR staged.clan_castle_troops <> stored.clan_castle_troops
		   OR staged.spells <> stored.spells
		   OR staged.heroes <> stored.heroes
		   OR staged.equipment <> stored.equipment
		   OR staged.pet_assignments <> stored.pet_assignments
		   OR staged.siege_machine_id IS DISTINCT FROM stored.siege_machine_id
	`).Scan(&mismatchCount); err != nil {
		return err
	}
	if mismatchCount > 0 {
		return fmt.Errorf("%d canonical army hashes conflict with immutable compositions", mismatchCount)
	}
	return nil
}

func armyCompositionFromColumns(static *clashy.StaticData, shareCode string, columns map[string]uint16) armyCompositionRecord {
	record := armyCompositionRecord{
		MainTroops: []armyQuantity{}, ClanCastleTroops: []armyQuantity{},
		Spells: []armySpellQuantity{}, Heroes: []int32{},
		Equipment: []armyHeroEquipment{}, PetAssignments: []armyPetAssignment{},
	}
	for _, key := range sortedArmyColumnKeys(columns) {
		section, localID := splitArmyColumn(key)
		quantity := int(columns[key])
		switch section {
		case "u":
			id := clashy.TroopBaseID + localID
			if data := static.LookupByID(id); data != nil && data["production_building"] == "Workshop" {
				value := int32(id)
				record.SiegeMachineID = &value
			} else {
				record.MainTroops = append(record.MainTroops, armyQuantity{ID: id, Quantity: quantity})
			}
		case "i":
			record.ClanCastleTroops = append(record.ClanCastleTroops, armyQuantity{ID: clashy.TroopBaseID + localID, Quantity: quantity})
		case "s":
			record.Spells = append(record.Spells, armySpellQuantity{ID: clashy.SpellBaseID + localID, Quantity: quantity})
		case "d":
			record.Spells = append(record.Spells, armySpellQuantity{ID: clashy.SpellBaseID + localID, Quantity: quantity, ClanCastle: true})
		case "h":
			record.Heroes = append(record.Heroes, int32(clashy.HeroBaseID+localID))
		}
	}
	for _, section := range splitArmyShareSections(extractArmySharePayload(shareCode)) {
		if len(section) == 0 || section[0] != 'h' {
			continue
		}
		for _, loadout := range parseArmyHeroLoadouts(section[1:]) {
			heroID := clashy.HeroBaseID + loadout.HeroID
			if loadout.PetID >= 0 {
				record.PetAssignments = append(record.PetAssignments, armyPetAssignment{HeroID: heroID, PetID: clashy.PetBaseID + loadout.PetID})
			}
			for _, equipmentID := range loadout.Equipment {
				record.Equipment = append(record.Equipment, armyHeroEquipment{HeroID: heroID, EquipmentID: clashy.EquipmentBaseID + equipmentID})
			}
		}
	}
	sort.Slice(record.MainTroops, func(i, j int) bool { return record.MainTroops[i].ID < record.MainTroops[j].ID })
	sort.Slice(record.ClanCastleTroops, func(i, j int) bool { return record.ClanCastleTroops[i].ID < record.ClanCastleTroops[j].ID })
	sort.Slice(record.Spells, func(i, j int) bool {
		if record.Spells[i].ID != record.Spells[j].ID {
			return record.Spells[i].ID < record.Spells[j].ID
		}
		return !record.Spells[i].ClanCastle && record.Spells[j].ClanCastle
	})
	sort.Slice(record.Equipment, func(i, j int) bool {
		if record.Equipment[i].EquipmentID != record.Equipment[j].EquipmentID {
			return record.Equipment[i].EquipmentID < record.Equipment[j].EquipmentID
		}
		return record.Equipment[i].HeroID < record.Equipment[j].HeroID
	})
	sort.Slice(record.PetAssignments, func(i, j int) bool {
		if record.PetAssignments[i].PetID != record.PetAssignments[j].PetID {
			return record.PetAssignments[i].PetID < record.PetAssignments[j].PetID
		}
		return record.PetAssignments[i].HeroID < record.PetAssignments[j].HeroID
	})
	return record
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
	armyShareCode := normalizeArmyShareCode(entry.ArmyShareCode)
	timestamp := battlelogEntryTimestamp(entry)
	return models.BattlelogRow{
		ArmyShareCode:         armyShareCode,
		ArmyHash:              canonicalArmyHash(armyShareCode),
		PlayerTag:             playerTag,
		OpponentTag:           entry.OpponentPlayerTag,
		OpponentTH:            uint8(entry.OpponentTownHallLevel),
		BattleType:            battlelogStorageMode(entry.BattleType),
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

func battlelogStorageMode(value clashy.BattleType) string {
	normalized := strings.ToLower(strings.NewReplacer("_", "", "-", "", " ", "").Replace(string(value)))
	switch normalized {
	case "homevillage", "farming":
		return "farming"
	case "ranked":
		return "ranked"
	case "legend":
		return "legend"
	default:
		return ""
	}
}

func canonicalArmyHash(normalizedShareCode string) [sha256.Size]byte {
	// The canonical share code sorts sections, heroes, pets, equipment, and item
	// IDs. Hash the complete normalized loadout so equal item counts with
	// different hero assignments cannot collapse into one composition.
	hash := sha256.New()
	hash.Write([]byte{2})
	hash.Write([]byte(normalizedShareCode))
	var out [sha256.Size]byte
	copy(out[:], hash.Sum(nil))
	return out
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
		loadout := armyHeroLoadout{HeroID: heroID, PetID: -1}
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
		if hero.PetID >= 0 {
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
