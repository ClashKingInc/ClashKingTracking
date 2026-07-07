package scripts

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type timescaleWarStore struct {
	pool    *pgxpool.Pool
	objects platform.ObjectStore
}

func newTimescaleWarStore(ctx context.Context, dsn string, objects platform.ObjectStore) (*timescaleWarStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleWarStore{pool: pool, objects: objects}, nil
}

func (s *timescaleWarStore) Close() error {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
	return nil
}

func (s *timescaleWarStore) LoadPendingSchedules(ctx context.Context) ([]models.WarScheduleRow, error) {
	// Pending schedules are reloaded into the process-local timer wheel during Run startup.
	rows, err := s.pool.Query(ctx, `
		SELECT schedule_key, war_id, source_clan_tag, opponent_tag, prep_time,
		       end_time, next_run_at, COALESCE(war_tag, '')
		FROM war_schedule
		ORDER BY next_run_at
	`)
	if err != nil {
		return nil, warStoreError("load schedules", err)
	}
	defer rows.Close()
	var out []models.WarScheduleRow
	for rows.Next() {
		var row models.WarScheduleRow
		if err := rows.Scan(&row.ScheduleKey, &row.WarID, &row.SourceClanTag, &row.OpponentTag, &row.PrepTime, &row.EndTime, &row.NextRunAt, &row.WarTag); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *timescaleWarStore) LoadCWLLeague(ctx context.Context, tag string) (int, error) {
	var leagueID int
	err := s.pool.QueryRow(ctx, `SELECT cwl_league_id FROM basic_clan WHERE tag = $1`, tag).Scan(&leagueID)
	if err != nil {
		return 0, warStoreError("load cwl league", err)
	}
	return leagueID, nil
}

func (s *timescaleWarStore) Store(ctx context.Context, ingest models.WarIngest) error {
	if len(ingest.IndexRows) == 0 && len(ingest.AttackRows) == 0 && len(ingest.Players) == 0 && len(ingest.Schedules) == 0 && len(ingest.CWLGroups) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if ingest.FinishedWarID != "" {
		if err := s.prepareFinishedWar(ctx, tx, &ingest); err != nil {
			return err
		}
		if ingest.FinishedWarID == "" {
			return nil
		}
		if err := s.storeFinishedWarObject(ctx, ingest); err != nil {
			return err
		}
	}
	if err := utils.UpsertBasicPlayers(ctx, tx, ingest.Players, warsDomainName); err != nil {
		return err
	}
	if err := insertWarIndexRows(ctx, tx, ingest.IndexRows); err != nil {
		return err
	}
	if err := insertWarAttackRows(ctx, tx, ingest.AttackRows); err != nil {
		return err
	}
	if err := touchWarAttackPlayers(ctx, tx, ingest.AttackRows); err != nil {
		return err
	}
	if err := upsertWarSchedules(ctx, tx, ingest.Schedules); err != nil {
		return err
	}
	if err := upsertCWLGroups(ctx, tx, ingest.CWLGroups); err != nil {
		return err
	}
	if ingest.FinishedWarID == "" {
		return tx.Commit(ctx)
	}
	if err := deleteWarSchedule(ctx, tx, ingest.FinishedScheduleKey); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *timescaleWarStore) prepareFinishedWar(ctx context.Context, tx pgx.Tx, ingest *models.WarIngest) error {
	if ingest.FinishedScheduleKey == "" {
		return nil
	}
	var canonicalWarID string
	err := tx.QueryRow(ctx, `
		SELECT war_id
		FROM war_schedule
		WHERE schedule_key = $1
		FOR UPDATE
	`, ingest.FinishedScheduleKey).Scan(&canonicalWarID)
	if errors.Is(err, pgx.ErrNoRows) {
		// Another timer/process already stored and cleared this schedule.
		ingest.FinishedWarID = ""
		ingest.IndexRows = nil
		ingest.AttackRows = nil
		ingest.Players = nil
		return nil
	}
	if err != nil {
		return err
	}
	rewriteWarIngestID(ingest, canonicalWarID)
	return nil
}

func (s *timescaleWarStore) storeFinishedWarObject(ctx context.Context, ingest models.WarIngest) error {
	if s.objects == nil {
		return nil
	}
	// R2 stores the full API payload; SQL keeps lookup and analytics rows.
	compressed := utils.Compress(ingest.RawWarJSON)
	return s.objects.PutObject(ctx, warR2Key(ingest.FinishedWarID), compressed, "application/json")
}

func rewriteWarIngestID(ingest *models.WarIngest, warID string) {
	ingest.FinishedWarID = warID
	for i := range ingest.IndexRows {
		ingest.IndexRows[i].WarID = warID
	}
	for i := range ingest.AttackRows {
		ingest.AttackRows[i].WarID = warID
	}
}

func insertWarIndexRows(ctx context.Context, tx pgx.Tx, rows []models.WarLogIndexRow) error {
	if len(rows) == 0 {
		return nil
	}
	// This pgx batch only groups rows from the current ingest. It does not delay other wars.
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.WarID == "" || row.ClanTag == "" || row.OpponentTag == "" || row.PrepTime.IsZero() || row.EndTime.IsZero() {
			continue
		}
		batch.Queue(`
			INSERT INTO wars (
				war_id, clan_tag, opponent_tag, prep_time, start_time, end_time,
				size, war_type, state,
				battle_modifier, war_tag
			)
			VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, ''))
			ON CONFLICT (war_id, clan_tag) DO UPDATE SET
				opponent_tag = EXCLUDED.opponent_tag,
				prep_time = EXCLUDED.prep_time,
				start_time = EXCLUDED.start_time,
				end_time = EXCLUDED.end_time,
				size = EXCLUDED.size,
				war_type = EXCLUDED.war_type,
				state = EXCLUDED.state,
				battle_modifier = EXCLUDED.battle_modifier,
				war_tag = EXCLUDED.war_tag
			WHERE
				wars.opponent_tag IS DISTINCT FROM EXCLUDED.opponent_tag OR
				wars.prep_time IS DISTINCT FROM EXCLUDED.prep_time OR
				wars.start_time IS DISTINCT FROM EXCLUDED.start_time OR
				wars.end_time IS DISTINCT FROM EXCLUDED.end_time OR
				wars.size IS DISTINCT FROM EXCLUDED.size OR
				wars.war_type IS DISTINCT FROM EXCLUDED.war_type OR
				wars.state IS DISTINCT FROM EXCLUDED.state OR
				wars.battle_modifier IS DISTINCT FROM EXCLUDED.battle_modifier OR
				wars.war_tag IS DISTINCT FROM EXCLUDED.war_tag
		`, row.WarID, row.ClanTag, row.OpponentTag, row.PrepTime, row.StartTime, row.EndTime,
			row.Size, row.WarType, row.State, row.BattleModifier, row.WarTag)
	}
	return utils.SendBatch(ctx, tx, batch)
}

func insertWarAttackRows(ctx context.Context, tx pgx.Tx, rows []models.WarAttackRow) error {
	if len(rows) == 0 {
		return nil
	}
	// Attack rows are idempotent so retrying an end-time fetch can safely refresh analytics.
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.WarID == "" || row.AttackerTag == "" || row.DefenderTag == "" || row.WarEndTime.IsZero() {
			continue
		}
		batch.Queue(`
			INSERT INTO war_attacks (
				war_id, war_end_time, war_type, war_size, attacking_clan_tag, defending_clan_tag,
				attacker_tag, defender_tag, defender_name, attacker_townhall, defender_townhall,
				attacker_map_position, defender_map_position, stars, destruction_percentage,
				duration, attack_order, battle_modifier
			)
			VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
			ON CONFLICT (war_id, war_end_time, attacker_tag, defender_tag, attack_order) DO UPDATE SET
				war_type = EXCLUDED.war_type,
				war_size = EXCLUDED.war_size,
				attacking_clan_tag = EXCLUDED.attacking_clan_tag,
				defending_clan_tag = EXCLUDED.defending_clan_tag,
				defender_name = EXCLUDED.defender_name,
				attacker_townhall = EXCLUDED.attacker_townhall,
				defender_townhall = EXCLUDED.defender_townhall,
				attacker_map_position = EXCLUDED.attacker_map_position,
				defender_map_position = EXCLUDED.defender_map_position,
				stars = EXCLUDED.stars,
				destruction_percentage = EXCLUDED.destruction_percentage,
				duration = EXCLUDED.duration,
				battle_modifier = EXCLUDED.battle_modifier
		`, row.WarID, row.WarEndTime, row.WarType, row.WarSize, row.AttackingClanTag, row.DefendingClanTag,
			row.AttackerTag, row.DefenderTag, row.DefenderName, row.AttackerTownHall, row.DefenderTownHall,
			row.AttackerMapPosition, row.DefenderMapPosition, row.Stars, row.DestructionPercentage,
			row.Duration, row.AttackOrder, row.BattleModifier)
	}
	return utils.SendBatch(ctx, tx, batch)
}

func touchWarAttackPlayers(ctx context.Context, tx pgx.Tx, rows []models.WarAttackRow) error {
	if len(rows) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(rows))
	tags := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.AttackerTag == "" {
			continue
		}
		if _, ok := seen[row.AttackerTag]; ok {
			continue
		}
		seen[row.AttackerTag] = struct{}{}
		tags = append(tags, row.AttackerTag)
	}
	if len(tags) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `
		UPDATE basic_player
		SET battlelogs_tracking_ttl = now()
		WHERE tag = ANY($1)
	`, tags)
	return err
}

func upsertWarSchedules(ctx context.Context, tx pgx.Tx, rows []models.WarScheduleRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.ScheduleKey == "" || row.WarID == "" || row.SourceClanTag == "" || row.OpponentTag == "" || row.PrepTime.IsZero() || row.EndTime.IsZero() || row.NextRunAt.IsZero() {
			continue
		}
		batch.Queue(`
			INSERT INTO war_schedule (
				schedule_key, war_id, source_clan_tag, opponent_tag, prep_time,
				end_time, next_run_at, war_tag
			)
			VALUES ($1, $2::uuid, $3, $4, $5, $6, $7, NULLIF($8, ''))
			ON CONFLICT (schedule_key) DO UPDATE SET
				source_clan_tag = EXCLUDED.source_clan_tag,
				opponent_tag = EXCLUDED.opponent_tag,
				prep_time = EXCLUDED.prep_time,
				end_time = EXCLUDED.end_time,
				next_run_at = EXCLUDED.next_run_at,
				war_tag = EXCLUDED.war_tag
		`, row.ScheduleKey, row.WarID, row.SourceClanTag, row.OpponentTag, row.PrepTime, row.EndTime, row.NextRunAt, row.WarTag)
	}
	return utils.SendBatch(ctx, tx, batch)
}

func upsertCWLGroups(ctx context.Context, tx pgx.Tx, rows []models.CWLGroupRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.CWLID == "" || row.Season == "" || row.CWLLeagueID == 0 {
			continue
		}
		raw, err := json.Marshal(row.Data)
		if err != nil {
			return err
		}
		rounds, err := json.Marshal(row.Rounds)
		if err != nil {
			return err
		}
		batch.Queue(`
			INSERT INTO cwl_groups (
				cwl_id, season, cwl_league_id, clan_tags, rounds, data
			)
			VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb)
			ON CONFLICT (cwl_id) DO UPDATE SET
				season = EXCLUDED.season,
				cwl_league_id = EXCLUDED.cwl_league_id,
				clan_tags = EXCLUDED.clan_tags,
				rounds = EXCLUDED.rounds,
				data = EXCLUDED.data,
				updated_at = now()
			WHERE
				cwl_groups.season IS DISTINCT FROM EXCLUDED.season OR
				cwl_groups.cwl_league_id IS DISTINCT FROM EXCLUDED.cwl_league_id OR
				cwl_groups.clan_tags IS DISTINCT FROM EXCLUDED.clan_tags OR
				cwl_groups.rounds IS DISTINCT FROM EXCLUDED.rounds OR
				cwl_groups.data IS DISTINCT FROM EXCLUDED.data
		`, row.CWLID, row.Season, row.CWLLeagueID, row.ClanTags, string(rounds), string(raw))
	}
	return utils.SendBatch(ctx, tx, batch)
}

func deleteWarSchedule(ctx context.Context, tx pgx.Tx, scheduleKey string) error {
	if scheduleKey == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `DELETE FROM war_schedule WHERE schedule_key = $1`, scheduleKey)
	return err
}

func (s *timescaleWarStore) ShiftMaintenance(ctx context.Context, duration time.Duration) error {
	// Maintenance pauses war timers in-game, so pending end-time jobs move by observed
	// downtime. Stored wars are left untouched.
	_, err := s.pool.Exec(ctx, `
		UPDATE war_schedule
		SET next_run_at = next_run_at + ($1 * interval '1 second'),
		    end_time = end_time + ($1 * interval '1 second')
	`, int(duration.Seconds()))
	return err
}

type memoryWarStore struct {
	mu        sync.Mutex
	targets   []models.BasicClanRow
	indexRows map[string]models.WarLogIndexRow
	attacks   map[string]models.WarAttackRow
	schedules map[string]models.WarScheduleRow
	cwlGroups map[string]models.CWLGroupRow
	objects   map[string][]byte
}

func newMemoryWarStore() *memoryWarStore {
	return &memoryWarStore{
		indexRows: make(map[string]models.WarLogIndexRow),
		attacks:   make(map[string]models.WarAttackRow),
		schedules: make(map[string]models.WarScheduleRow),
		cwlGroups: make(map[string]models.CWLGroupRow),
		objects:   make(map[string][]byte),
	}
}

func (s *memoryWarStore) Close() error { return nil }

func (s *memoryWarStore) LoadPendingSchedules(context.Context) ([]models.WarScheduleRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]models.WarScheduleRow, 0, len(s.schedules))
	for _, schedule := range s.schedules {
		out = append(out, schedule)
	}
	return out, nil
}

func (s *memoryWarStore) LoadCWLLeague(_ context.Context, tag string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, target := range s.targets {
		if target.Tag == tag {
			return target.CWLLeagueID, nil
		}
	}
	return 0, sql.ErrNoRows
}

func (s *memoryWarStore) Store(_ context.Context, ingest models.WarIngest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ingest.FinishedWarID != "" && ingest.FinishedScheduleKey != "" {
		if schedule, ok := s.schedules[ingest.FinishedScheduleKey]; ok {
			rewriteWarIngestID(&ingest, schedule.WarID)
		} else {
			return nil
		}
	}
	for _, row := range ingest.IndexRows {
		s.indexRows[row.WarID+"|"+row.ClanTag] = row
	}
	for _, row := range ingest.AttackRows {
		key := row.WarID + "|" + row.WarEndTime.Format(time.RFC3339Nano) + "|" + row.AttackerTag + "|" + row.DefenderTag
		s.attacks[key] = row
	}
	for _, row := range ingest.Schedules {
		s.schedules[row.ScheduleKey] = row
	}
	for _, row := range ingest.CWLGroups {
		s.cwlGroups[row.CWLID] = row
	}
	if ingest.FinishedWarID != "" {
		// Mirror the production ordering closely enough for store-ordering unit tests.
		s.objects[warR2Key(ingest.FinishedWarID)] = utils.Compress(ingest.RawWarJSON)
		delete(s.schedules, ingest.FinishedScheduleKey)
	}
	return nil
}

func (s *memoryWarStore) ShiftMaintenance(_ context.Context, duration time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, schedule := range s.schedules {
		schedule.NextRunAt = schedule.NextRunAt.Add(duration)
		schedule.EndTime = schedule.EndTime.Add(duration)
		s.schedules[id] = schedule
	}
	return nil
}
