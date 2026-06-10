package scripts

import (
	"context"
	"database/sql"
	"encoding/json"
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
		SELECT war_id, source_clan_tag, opponent_tag, prep_time, end_time, next_run_at,
		       COALESCE(cwl_war_tag, ''), status, attempts, COALESCE(last_error, '')
		FROM war_schedule
		WHERE status IN ('pending', 'storing')
		ORDER BY next_run_at
	`)
	if err != nil {
		return nil, warStoreError("load schedules", err)
	}
	defer rows.Close()
	var out []models.WarScheduleRow
	for rows.Next() {
		var row models.WarScheduleRow
		if err := rows.Scan(&row.WarID, &row.SourceClanTag, &row.OpponentTag, &row.PrepTime, &row.EndTime, &row.NextRunAt, &row.CWLWarTag, &row.Status, &row.Attempts, &row.LastError); err != nil {
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
	// Write all durable SQL rows first. For finished wars, R2 and completion markers happen
	// after this commit so a failed object upload leaves the schedule retryable.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := utils.UpsertBasicPlayers(ctx, tx, ingest.Players, warsDomainName); err != nil {
		return err
	}
	if err := insertWarIndexRows(ctx, tx, ingest.IndexRows); err != nil {
		return err
	}
	if err := insertWarAttackRows(ctx, tx, ingest.AttackRows); err != nil {
		return err
	}
	if err := upsertWarSchedules(ctx, tx, ingest.Schedules); err != nil {
		return err
	}
	if err := upsertCWLGroups(ctx, tx, ingest.CWLGroups); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if ingest.FinishedWarID == "" {
		return nil
	}
	if err := s.storeFinishedWarObject(ctx, ingest); err != nil {
		return err
	}
	// Marking complete is intentionally last: SQL rows and R2 must both be present first.
	tx, err = s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := markWarStored(ctx, tx, ingest.FinishedWarID, ingest.R2Key); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *timescaleWarStore) storeFinishedWarObject(ctx context.Context, ingest models.WarIngest) error {
	if s.objects == nil {
		return nil
	}
	// R2 stores the full API payload; SQL keeps lookup and analytics rows.
	compressed := utils.Compress(ingest.RawWarJSON)
	return s.objects.PutObject(ctx, ingest.R2Key, compressed, "application/json")
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
			INSERT INTO war_log_index (
				war_id, clan_tag, opponent_tag, prep_time, start_time, end_time,
				clan_badge_url, opponent_badge_url, size, war_type, state,
				battle_modifier, cwl_war_tag
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULLIF($13, ''))
			ON CONFLICT (war_id, clan_tag) DO UPDATE SET
				opponent_tag = EXCLUDED.opponent_tag,
				prep_time = EXCLUDED.prep_time,
				start_time = EXCLUDED.start_time,
				end_time = EXCLUDED.end_time,
				clan_badge_url = EXCLUDED.clan_badge_url,
				opponent_badge_url = EXCLUDED.opponent_badge_url,
				size = EXCLUDED.size,
				war_type = EXCLUDED.war_type,
				state = EXCLUDED.state,
				battle_modifier = EXCLUDED.battle_modifier,
				cwl_war_tag = EXCLUDED.cwl_war_tag
			WHERE
				war_log_index.opponent_tag IS DISTINCT FROM EXCLUDED.opponent_tag OR
				war_log_index.prep_time IS DISTINCT FROM EXCLUDED.prep_time OR
				war_log_index.start_time IS DISTINCT FROM EXCLUDED.start_time OR
				war_log_index.end_time IS DISTINCT FROM EXCLUDED.end_time OR
				war_log_index.clan_badge_url IS DISTINCT FROM EXCLUDED.clan_badge_url OR
				war_log_index.opponent_badge_url IS DISTINCT FROM EXCLUDED.opponent_badge_url OR
				war_log_index.size IS DISTINCT FROM EXCLUDED.size OR
				war_log_index.war_type IS DISTINCT FROM EXCLUDED.war_type OR
				war_log_index.state IS DISTINCT FROM EXCLUDED.state OR
				war_log_index.battle_modifier IS DISTINCT FROM EXCLUDED.battle_modifier OR
				war_log_index.cwl_war_tag IS DISTINCT FROM EXCLUDED.cwl_war_tag
		`, row.WarID, row.ClanTag, row.OpponentTag, row.PrepTime, row.StartTime, row.EndTime,
			row.ClanBadgeURL, row.OpponentBadgeURL, row.Size, row.WarType, row.State, row.BattleModifier, row.CWLWarTag)
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
			INSERT INTO war_attack_events (
				war_id, war_end_time, war_type, war_size, attacking_clan_tag, defending_clan_tag,
				attacker_tag, defender_tag, attacker_townhall, defender_townhall,
				attacker_map_position, defender_map_position, stars, destruction_percentage,
				duration, attack_order, battle_modifier
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
			ON CONFLICT (war_id, war_end_time, attacker_tag, defender_tag, attack_order) DO UPDATE SET
				war_type = EXCLUDED.war_type,
				war_size = EXCLUDED.war_size,
				attacking_clan_tag = EXCLUDED.attacking_clan_tag,
				defending_clan_tag = EXCLUDED.defending_clan_tag,
				attacker_townhall = EXCLUDED.attacker_townhall,
				defender_townhall = EXCLUDED.defender_townhall,
				attacker_map_position = EXCLUDED.attacker_map_position,
				defender_map_position = EXCLUDED.defender_map_position,
				stars = EXCLUDED.stars,
				destruction_percentage = EXCLUDED.destruction_percentage,
				duration = EXCLUDED.duration,
				battle_modifier = EXCLUDED.battle_modifier
		`, row.WarID, row.WarEndTime, row.WarType, row.WarSize, row.AttackingClanTag, row.DefendingClanTag,
			row.AttackerTag, row.DefenderTag, row.AttackerTownHall, row.DefenderTownHall,
			row.AttackerMapPosition, row.DefenderMapPosition, row.Stars, row.DestructionPercentage,
			row.Duration, row.AttackOrder, row.BattleModifier)
	}
	return utils.SendBatch(ctx, tx, batch)
}

func upsertWarSchedules(ctx context.Context, tx pgx.Tx, rows []models.WarScheduleRow) error {
	if len(rows) == 0 {
		return nil
	}
	// Completed schedules are terminal; a stale active-war ingest should not reopen them.
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.WarID == "" || row.SourceClanTag == "" || row.OpponentTag == "" || row.PrepTime.IsZero() || row.EndTime.IsZero() || row.NextRunAt.IsZero() {
			continue
		}
		batch.Queue(`
			INSERT INTO war_schedule (
				war_id, source_clan_tag, opponent_tag, prep_time, end_time, next_run_at,
				cwl_war_tag, status
			)
			VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8)
			ON CONFLICT (war_id) DO UPDATE SET
				source_clan_tag = EXCLUDED.source_clan_tag,
				opponent_tag = EXCLUDED.opponent_tag,
				prep_time = EXCLUDED.prep_time,
				end_time = EXCLUDED.end_time,
				next_run_at = EXCLUDED.next_run_at,
				cwl_war_tag = EXCLUDED.cwl_war_tag,
				status = EXCLUDED.status,
				updated_at = now()
			WHERE war_schedule.status <> 'complete'
		`, row.WarID, row.SourceClanTag, row.OpponentTag, row.PrepTime, row.EndTime, row.NextRunAt, row.CWLWarTag, warSchedulePending)
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

func markWarStored(ctx context.Context, tx pgx.Tx, warID, key string) error {
	// The same R2 key is written to both perspective rows for lookup by either clan.
	_, err := tx.Exec(ctx, `
		UPDATE war_log_index
		SET r2_key = $2, stored_at = now()
		WHERE war_id = $1
	`, warID, key)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		UPDATE war_schedule
		SET status = $2, updated_at = now(), last_error = NULL
		WHERE war_id = $1
	`, warID, warScheduleComplete)
	return err
}

func (s *timescaleWarStore) ShiftMaintenance(ctx context.Context, duration time.Duration) error {
	// Maintenance pauses war timers in-game, so pending end-time jobs move by observed
	// downtime. Stored wars are left untouched.
	_, err := s.pool.Exec(ctx, `
		UPDATE war_schedule
		SET next_run_at = next_run_at + ($1 * interval '1 second'),
		    end_time = end_time + ($1 * interval '1 second'),
		    updated_at = now()
		WHERE status IN ('pending', 'storing')
	`, int(duration.Seconds()))
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE war_log_index
		SET end_time = end_time + ($1 * interval '1 second')
		WHERE stored_at IS NULL
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
		if schedule.Status == warSchedulePending || schedule.Status == "storing" {
			out = append(out, schedule)
		}
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
	for _, row := range ingest.IndexRows {
		s.indexRows[row.WarID+"|"+row.ClanTag] = row
	}
	for _, row := range ingest.AttackRows {
		key := row.WarID + "|" + row.WarEndTime.Format(time.RFC3339Nano) + "|" + row.AttackerTag + "|" + row.DefenderTag
		s.attacks[key] = row
	}
	for _, row := range ingest.Schedules {
		s.schedules[row.WarID] = row
	}
	for _, row := range ingest.CWLGroups {
		s.cwlGroups[row.CWLID] = row
	}
	if ingest.FinishedWarID != "" {
		// Mirror the production ordering closely enough for store-ordering unit tests.
		s.objects[ingest.R2Key] = utils.Compress(ingest.RawWarJSON)
		for key, row := range s.indexRows {
			if row.WarID == ingest.FinishedWarID {
				now := time.Now().UTC()
				row.R2Key = ingest.R2Key
				row.StoredAt = &now
				s.indexRows[key] = row
			}
		}
		if schedule, ok := s.schedules[ingest.FinishedWarID]; ok {
			schedule.Status = warScheduleComplete
			s.schedules[ingest.FinishedWarID] = schedule
		}
	}
	return nil
}

func (s *memoryWarStore) ShiftMaintenance(_ context.Context, duration time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, schedule := range s.schedules {
		if schedule.Status != warSchedulePending && schedule.Status != "storing" {
			continue
		}
		schedule.NextRunAt = schedule.NextRunAt.Add(duration)
		schedule.EndTime = schedule.EndTime.Add(duration)
		s.schedules[id] = schedule
	}
	for key, row := range s.indexRows {
		if row.StoredAt == nil {
			row.EndTime = row.EndTime.Add(duration)
			s.indexRows[key] = row
		}
	}
	return nil
}
