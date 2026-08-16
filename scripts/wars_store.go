package scripts

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type timescaleWarStore struct {
	pool *pgxpool.Pool
}

func newTimescaleWarStore(ctx context.Context, dsn string) (*timescaleWarStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleWarStore{pool: pool}, nil
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

const loadActiveCurrentWarTimerSQL = `
	SELECT player_tag, war_id, clan_tag, opponent_tag, end_time
	FROM current_war_timers
	WHERE player_tag = $1 AND end_time > now()
`

const deleteExpiredCurrentWarTimersSQL = `DELETE FROM current_war_timers WHERE end_time <= now()`

func (s *timescaleWarStore) LoadActiveCurrentWarTimer(ctx context.Context, playerTag string) (models.CurrentWarTimerRow, bool, error) {
	var row models.CurrentWarTimerRow
	err := s.pool.QueryRow(ctx, loadActiveCurrentWarTimerSQL, playerTag).Scan(&row.PlayerTag, &row.WarID, &row.ClanTag, &row.OpponentTag, &row.EndTime)
	if errors.Is(err, pgx.ErrNoRows) {
		return models.CurrentWarTimerRow{}, false, nil
	}
	if err != nil {
		return models.CurrentWarTimerRow{}, false, warStoreError("load active current war timer", err)
	}
	return row, true, nil
}

func (s *timescaleWarStore) DeleteExpiredCurrentWarTimers(ctx context.Context) (int, error) {
	result, err := s.pool.Exec(ctx, deleteExpiredCurrentWarTimersSQL)
	if err != nil {
		return 0, warStoreError("delete expired current war timers", err)
	}
	return int(result.RowsAffected()), nil
}

func (s *timescaleWarStore) Store(ctx context.Context, ingest models.WarIngest) error {
	if len(ingest.IndexRows) == 0 && len(ingest.AttackRows) == 0 && len(ingest.Schedules) == 0 && len(ingest.CurrentWarTimers) == 0 && len(ingest.CWLGroups) == 0 {
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
	if err := upsertCurrentWarTimers(ctx, tx, ingest.CurrentWarTimers); err != nil {
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
		return nil
	}
	if err != nil {
		return err
	}
	rewriteWarIngestID(ingest, canonicalWarID)
	return nil
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
				size, attacks_per_member, war_type, state,
				battle_modifier, war_tag, clan_name, opponent_name,
				clan_badge_token, opponent_badge_token, clan_level, opponent_clan_level,
				clan_attacks, opponent_attacks, clan_stars, opponent_stars,
				clan_destruction_percentage, opponent_destruction_percentage
			)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, ''),
				$13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
			)
			ON CONFLICT (war_id) DO UPDATE SET
				clan_tag = EXCLUDED.clan_tag,
				opponent_tag = EXCLUDED.opponent_tag,
				prep_time = EXCLUDED.prep_time,
				start_time = EXCLUDED.start_time,
				end_time = EXCLUDED.end_time,
				size = EXCLUDED.size,
				attacks_per_member = EXCLUDED.attacks_per_member,
				war_type = EXCLUDED.war_type,
				state = EXCLUDED.state,
				battle_modifier = EXCLUDED.battle_modifier,
				war_tag = EXCLUDED.war_tag,
				clan_name = EXCLUDED.clan_name,
				opponent_name = EXCLUDED.opponent_name,
				clan_badge_token = EXCLUDED.clan_badge_token,
				opponent_badge_token = EXCLUDED.opponent_badge_token,
				clan_level = EXCLUDED.clan_level,
				opponent_clan_level = EXCLUDED.opponent_clan_level,
				clan_attacks = EXCLUDED.clan_attacks,
				opponent_attacks = EXCLUDED.opponent_attacks,
				clan_stars = EXCLUDED.clan_stars,
				opponent_stars = EXCLUDED.opponent_stars,
				clan_destruction_percentage = EXCLUDED.clan_destruction_percentage,
				opponent_destruction_percentage = EXCLUDED.opponent_destruction_percentage
			WHERE
				wars.clan_tag IS DISTINCT FROM EXCLUDED.clan_tag OR
				wars.opponent_tag IS DISTINCT FROM EXCLUDED.opponent_tag OR
				wars.prep_time IS DISTINCT FROM EXCLUDED.prep_time OR
				wars.start_time IS DISTINCT FROM EXCLUDED.start_time OR
				wars.end_time IS DISTINCT FROM EXCLUDED.end_time OR
				wars.size IS DISTINCT FROM EXCLUDED.size OR
				wars.attacks_per_member IS DISTINCT FROM EXCLUDED.attacks_per_member OR
				wars.war_type IS DISTINCT FROM EXCLUDED.war_type OR
				wars.state IS DISTINCT FROM EXCLUDED.state OR
				wars.battle_modifier IS DISTINCT FROM EXCLUDED.battle_modifier OR
				wars.war_tag IS DISTINCT FROM EXCLUDED.war_tag OR
				wars.clan_name IS DISTINCT FROM EXCLUDED.clan_name OR
				wars.opponent_name IS DISTINCT FROM EXCLUDED.opponent_name OR
				wars.clan_badge_token IS DISTINCT FROM EXCLUDED.clan_badge_token OR
				wars.opponent_badge_token IS DISTINCT FROM EXCLUDED.opponent_badge_token OR
				wars.clan_level IS DISTINCT FROM EXCLUDED.clan_level OR
				wars.opponent_clan_level IS DISTINCT FROM EXCLUDED.opponent_clan_level OR
				wars.clan_attacks IS DISTINCT FROM EXCLUDED.clan_attacks OR
				wars.opponent_attacks IS DISTINCT FROM EXCLUDED.opponent_attacks OR
				wars.clan_stars IS DISTINCT FROM EXCLUDED.clan_stars OR
				wars.opponent_stars IS DISTINCT FROM EXCLUDED.opponent_stars OR
				wars.clan_destruction_percentage IS DISTINCT FROM EXCLUDED.clan_destruction_percentage OR
				wars.opponent_destruction_percentage IS DISTINCT FROM EXCLUDED.opponent_destruction_percentage
		`, row.WarID, row.ClanTag, row.OpponentTag, row.PrepTime, row.StartTime, row.EndTime,
			row.Size, row.AttacksPerMember, row.WarType, row.State, row.BattleModifier, row.WarTag,
			row.ClanName, row.OpponentName, row.ClanBadgeToken, row.OpponentBadgeToken,
			row.ClanLevel, row.OpponentClanLevel, row.ClanAttacks, row.OpponentAttacks,
			row.ClanStars, row.OpponentStars, row.ClanDestructionPercentage, row.OpponentDestructionPercentage)
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
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
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
			VALUES ($1, $2, $3, $4, $5, $6, $7, NULLIF($8, ''))
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

const upsertCurrentWarTimersSQL = `
	INSERT INTO current_war_timers (player_tag, war_id, clan_tag, opponent_tag, end_time)
	SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::timestamptz[])
	ON CONFLICT (player_tag) DO UPDATE SET
		war_id = EXCLUDED.war_id,
		clan_tag = EXCLUDED.clan_tag,
		opponent_tag = EXCLUDED.opponent_tag,
		end_time = EXCLUDED.end_time
`

func upsertCurrentWarTimers(ctx context.Context, tx pgx.Tx, rows []models.CurrentWarTimerRow) error {
	playerTags := make([]string, 0, len(rows))
	warIDs := make([]string, 0, len(rows))
	clanTags := make([]string, 0, len(rows))
	opponentTags := make([]string, 0, len(rows))
	endTimes := make([]time.Time, 0, len(rows))
	for _, row := range rows {
		if row.PlayerTag == "" || row.WarID == "" || row.ClanTag == "" || row.OpponentTag == "" || row.EndTime.IsZero() {
			continue
		}
		playerTags = append(playerTags, row.PlayerTag)
		warIDs = append(warIDs, row.WarID)
		clanTags = append(clanTags, row.ClanTag)
		opponentTags = append(opponentTags, row.OpponentTag)
		endTimes = append(endTimes, row.EndTime)
	}
	if len(playerTags) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, upsertCurrentWarTimersSQL, playerTags, warIDs, clanTags, opponentTags, endTimes)
	return err
}

const upsertCWLGroupsSQLShape = `
	INSERT INTO cwl_groups (
		cwl_id, season, cwl_league_id, state, war_size, rounds
	)
	VALUES ($1, $2, $3, $4, $5, $6::jsonb)
	ON CONFLICT (cwl_id) DO UPDATE SET
		season = EXCLUDED.season,
		cwl_league_id = COALESCE(EXCLUDED.cwl_league_id, cwl_groups.cwl_league_id),
		state = EXCLUDED.state,
		war_size = EXCLUDED.war_size,
		rounds = EXCLUDED.rounds
	WHERE
		cwl_groups.season IS DISTINCT FROM EXCLUDED.season OR
		cwl_groups.cwl_league_id IS DISTINCT FROM COALESCE(EXCLUDED.cwl_league_id, cwl_groups.cwl_league_id) OR
		cwl_groups.state IS DISTINCT FROM EXCLUDED.state OR
		cwl_groups.war_size IS DISTINCT FROM EXCLUDED.war_size OR
		cwl_groups.rounds IS DISTINCT FROM EXCLUDED.rounds
`

func upsertCWLGroups(ctx context.Context, tx pgx.Tx, rows []models.CWLGroupRow) error {
	if len(rows) == 0 {
		return nil
	}
	for _, row := range rows {
		if row.CWLID == "" || row.Season == "" {
			continue
		}
		rounds, err := json.Marshal(row.Rounds)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, upsertCWLGroupsSQLShape, row.CWLID, row.Season, row.CWLLeagueID, row.State, row.WarSize, string(rounds)); err != nil {
			return err
		}
		if err := upsertCWLGroupClans(ctx, tx, row.CWLID, row.Clans); err != nil {
			return err
		}
	}
	return nil
}

const upsertCWLGroupClansSQL = `
	INSERT INTO cwl_group_clans (cwl_id, clan_tag, name, clan_level, badge_token)
	SELECT $1, snapshot.clan_tag, snapshot.name, snapshot.clan_level, snapshot.badge_token
	FROM unnest($2::text[], $3::text[], $4::int[], $5::text[]) AS snapshot(clan_tag, name, clan_level, badge_token)
	ON CONFLICT (cwl_id, clan_tag) DO UPDATE SET
		name = EXCLUDED.name,
		clan_level = EXCLUDED.clan_level,
		badge_token = EXCLUDED.badge_token
`

const upsertCWLGroupMembersSQL = `
	INSERT INTO cwl_group_members (cwl_id, clan_tag, name, tag, town_hall)
	SELECT $1, $2, source.name, source.tag, source.town_hall::smallint
	FROM unnest($3::text[], $4::text[], $5::int[]) AS source(tag, name, town_hall)
	ON CONFLICT (cwl_id, tag) DO UPDATE SET
		clan_tag = EXCLUDED.clan_tag,
		name = EXCLUDED.name,
		town_hall = EXCLUDED.town_hall
`

const deleteStaleCWLGroupMembersSQL = `
	DELETE FROM cwl_group_members
	WHERE cwl_id = $1
	  AND clan_tag = $2
	  AND NOT (tag = ANY($3::text[]))
`

func upsertCWLGroupClans(ctx context.Context, tx pgx.Tx, cwlID string, rows []models.CWLGroupClanRow) error {
	if cwlID == "" || len(rows) == 0 {
		return nil
	}
	tags := make([]string, 0, len(rows))
	names := make([]string, 0, len(rows))
	levels := make([]int, 0, len(rows))
	badges := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.ClanTag == "" {
			continue
		}
		tags = append(tags, row.ClanTag)
		names = append(names, row.Name)
		levels = append(levels, row.ClanLevel)
		badges = append(badges, row.BadgeToken)
	}
	if len(tags) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, upsertCWLGroupClansSQL, cwlID, tags, names, levels, badges); err != nil {
		return err
	}
	for _, row := range rows {
		if row.ClanTag == "" {
			continue
		}
		if err := replaceCWLGroupMembers(ctx, tx, cwlID, row.ClanTag, row.Members); err != nil {
			return err
		}
	}
	return nil
}

func replaceCWLGroupMembers(ctx context.Context, tx pgx.Tx, cwlID, clanTag string, members []models.BasicClanMember) error {
	tags := make([]string, 0, len(members))
	names := make([]string, 0, len(members))
	townHalls := make([]int, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		if member.Tag == "" {
			continue
		}
		if _, exists := seen[member.Tag]; exists {
			continue
		}
		seen[member.Tag] = struct{}{}
		tags = append(tags, member.Tag)
		names = append(names, member.Name)
		townHalls = append(townHalls, member.TownHall)
	}
	if len(tags) > 0 {
		if _, err := tx.Exec(ctx, upsertCWLGroupMembersSQL, cwlID, clanTag, tags, names, townHalls); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, deleteStaleCWLGroupMembersSQL, cwlID, clanTag, tags)
	return err
}

func deleteWarSchedule(ctx context.Context, tx pgx.Tx, scheduleKey string) error {
	if scheduleKey == "" {
		return nil
	}
	_, err := tx.Exec(ctx, `DELETE FROM war_schedule WHERE schedule_key = $1`, scheduleKey)
	return err
}

func (s *timescaleWarStore) ShiftMaintenance(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, shiftActiveWarMaintenanceSQL, duration); err != nil {
		return warStoreError("shift active war maintenance", err)
	}
	return tx.Commit(ctx)
}

// The CTE returns exactly the active scheduled wars shifted in this transaction.
// Current timer rows move only when their war is in that set and are independently
// guarded by end_time, so stale rows cannot be revived by maintenance.
const shiftActiveWarMaintenanceSQL = `
	WITH shifted_wars AS (
		UPDATE war_schedule
		SET next_run_at = next_run_at + $1,
		    end_time = end_time + $1
		WHERE end_time > now()
		RETURNING war_id
	)
	UPDATE current_war_timers AS timer
	SET end_time = timer.end_time + $1
	WHERE timer.end_time > now()
	  AND timer.war_id IN (SELECT DISTINCT war_id FROM shifted_wars)
`

type memoryWarStore struct {
	mu               sync.Mutex
	targets          []models.BasicClanRow
	indexRows        map[string]models.WarLogIndexRow
	attacks          map[string]models.WarAttackRow
	schedules        map[string]models.WarScheduleRow
	cwlGroups        map[string]models.CWLGroupRow
	currentWarTimers map[string]models.CurrentWarTimerRow
}

func newMemoryWarStore() *memoryWarStore {
	return &memoryWarStore{
		indexRows:        make(map[string]models.WarLogIndexRow),
		attacks:          make(map[string]models.WarAttackRow),
		schedules:        make(map[string]models.WarScheduleRow),
		cwlGroups:        make(map[string]models.CWLGroupRow),
		currentWarTimers: make(map[string]models.CurrentWarTimerRow),
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

func (s *memoryWarStore) LoadActiveCurrentWarTimer(_ context.Context, playerTag string) (models.CurrentWarTimerRow, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.currentWarTimers[playerTag]
	if !ok || !row.EndTime.After(time.Now().UTC()) {
		return models.CurrentWarTimerRow{}, false, nil
	}
	return row, true, nil
}

func (s *memoryWarStore) DeleteExpiredCurrentWarTimers(_ context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	deleted := 0
	for playerTag, row := range s.currentWarTimers {
		if !row.EndTime.After(now) {
			delete(s.currentWarTimers, playerTag)
			deleted++
		}
	}
	return deleted, nil
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
	for _, row := range ingest.CurrentWarTimers {
		if row.PlayerTag != "" && row.WarID != "" && row.ClanTag != "" && row.OpponentTag != "" && !row.EndTime.IsZero() {
			s.currentWarTimers[row.PlayerTag] = row
		}
	}
	for _, row := range ingest.CWLGroups {
		s.cwlGroups[row.CWLID] = row
	}
	if ingest.FinishedWarID != "" {
		delete(s.schedules, ingest.FinishedScheduleKey)
	}
	return nil
}

func (s *memoryWarStore) ShiftMaintenance(_ context.Context, duration time.Duration) error {
	if duration <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	shiftedWarIDs := make(map[string]struct{})
	for id, schedule := range s.schedules {
		if !schedule.EndTime.After(now) {
			continue
		}
		schedule.NextRunAt = schedule.NextRunAt.Add(duration)
		schedule.EndTime = schedule.EndTime.Add(duration)
		s.schedules[id] = schedule
		shiftedWarIDs[schedule.WarID] = struct{}{}
	}
	for playerTag, timer := range s.currentWarTimers {
		if _, ok := shiftedWarIDs[timer.WarID]; ok && timer.EndTime.After(now) {
			timer.EndTime = timer.EndTime.Add(duration)
			s.currentWarTimers[playerTag] = timer
		}
	}
	return nil
}
