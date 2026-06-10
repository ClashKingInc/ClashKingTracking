package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel/attribute"
)

const (
	botPlayersDomainName = "botplayers"
	botPlayerCursorKey   = "botplayers:cursor:targets"
)

func playerSnapshotKey(tag string) string {
	return "ps:" + tag
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type botPlayersDomain struct {
	valkey valkey.Client
	store  botPlayerStore
}

type botPlayerStore interface {
	Close()
	NextTargetBatch(context.Context, int) (botPlayerTargetBatch, error)
	CommitTargetBatch(context.Context, botPlayerTargetBatch) error
	StoreIngest(context.Context, models.BotPlayerIngest) error
}

type botPlayerTargetBatch struct {
	Targets []models.BotPlayerTarget
	Cursor  string
}

func NewBotPlayersDomain() platform.Domain { return &botPlayersDomain{} }

func (d *botPlayersDomain) Name() string { return botPlayersDomainName }

func (d *botPlayersDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateBotPlayersConfig(app.Config); err != nil {
		return err
	}
	store, err := newBotPlayerStore(ctx, app)
	if err != nil {
		return err
	}
	defer store.Close()
	d.valkey = app.Valkey
	d.store = store

	limiter := platform.NewRequestLimiter(app.Config.BotPlayerRequestsPerSecond)
	targetPageSize := app.Config.BotPlayerRequestsPerSecond * app.Config.TargetPageMultiplier
	for {
		start := time.Now()
		err = d.runCycle(ctx, app, limiter, targetPageSize)
		app.Stats.RecordProcess(botPlayersDomainName, time.Since(start))
		if err != nil {
			if app.Config.RunOnce {
				return err
			}
			app.Stats.SetReady(botPlayersDomainName, false, err.Error())
		}
		if app.Config.RunOnce {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func validateBotPlayersConfig(cfg platform.Config) error {
	if cfg.BotPlayerRequestsPerSecond <= 0 {
		return errors.New("botplayers.requests_per_second must be greater than zero")
	}
	if cfg.TargetPageMultiplier <= 0 {
		return errors.New("target_page_multiplier must be greater than zero")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required for botplayers")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.ValkeyAddr == "" {
		return errors.New("valkey_addr is required for botplayers snapshots and cursors")
	}
	return nil
}

func newBotPlayerStore(ctx context.Context, app *platform.App) (botPlayerStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return newMemoryBotPlayerStore(), nil
	}
	return newTimescaleBotPlayerStore(ctx, app.Config.TimescaleURL, app.Valkey)
}

func (d *botPlayersDomain) runCycle(
	ctx context.Context,
	app *platform.App,
	limiter *platform.RequestLimiter,
	targetPageSize int,
) error {
	batch, err := d.store.NextTargetBatch(ctx, targetPageSize)
	if err != nil {
		return err
	}
	if len(batch.Targets) == 0 {
		if app.Config.RunOnce {
			return nil
		}
		return sleepOrDone(ctx, time.Second)
	}
	if err := d.processTargets(ctx, app, limiter, batch.Targets); err != nil {
		return err
	}
	return d.store.CommitTargetBatch(ctx, batch)
}

func (d *botPlayersDomain) processTargets(
	ctx context.Context,
	app *platform.App,
	limiter *platform.RequestLimiter,
	targets []models.BotPlayerTarget,
) error {
	ctx, span := platform.StartSpan(ctx, "tracker.group",
		attribute.String("domain", botPlayersDomainName),
		attribute.String("group", "players"),
		attribute.Int("target.count", len(targets)),
	)
	defer span.End()

	var wg sync.WaitGroup
	errCh := make(chan error, len(targets))
	for _, target := range targets {
		target := target
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.processTarget(ctx, app, limiter, target); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			platform.RecordSpanError(span, err)
			span.SetAttributes(platform.SpanErrorStatus(err))
			return err
		}
	}
	span.SetAttributes(platform.SpanErrorStatus(nil))
	return nil
}

func (d *botPlayersDomain) processTarget(
	ctx context.Context,
	app *platform.App,
	limiter *platform.RequestLimiter,
	target models.BotPlayerTarget,
) error {
	if target.Tag == "" {
		return nil
	}
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return err
	}
	start := time.Now()
	player, err := app.Clash.GetPlayer(ctx, target.Tag)
	release()
	app.Stats.RecordRequest(botPlayersDomainName, time.Since(start), err)
	if err != nil || player == nil {
		return nil
	}
	ingest, err := d.doPlayer(ctx, target.Tag, *player)
	if err != nil {
		return err
	}
	if len(ingest.ProfileChanges) == 0 && len(ingest.SeasonStats) == 0 {
		return d.savePlayerSnapshot(ctx, ingest.SnapshotTag, ingest.SnapshotRaw)
	}
	if err := d.store.StoreIngest(ctx, ingest); err != nil {
		return err
	}
	app.Stats.RecordWrite(botPlayersDomainName,
		len(ingest.Players)+len(ingest.ProfileChanges)+len(ingest.SeasonStats),
	)
	app.Stats.SetReady(botPlayersDomainName, true, "")
	if err := app.PublishEvent(ctx, platform.Event{
		Topic:   ingest.Event.Topic,
		ClanTag: ingest.Event.Key,
		Value:   ingest.Event.Value,
	}); err != nil {
		return err
	}
	return d.savePlayerSnapshot(ctx, ingest.SnapshotTag, ingest.SnapshotRaw)
}

func (d *botPlayersDomain) doPlayer(
	ctx context.Context,
	tag string,
	player clashy.Player,
) (models.BotPlayerIngest, error) {
	raw, err := json.Marshal(player)
	if err != nil {
		return models.BotPlayerIngest{}, err
	}
	current := playerMap(player)
	previousRaw, err := d.loadPlayerSnapshot(ctx, tag)
	if err != nil {
		return models.BotPlayerIngest{}, err
	}
	if len(previousRaw) == 0 || equalBytes(previousRaw, raw) {
		return models.BotPlayerIngest{SnapshotTag: tag, SnapshotRaw: raw}, nil
	}
	var previousPlayer clashy.Player
	if err := json.Unmarshal(previousRaw, &previousPlayer); err != nil {
		return models.BotPlayerIngest{SnapshotTag: tag, SnapshotRaw: raw}, nil
	}
	previous := playerMap(previousPlayer)
	if equalJSON(previous, current) {
		return models.BotPlayerIngest{SnapshotTag: tag, SnapshotRaw: raw}, nil
	}
	changes, activityScore, stats := playerChanges(tag, previous, current)
	clan := clanTag(current)
	now := time.Now().UTC()
	var lastOnline *time.Time
	if activityScore > 0 {
		lastOnline = &now
		for i := range stats {
			stats[i].LastOnlineAt = lastOnline
		}
	}
	return models.BotPlayerIngest{
		Players: []models.BasicPlayerRow{{
			Tag:      player.Tag,
			Name:     player.Name,
			LeagueID: player.LeagueTier.ID,
			TownHall: player.TownHall,
		}},
		ProfileChanges: changes,
		SeasonStats:    stats,
		LastOnlineAt:   lastOnline,
		Event: models.Event{
			Topic: "player",
			Key:   clan,
			Type:  "player_update",
			Value: map[string]any{
				"tag":           tag,
				"changed_types": playerChangeTypes(changes),
				"new_player":    current,
				"old_player":    previous,
			},
			CreatedAt: now,
		},
		SnapshotTag: tag,
		SnapshotRaw: raw,
	}, nil
}

func (d *botPlayersDomain) loadPlayerSnapshot(ctx context.Context, tag string) ([]byte, error) {
	if d.valkey == nil {
		return nil, nil
	}
	ctx, span := platform.StartSpan(ctx, "valkey.snapshot.get",
		attribute.String("domain", botPlayersDomainName),
		attribute.String("operation", "get"),
	)
	defer span.End()
	value, err := d.valkey.Do(ctx, d.valkey.B().Get().Key(playerSnapshotKey(tag)).Build()).ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			span.SetAttributes(platform.SpanErrorStatus(nil))
			return nil, nil
		}
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return nil, err
	}
	raw, err := utils.Decompress([]byte(value))
	platform.RecordSpanError(span, err)
	span.SetAttributes(platform.SpanErrorStatus(err))
	return raw, err
}

func (d *botPlayersDomain) savePlayerSnapshot(ctx context.Context, tag string, raw []byte) error {
	if d.valkey == nil || tag == "" || len(raw) == 0 {
		return nil
	}
	ctx, span := platform.StartSpan(ctx, "valkey.snapshot.set",
		attribute.String("domain", botPlayersDomainName),
		attribute.String("operation", "set"),
	)
	defer span.End()
	err := d.valkey.Do(ctx, d.valkey.B().Set().
		Key(playerSnapshotKey(tag)).
		Value(valkey.BinaryString(utils.Compress(raw))).
		Build(),
	).Error()
	platform.RecordSpanError(span, err)
	span.SetAttributes(platform.SpanErrorStatus(err))
	return err
}

func playerMap(player clashy.Player) map[string]any {
	raw, _ := json.Marshal(player)
	var value map[string]any
	_ = json.Unmarshal(raw, &value)
	return value
}

func playerChanges(
	tag string,
	previous map[string]any,
	current map[string]any,
) ([]models.PlayerProfileChangeRow, int, []models.PlayerSeasonStatRow) {
	var profileChanges []models.PlayerProfileChangeRow
	statsByKey := map[string]models.PlayerSeasonStatRow{}
	activityScore := 0
	season := utils.CurrentSeason(time.Now())
	clan := clanTag(current)
	now := time.Now().UTC()
	townhall, _ := asInt(current["townHallLevel"])

	for key, currentValue := range current {
		previousValue, exists := previous[key]
		if exists && equalJSON(previousValue, currentValue) {
			continue
		}
		if isHistoricalField(key) {
			profileChanges = append(profileChanges, models.PlayerProfileChangeRow{
				EventTime:     now,
				PlayerTag:     tag,
				ClanTag:       clan,
				TownHallLevel: townhall,
				ChangeType:    key,
				PreviousValue: previousValue,
				CurrentValue:  currentValue,
			})
		}
		if isOnlineField(key) {
			activityScore++
		}
		if incKey, ok := seasonalIncField(key); ok {
			change := numericChange(previousValue, currentValue)
			if change > 0 && clan != "" {
				row := statsByKey[clan]
				row.PlayerTag = tag
				row.Season = season
				row.ClanTag = clan
				switch incKey {
				case "donated":
					row.Donated += change
				case "received":
					row.Received += change
				case "capital_gold_dono":
					row.CapitalGoldDonos += change
				}
				statsByKey[clan] = row
			}
		}
	}
	if activityScore > 0 && clan != "" {
		row := statsByKey[clan]
		row.PlayerTag = tag
		row.Season = season
		row.ClanTag = clan
		row.ActivityScore++
		statsByKey[clan] = row
	}
	out := make([]models.PlayerSeasonStatRow, 0, len(statsByKey))
	for _, row := range statsByKey {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ClanTag == out[j].ClanTag {
			return out[i].PlayerTag < out[j].PlayerTag
		}
		return out[i].ClanTag < out[j].ClanTag
	})
	return profileChanges, activityScore, out
}

func isHistoricalField(key string) bool {
	switch key {
	case "name", "troops", "heroes", "spells", "heroEquipment", "townHallLevel",
		"warStars", "warPreference", "bestBuilderBaseTrophies", "bestTrophies",
		"expLevel":
		return true
	default:
		return false
	}
}

func isOnlineField(key string) bool {
	switch key {
	case "donations", "attackWins", "warStars", "builderBaseTrophies",
		"warPreference", "name", "heroEquipment":
		return true
	default:
		return false
	}
}

func seasonalIncField(key string) (string, bool) {
	switch key {
	case "donations":
		return "donated", true
	case "donationsReceived":
		return "received", true
	case "clanCapitalContributions":
		return "capital_gold_dono", true
	default:
		return "", false
	}
}

func numericChange(previous, current any) int {
	prev, ok1 := asInt(previous)
	curr, ok2 := asInt(current)
	if !ok1 || !ok2 {
		return 0
	}
	return curr - prev
}

func asInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func equalJSON(a, b any) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}

func playerChangeTypes(changes []models.PlayerProfileChangeRow) []string {
	out := make([]string, 0, len(changes))
	for _, change := range changes {
		out = append(out, change.ChangeType)
	}
	sort.Strings(out)
	return out
}

func clanTag(player map[string]any) string {
	clan, _ := player["clan"].(map[string]any)
	value, _ := clan["tag"].(string)
	return value
}

type timescaleBotPlayerStore struct {
	pool   *pgxpool.Pool
	valkey valkey.Client
	cursor string
}

func newTimescaleBotPlayerStore(
	ctx context.Context,
	dsn string,
	client valkey.Client,
) (*timescaleBotPlayerStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleBotPlayerStore{pool: pool, valkey: client}, nil
}

func (s *timescaleBotPlayerStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *timescaleBotPlayerStore) NextTargetBatch(
	ctx context.Context,
	limit int,
) (botPlayerTargetBatch, error) {
	if limit <= 0 {
		return botPlayerTargetBatch{}, nil
	}
	cursor, err := s.getCursor(ctx)
	if err != nil {
		return botPlayerTargetBatch{}, err
	}
	rows, err := s.pool.Query(ctx, botPlayerTargetsSQL, cursor, limit+1)
	if err != nil {
		return botPlayerTargetBatch{}, err
	}
	defer rows.Close()
	var targets []models.BotPlayerTarget
	for rows.Next() {
		var target models.BotPlayerTarget
		if err := rows.Scan(&target.Tag); err != nil {
			return botPlayerTargetBatch{}, err
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return botPlayerTargetBatch{}, err
	}
	nextCursor := ""
	if len(targets) > limit {
		nextCursor = targets[limit-1].Tag
		targets = targets[:limit]
	}
	return botPlayerTargetBatch{Targets: targets, Cursor: nextCursor}, nil
}

func (s *timescaleBotPlayerStore) CommitTargetBatch(
	ctx context.Context,
	batch botPlayerTargetBatch,
) error {
	s.cursor = batch.Cursor
	if s.valkey == nil {
		return nil
	}
	cmd := cursorCommand(s.valkey, botPlayerCursorKey, batch.Cursor)
	return s.valkey.Do(ctx, cmd).Error()
}

func (s *timescaleBotPlayerStore) StoreIngest(
	ctx context.Context,
	ingest models.BotPlayerIngest,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := utils.UpsertBasicPlayers(ctx, tx, ingest.Players, botPlayersDomainName); err != nil {
		return err
	}
	if err := insertPlayerProfileChanges(ctx, tx, ingest.ProfileChanges); err != nil {
		return err
	}
	if err := upsertPlayerSeasonStats(ctx, tx, ingest.SeasonStats); err != nil {
		return err
	}
	if ingest.LastOnlineAt != nil && ingest.SnapshotTag != "" {
		if _, err := tx.Exec(ctx, updatePlayerLastActivitySQL, *ingest.LastOnlineAt, ingest.SnapshotTag); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (s *timescaleBotPlayerStore) getCursor(ctx context.Context) (string, error) {
	if s.valkey == nil {
		return s.cursor, nil
	}
	value, err := s.valkey.Do(ctx, s.valkey.B().Get().Key(botPlayerCursorKey).Build()).ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return "", nil
		}
		return "", err
	}
	return value, nil
}

const botPlayerTargetsSQL = `
	SELECT tag
	FROM (
		SELECT unnest(member_tags) AS tag
		FROM basic_clan
		WHERE cardinality(member_tags) > 0
		UNION
		SELECT tag
		FROM tracked_player_targets
		WHERE enabled = true
	) targets
	WHERE tag > $1
	ORDER BY tag
	LIMIT $2
`

const updatePlayerLastActivitySQL = `
	UPDATE basic_player
	SET last_activity = $1
	WHERE tag = $2
	  AND (last_activity IS NULL OR last_activity < $1)
`

func insertPlayerProfileChanges(
	ctx context.Context,
	tx pgx.Tx,
	rows []models.PlayerProfileChangeRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.PlayerTag == "" || row.ChangeType == "" {
			continue
		}
		previous, _ := json.Marshal(row.PreviousValue)
		current, _ := json.Marshal(row.CurrentValue)
		batch.Queue(`
			INSERT INTO player_profile_changes (
				event_time, player_tag, clan_tag, townhall_level, change_type,
				previous_value, current_value
			)
			VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb)
			ON CONFLICT DO NOTHING
		`, row.EventTime, row.PlayerTag, row.ClanTag, row.TownHallLevel, row.ChangeType,
			string(previous), string(current))
	}
	return utils.SendBatch(ctx, tx, batch)
}

func upsertPlayerSeasonStats(
	ctx context.Context,
	tx pgx.Tx,
	rows []models.PlayerSeasonStatRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.PlayerTag == "" || row.Season == "" {
			continue
		}
		batch.Queue(`
			INSERT INTO player_season_stats (
				player_tag, season, clan_tag, donated, received, capital_gold_donos,
				activity_score, last_online_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			ON CONFLICT (player_tag, season, clan_tag) DO UPDATE SET
				donated = player_season_stats.donated + EXCLUDED.donated,
				received = player_season_stats.received + EXCLUDED.received,
				capital_gold_donos =
					player_season_stats.capital_gold_donos + EXCLUDED.capital_gold_donos,
				activity_score =
					player_season_stats.activity_score + EXCLUDED.activity_score,
				last_online_at = GREATEST(
					player_season_stats.last_online_at,
					EXCLUDED.last_online_at
				),
				updated_at = now()
		`, row.PlayerTag, row.Season, row.ClanTag, row.Donated, row.Received,
			row.CapitalGoldDonos, row.ActivityScore, row.LastOnlineAt)
	}
	return utils.SendBatch(ctx, tx, batch)
}

type memoryBotPlayerStore struct {
	targets []models.BotPlayerTarget
	cursor  int
}

func newMemoryBotPlayerStore() *memoryBotPlayerStore {
	return &memoryBotPlayerStore{}
}

func (s *memoryBotPlayerStore) Close() {}

func (s *memoryBotPlayerStore) NextTargetBatch(
	_ context.Context,
	limit int,
) (botPlayerTargetBatch, error) {
	if limit <= 0 || len(s.targets) == 0 {
		return botPlayerTargetBatch{}, nil
	}
	if s.cursor >= len(s.targets) {
		s.cursor = 0
	}
	end := s.cursor + limit
	if end > len(s.targets) {
		end = len(s.targets)
	}
	targets := append([]models.BotPlayerTarget(nil), s.targets[s.cursor:end]...)
	cursor := ""
	if end < len(s.targets) {
		cursor = targets[len(targets)-1].Tag
	}
	return botPlayerTargetBatch{Targets: targets, Cursor: cursor}, nil
}

func (s *memoryBotPlayerStore) CommitTargetBatch(
	_ context.Context,
	batch botPlayerTargetBatch,
) error {
	if batch.Cursor == "" {
		s.cursor = 0
		return nil
	}
	for i, target := range s.targets {
		if target.Tag == batch.Cursor {
			s.cursor = i + 1
			return nil
		}
	}
	return nil
}

func (s *memoryBotPlayerStore) StoreIngest(context.Context, models.BotPlayerIngest) error {
	return nil
}

func sleepOrDone(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
