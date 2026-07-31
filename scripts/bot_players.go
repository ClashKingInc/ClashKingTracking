package scripts

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const (
	botPlayersDomainName = "botplayers"

	playerStatDonated            = "donated"
	playerStatReceived           = "received"
	playerStatClanGames          = "clan_games"
	playerStatCapitalGoldDonated = "capital_gold_donated"
	playerClanGamesAchievement   = "Games Champion"
)

func playerSnapshotKey(tag string) string {
	return "ps:" + tag
}

func playerStatPendingKey(tag string) string {
	return "ps:stat-pending:" + tag
}

var reservePlayerStatEventTimeScript = valkey.NewLuaScript(`
	local existing = redis.call('GET', KEYS[1])
	if existing then
		local separator = string.find(existing, '|', 1, true)
		if separator and string.sub(existing, 1, separator - 1) == ARGV[1] then
			return string.sub(existing, separator + 1)
		end
	end
	local value = ARGV[1] .. '|' .. ARGV[2]
	redis.call('SET', KEYS[1], value)
	return ARGV[2]
`)

var storePlayerSnapshotScript = valkey.NewLuaScript(`
	redis.call('SET', KEYS[1], ARGV[1])
	redis.call('DEL', KEYS[2])
	return 1
`)

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
	snapshots botPlayerSnapshotStore
	store     botPlayerStore
}

type botPlayerStore interface {
	Close()
	NextTargetPage(context.Context, string, int) (botPlayerTargetPage, error)
	StoreIngest(context.Context, models.BotPlayerIngest) error
}

type botPlayerTargetPage struct {
	Targets    []models.BotPlayerTarget
	NextCursor string
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
	d.snapshots = newBotPlayerSnapshotStore(app.Valkey)
	d.store = store

	limiter, err := newTrackingLimiter(app.Config.BotPlayerRequestsPerSecond)
	if err != nil {
		return err
	}
	pageSize := app.Config.BotPlayerRequestsPerSecond * app.Config.TargetPageMultiplier
	cursor := ""
	for {
		page, err := d.store.NextTargetPage(ctx, cursor, pageSize)
		if err != nil {
			return err
		}
		if len(page.Targets) == 0 {
			cursor = ""
			if err := sleepOrDone(ctx, time.Second); err != nil {
				return err
			}
			continue
		}
		start := time.Now()
		if err := runBounded(ctx, platform.RequestConcurrency(app.Config.BotPlayerRequestsPerSecond), page.Targets, func(workerCtx context.Context, target models.BotPlayerTarget) error {
			ingest, err := retryLimitedClashFetch(workerCtx, limiter, func(fetchCtx context.Context) (models.BotPlayerIngest, error) {
				return d.fetchAndPreparePlayer(fetchCtx, app, target)
			})
			if err != nil {
				app.Logger.Error("bot player processing failed", "tag", target.Tag, "err", err)
				app.Stats.SetReady(botPlayersDomainName, false, err.Error())
				return err
			}
			if err := d.storePlayerIngest(workerCtx, app, ingest); err != nil {
				return err
			}
			app.Stats.RecordTrackedTarget(botPlayersDomainName)
			return nil
		}); err != nil {
			return err
		}
		app.Stats.RecordProcess(botPlayersDomainName, time.Since(start))
		cursor = page.NextCursor
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
		return errors.New("valkey_addr is required for botplayers snapshots")
	}
	return nil
}

func newBotPlayerStore(ctx context.Context, app *platform.App) (botPlayerStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return newMemoryBotPlayerStore(), nil
	}
	return newTimescaleBotPlayerStore(ctx, app.Config.TimescaleURL)
}

func (d *botPlayersDomain) fetchAndPreparePlayer(
	ctx context.Context,
	app *platform.App,
	target models.BotPlayerTarget,
) (models.BotPlayerIngest, error) {
	if target.Tag == "" {
		return models.BotPlayerIngest{}, nil
	}
	start := time.Now()
	player, err := app.Clash.GetPlayer(ctx, target.Tag)
	app.Stats.RecordRequest(botPlayersDomainName, time.Since(start), err)
	if err != nil {
		if _, ok := platform.ClashFetchRetryPolicy(err); ok {
			return models.BotPlayerIngest{}, err
		}
		return models.BotPlayerIngest{}, nil
	}
	if player == nil {
		return models.BotPlayerIngest{}, nil
	}
	return d.doPlayer(ctx, target.Tag, *player)
}

func (d *botPlayersDomain) storePlayerIngest(ctx context.Context, app *platform.App, ingest models.BotPlayerIngest) error {
	if len(ingest.Players) == 0 && len(ingest.ProfileChanges) == 0 && len(ingest.StatChanges) == 0 {
		return d.savePlayerSnapshot(ctx, ingest.SnapshotTag, ingest.SnapshotRaw)
	}
	if err := d.store.StoreIngest(ctx, ingest); err != nil {
		return err
	}
	app.Stats.RecordWrite(botPlayersDomainName,
		len(ingest.Players)+len(ingest.ProfileChanges)+len(ingest.StatChanges),
	)
	app.Stats.SetReady(botPlayersDomainName, true, "")
	if ingest.Event.Topic != "" {
		if err := app.PublishEvent(ctx, platform.Event{
			Topic:   ingest.Event.Topic,
			ClanTag: ingest.Event.Key,
			Value:   ingest.Event.Value,
		}); err != nil {
			return err
		}
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
	if len(previousRaw) == 0 {
		return models.BotPlayerIngest{
			Players:     []models.BasicPlayerRow{botPlayerRow(player)},
			SnapshotTag: tag,
			SnapshotRaw: raw,
		}, nil
	}
	if equalBytes(previousRaw, raw) {
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
	now := time.Now().UTC()
	changes, activityScore := playerChanges(tag, previous, current, now)
	statChanges := playerStatChanges(tag, previousPlayer, player, now)
	if len(statChanges) > 0 {
		statEventTime, err := d.reservePlayerStatEventTime(ctx, tag, previousRaw, now)
		if err != nil {
			return models.BotPlayerIngest{}, err
		}
		for index := range statChanges {
			statChanges[index].EventTime = statEventTime
		}
	}
	clan := clanTag(current)
	var lastOnline *time.Time
	if activityScore > 0 {
		lastOnline = &now
	}
	return models.BotPlayerIngest{
		Players:        []models.BasicPlayerRow{botPlayerRow(player)},
		ProfileChanges: changes,
		StatChanges:    statChanges,
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

func botPlayerRow(player clashy.Player) models.BasicPlayerRow {
	clan := ""
	if player.Clan != nil {
		clan = player.Clan.Tag
	}
	return models.BasicPlayerRow{
		Tag:          player.Tag,
		Name:         player.Name,
		LeagueID:     player.LeagueTier.ID,
		ClanTag:      clan,
		ClanTagKnown: true,
		TownHall:     player.TownHall,
		Trophies:     player.Trophies,
	}
}

type botPlayerSnapshotStore interface {
	Load(context.Context, string) ([]byte, bool, error)
	ReserveStatEventTime(context.Context, string, string, time.Time) (time.Time, error)
	StoreAndClear(context.Context, string, string, []byte) error
}

type valkeyBotPlayerSnapshotStore struct {
	client valkey.Client
}

type memoryBotPlayerSnapshotStore struct {
	mu             sync.Mutex
	values         map[string][]byte
	statEventTimes map[string]memoryPlayerStatEventTime
}

type memoryPlayerStatEventTime struct {
	SnapshotHash string
	EventTime    time.Time
}

func newBotPlayerSnapshotStore(client valkey.Client) botPlayerSnapshotStore {
	if client != nil {
		return valkeyBotPlayerSnapshotStore{client: client}
	}
	return &memoryBotPlayerSnapshotStore{
		values:         make(map[string][]byte),
		statEventTimes: make(map[string]memoryPlayerStatEventTime),
	}
}

func (s valkeyBotPlayerSnapshotStore) Load(ctx context.Context, key string) ([]byte, bool, error) {
	value, err := s.client.Do(ctx, s.client.B().Get().Key(key).Build()).ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	raw, err := utils.Decompress([]byte(value))
	return raw, err == nil, err
}

func (s valkeyBotPlayerSnapshotStore) ReserveStatEventTime(
	ctx context.Context,
	key string,
	snapshotHash string,
	proposed time.Time,
) (time.Time, error) {
	value, err := reservePlayerStatEventTimeScript.Exec(
		ctx,
		s.client,
		[]string{key},
		[]string{snapshotHash, strconv.FormatInt(proposed.UTC().UnixNano(), 10)},
	).ToString()
	if err != nil {
		return time.Time{}, err
	}
	nanoseconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nanoseconds).UTC(), nil
}

func (s valkeyBotPlayerSnapshotStore) StoreAndClear(
	ctx context.Context,
	key string,
	pendingKey string,
	raw []byte,
) error {
	return storePlayerSnapshotScript.Exec(
		ctx,
		s.client,
		[]string{key, pendingKey},
		[]string{valkey.BinaryString(utils.Compress(raw))},
	).Error()
}

func (s *memoryBotPlayerSnapshotStore) Load(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.values[key]
	return append([]byte(nil), raw...), ok, nil
}

func (s *memoryBotPlayerSnapshotStore) ReserveStatEventTime(
	_ context.Context,
	key string,
	snapshotHash string,
	proposed time.Time,
) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.statEventTimes[key]; ok && existing.SnapshotHash == snapshotHash {
		return existing.EventTime, nil
	}
	proposed = proposed.UTC()
	s.statEventTimes[key] = memoryPlayerStatEventTime{
		SnapshotHash: snapshotHash,
		EventTime:    proposed,
	}
	return proposed, nil
}

func (s *memoryBotPlayerSnapshotStore) StoreAndClear(
	_ context.Context,
	key string,
	pendingKey string,
	raw []byte,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = append([]byte(nil), raw...)
	delete(s.statEventTimes, pendingKey)
	return nil
}

func (d *botPlayersDomain) loadPlayerSnapshot(ctx context.Context, tag string) ([]byte, error) {
	if d.snapshots == nil {
		return nil, nil
	}
	raw, _, err := d.snapshots.Load(ctx, playerSnapshotKey(tag))
	return raw, err
}

func (d *botPlayersDomain) savePlayerSnapshot(ctx context.Context, tag string, raw []byte) error {
	if d.snapshots == nil || tag == "" || len(raw) == 0 {
		return nil
	}
	return d.snapshots.StoreAndClear(
		ctx,
		playerSnapshotKey(tag),
		playerStatPendingKey(tag),
		raw,
	)
}

func (d *botPlayersDomain) reservePlayerStatEventTime(
	ctx context.Context,
	tag string,
	previousRaw []byte,
	proposed time.Time,
) (time.Time, error) {
	proposed = proposed.UTC().Truncate(time.Microsecond)
	if d.snapshots == nil {
		return proposed, nil
	}
	hash := sha256.Sum256(previousRaw)
	return d.snapshots.ReserveStatEventTime(
		ctx,
		playerStatPendingKey(tag),
		fmt.Sprintf("%x", hash[:]),
		proposed,
	)
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
	eventTime time.Time,
) ([]models.PlayerProfileChangeRow, int) {
	var profileChanges []models.PlayerProfileChangeRow
	activityScore := 0
	clan := clanTag(current)
	townhall, _ := asInt(current["townHallLevel"])

	for key, currentValue := range current {
		previousValue, exists := previous[key]
		if exists && equalJSON(previousValue, currentValue) {
			continue
		}
		if isHistoricalField(key) {
			profileChanges = append(profileChanges, models.PlayerProfileChangeRow{
				EventTime:     eventTime,
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
	}
	return profileChanges, activityScore
}

func playerStatChanges(
	tag string,
	previous clashy.Player,
	current clashy.Player,
	eventTime time.Time,
) []models.PlayerStatChangeRow {
	var clanTag *string
	if current.Clan != nil && current.Clan.Tag != "" {
		value := current.Clan.Tag
		clanTag = &value
	}
	counters := [...]struct {
		statType string
		previous int64
		current  int64
	}{
		{
			statType: playerStatDonated,
			previous: int64(previous.Donations),
			current:  int64(current.Donations),
		},
		{
			statType: playerStatReceived,
			previous: int64(previous.Received),
			current:  int64(current.Received),
		},
		{
			statType: playerStatClanGames,
			previous: playerClanGamesValue(previous),
			current:  playerClanGamesValue(current),
		},
		{
			statType: playerStatCapitalGoldDonated,
			previous: int64(previous.ClanCapitalContributions),
			current:  int64(current.ClanCapitalContributions),
		},
	}
	rows := make([]models.PlayerStatChangeRow, 0, len(counters))
	for _, counter := range counters {
		if counter.previous < 0 || counter.current <= counter.previous {
			continue
		}
		rows = append(rows, models.PlayerStatChangeRow{
			EventTime:     eventTime,
			PlayerTag:     tag,
			ClanTag:       clanTag,
			StatType:      counter.statType,
			PreviousValue: counter.previous,
			CurrentValue:  counter.current,
			Delta:         counter.current - counter.previous,
		})
	}
	return rows
}

func playerClanGamesValue(player clashy.Player) int64 {
	achievement := player.GetAchievement(playerClanGamesAchievement)
	if achievement == nil {
		return 0
	}
	return int64(achievement.Value)
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
	pool *pgxpool.Pool
}

func newTimescaleBotPlayerStore(
	ctx context.Context,
	dsn string,
) (*timescaleBotPlayerStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleBotPlayerStore{pool: pool}, nil
}

func (s *timescaleBotPlayerStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *timescaleBotPlayerStore) NextTargetPage(
	ctx context.Context,
	cursor string,
	limit int,
) (botPlayerTargetPage, error) {
	if limit <= 0 {
		return botPlayerTargetPage{}, nil
	}
	rows, err := s.pool.Query(ctx, botPlayerTargetsSQL, cursor, limit+1)
	if err != nil {
		return botPlayerTargetPage{}, err
	}
	defer rows.Close()
	var targets []models.BotPlayerTarget
	for rows.Next() {
		var target models.BotPlayerTarget
		if err := rows.Scan(&target.Tag); err != nil {
			return botPlayerTargetPage{}, err
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return botPlayerTargetPage{}, err
	}
	nextCursor := ""
	if len(targets) > limit {
		nextCursor = targets[limit-1].Tag
		targets = targets[:limit]
	}
	return botPlayerTargetPage{Targets: targets, NextCursor: nextCursor}, nil
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
	if len(ingest.StatChanges) > 0 {
		if _, err := tx.Exec(ctx, lockPlayerStatChangesSQL, ingest.SnapshotTag); err != nil {
			return err
		}
	}
	if err := utils.UpsertBasicPlayers(ctx, tx, ingest.Players, botPlayersDomainName); err != nil {
		return err
	}
	if err := insertPlayerProfileChanges(ctx, tx, ingest.ProfileChanges); err != nil {
		return err
	}
	if err := insertPlayerStatChanges(ctx, tx, ingest.StatChanges); err != nil {
		return err
	}
	if ingest.LastOnlineAt != nil && ingest.SnapshotTag != "" {
		if _, err := tx.Exec(ctx, updatePlayerLastActivitySQL, *ingest.LastOnlineAt, ingest.SnapshotTag); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

const botPlayerTargetsSQL = `
	SELECT tag
	FROM (
		SELECT member->>'tag' AS tag
		FROM basic_clan
		CROSS JOIN LATERAL jsonb_array_elements(COALESCE(members, '[]'::jsonb)) AS member
		WHERE member->>'tag' <> ''
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
	SET battlelogs_tracking_ttl = $1
	WHERE tag = $2
	  AND (battlelogs_tracking_ttl IS NULL OR battlelogs_tracking_ttl < $1)
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
		batch.Queue(insertPlayerProfileChangeSQL,
			row.EventTime, row.PlayerTag, row.ClanTag, row.TownHallLevel, row.ChangeType,
			string(previous), string(current))
	}
	return utils.SendBatch(ctx, tx, batch)
}

const insertPlayerProfileChangeSQL = `
	INSERT INTO player_change_history (
		event_time, player_tag, clan_tag, townhall_level, change_type,
		previous_value, current_value
	)
	VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb)
	ON CONFLICT DO NOTHING
`

const insertPlayerStatChangesSQL = `
	WITH existing AS MATERIALIZED (
		SELECT 1
		FROM player_stat_changes
		WHERE event_time = $1
		  AND player_tag = $2
		  AND stat_type = $4
		LIMIT 1
	),
	updated AS (
		UPDATE player_stat_changes
		SET clan_tag = $3,
			current_value = $6,
			delta = $7
		WHERE event_time = $1
		  AND player_tag = $2
		  AND stat_type = $4
		  AND current_value < $6
		RETURNING 1
	)
	INSERT INTO player_stat_changes (
		event_time, player_tag, clan_tag, stat_type,
		previous_value, current_value, delta
	)
	SELECT $1, $2, $3, $4, $5, $6, $7
	WHERE NOT EXISTS (SELECT 1 FROM existing)
	  AND NOT EXISTS (SELECT 1 FROM updated)
`

const lockPlayerStatChangesSQL = `
	SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
`

func insertPlayerStatChanges(
	ctx context.Context,
	tx pgx.Tx,
	rows []models.PlayerStatChangeRow,
) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if err := validatePlayerStatChange(row); err != nil {
			return err
		}
		batch.Queue(
			insertPlayerStatChangesSQL,
			row.EventTime,
			row.PlayerTag,
			row.ClanTag,
			row.StatType,
			row.PreviousValue,
			row.CurrentValue,
			row.Delta,
		)
	}
	return utils.SendBatch(ctx, tx, batch)
}

func validatePlayerStatChange(row models.PlayerStatChangeRow) error {
	if row.EventTime.IsZero() ||
		row.PlayerTag == "" ||
		!validPlayerStatType(row.StatType) ||
		row.PreviousValue < 0 ||
		row.CurrentValue <= row.PreviousValue ||
		row.Delta != row.CurrentValue-row.PreviousValue {
		return fmt.Errorf("invalid %s player stat change for %s", row.StatType, row.PlayerTag)
	}
	return nil
}

func validPlayerStatType(statType string) bool {
	switch statType {
	case playerStatDonated,
		playerStatReceived,
		playerStatClanGames,
		playerStatCapitalGoldDonated:
		return true
	default:
		return false
	}
}

type memoryBotPlayerStore struct {
	targets []models.BotPlayerTarget
}

func newMemoryBotPlayerStore() *memoryBotPlayerStore {
	return &memoryBotPlayerStore{}
}

func (s *memoryBotPlayerStore) Close() {}

func (s *memoryBotPlayerStore) NextTargetPage(
	_ context.Context,
	cursor string,
	limit int,
) (botPlayerTargetPage, error) {
	if limit <= 0 || len(s.targets) == 0 {
		return botPlayerTargetPage{}, nil
	}
	targets := append([]models.BotPlayerTarget(nil), s.targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].Tag < targets[j].Tag })
	start := sort.Search(len(targets), func(i int) bool { return targets[i].Tag > cursor })
	if start >= len(targets) {
		return botPlayerTargetPage{}, nil
	}
	end := start + limit
	if end > len(targets) {
		end = len(targets)
	}
	pageTargets := append([]models.BotPlayerTarget(nil), targets[start:end]...)
	nextCursor := ""
	if end < len(targets) {
		nextCursor = pageTargets[len(pageTargets)-1].Tag
	}
	return botPlayerTargetPage{Targets: pageTargets, NextCursor: nextCursor}, nil
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
