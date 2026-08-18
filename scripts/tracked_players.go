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
	trackedPlayersDomainName         = "trackedplayers"
	playerSnapshotTTL                = 30 * 24 * time.Hour
	playerSnapshotRefreshThreshold   = 23 * 24 * time.Hour
	removedPlayerSnapshotTTL         = 24 * time.Hour
	trackedPlayerSnapshotTargetsKey  = "tracking:tracked_player_snapshot_targets"
	trackedPlayerRegistryCommandSize = 1_000

	playerStatDonated            = "donated"
	playerStatReceived           = "received"
	playerStatClanGames          = "clan_games"
	playerStatCapitalGoldDonated = "capital_gold_donated"
	playerStatSeasonPass         = "season_pass"
	playerClanGamesAchievement   = "Games Champion"
	playerSeasonPassAchievement  = "Well Seasoned"
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
	redis.call('SET', KEYS[1], value, 'EX', ARGV[3])
	return ARGV[2]
`)

var storePlayerSnapshotScript = valkey.NewLuaScript(`
	redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
	redis.call('DEL', KEYS[2])
	return 1
`)

var loadAndRefreshPlayerSnapshotScript = valkey.NewLuaScript(`
	local value = redis.call('GET', KEYS[1])
	if not value then
		return false
	end
	local ttl = redis.call('TTL', KEYS[1])
	if ttl == -1 or (ttl >= 0 and ttl < tonumber(ARGV[1])) then
		redis.call('EXPIRE', KEYS[1], ARGV[2])
	end
	return value
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

type trackedPlayersDomain struct {
	snapshots              trackedPlayerSnapshotStore
	store                  trackedPlayerStore
	eventInterests         map[string]map[string]struct{}
	eventInterestsLoadedAt time.Time
}

type trackedPlayerStore interface {
	Close()
	NextTargetPage(context.Context, string, int) (trackedPlayerTargetPage, error)
	ListEventInterests(context.Context) (map[string]map[string]struct{}, error)
	StoreIngest(context.Context, models.TrackedPlayerIngest) error
}

type trackedPlayerTargetPage struct {
	Targets    []models.TrackedPlayerTarget
	NextCursor string
}

func NewTrackedPlayersDomain() platform.Domain { return &trackedPlayersDomain{} }

func (d *trackedPlayersDomain) Name() string { return trackedPlayersDomainName }

func (d *trackedPlayersDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateTrackedPlayersConfig(app.Config); err != nil {
		return err
	}
	store, err := newTrackedPlayerStore(ctx, app)
	if err != nil {
		return err
	}
	defer store.Close()
	d.snapshots = newTrackedPlayerSnapshotStore(app.Valkey)
	d.store = store
	if err := d.reloadEventInterests(ctx); err != nil {
		return err
	}

	limiter, err := newTrackingLimiter(app.Config.TrackedPlayerRequestsPerSecond)
	if err != nil {
		return err
	}
	pageSize := app.Config.TrackedPlayerRequestsPerSecond * app.Config.TargetPageMultiplier
	cursor := ""
	previousTargets, err := loadTrackedPlayerSnapshotTargets(ctx, app.Valkey)
	if err != nil {
		return err
	}
	cycleTargets := make(map[string]struct{}, max(pageSize, len(previousTargets)))
	cycle := make([]models.TrackedPlayerTarget, 0, pageSize)
	processTargets := func(targets []models.TrackedPlayerTarget) error {
		for _, target := range targets {
			if target.Tag != "" {
				cycleTargets[target.Tag] = struct{}{}
			}
		}
		return runBounded(ctx, platform.RequestConcurrency(app.Config.TrackedPlayerRequestsPerSecond), targets, func(workerCtx context.Context, target models.TrackedPlayerTarget) error {
			ingest, err := retryLimitedClashFetch(workerCtx, app, limiter, func(fetchCtx context.Context) (models.TrackedPlayerIngest, error) {
				return d.fetchAndPreparePlayer(fetchCtx, app, target)
			})
			if err != nil {
				if workerCtx.Err() != nil {
					return workerCtx.Err()
				}
				if errors.Is(err, context.Canceled) {
					return err
				}
				app.Logger.Error("tracked player processing failed", "tag", target.Tag, "err", err)
				app.Stats.SetReady(trackedPlayersDomainName, false, err.Error())
				return err
			}
			if err := d.storePlayerIngest(workerCtx, app, ingest); err != nil {
				return err
			}
			app.Stats.RecordTrackedTarget(trackedPlayersDomainName)
			return nil
		})
	}
	loadVerifiedTargets := func() ([]models.TrackedPlayerTarget, error) {
		verifiedTags, err := activeVerifiedPlayerTags(ctx, app.Valkey)
		if err != nil {
			return nil, err
		}
		verified := make([]models.TrackedPlayerTarget, 0, len(verifiedTags))
		for _, tag := range verifiedTags {
			verified = append(verified, models.TrackedPlayerTarget{Tag: tag, Verified: true})
		}
		return verified, nil
	}
	finishCycle := func() error {
		verified, err := loadVerifiedTargets()
		if err != nil {
			return err
		}
		cycle = append(cycle, verified...)
		if err := processTargets(cycle); err != nil {
			return err
		}
		if err := reconcileTrackedPlayerSnapshotTargets(ctx, app.Valkey, previousTargets, cycleTargets); err != nil {
			return err
		}
		previousTargets = cycleTargets
		cycleTargets = make(map[string]struct{}, max(pageSize, len(previousTargets)))
		cycle = cycle[:0]
		if time.Since(d.eventInterestsLoadedAt) >= time.Duration(app.Config.TrackedPlayerTargetRefreshSeconds)*time.Second {
			if err := d.reloadEventInterests(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	for {
		page, err := d.store.NextTargetPage(ctx, cursor, pageSize)
		if err != nil {
			return err
		}
		if len(page.Targets) == 0 {
			cursor = ""
			if err := finishCycle(); err != nil {
				return err
			}
			if err := sleepOrDone(ctx, time.Second); err != nil {
				return err
			}
			continue
		}
		cycle = append(cycle, page.Targets...)
		cursor = page.NextCursor
		if cursor == "" {
			start := time.Now()
			if err := finishCycle(); err != nil {
				return err
			}
			app.Stats.RecordProcess(trackedPlayersDomainName, time.Since(start))
		}
	}
}

func validateTrackedPlayersConfig(cfg platform.Config) error {
	if cfg.TrackedPlayerRequestsPerSecond <= 0 {
		return errors.New("trackedplayers.requests_per_second must be greater than zero")
	}
	if cfg.TargetPageMultiplier <= 0 {
		return errors.New("target_page_multiplier must be greater than zero")
	}
	if cfg.TrackedPlayerTargetRefreshSeconds <= 0 {
		return errors.New("trackedplayers.target_refresh_seconds must be greater than zero")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_* connection variables are required for trackedplayers")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.ValkeyAddr == "" {
		return errors.New("valkey_addr is required for trackedplayers snapshots")
	}
	return nil
}

func (d *trackedPlayersDomain) reloadEventInterests(ctx context.Context) error {
	interests, err := d.store.ListEventInterests(ctx)
	if err != nil {
		return err
	}
	d.eventInterests = interests
	d.eventInterestsLoadedAt = time.Now()
	return nil
}

func loadTrackedPlayerSnapshotTargets(ctx context.Context, client valkey.Client) (map[string]struct{}, error) {
	targets := make(map[string]struct{})
	if client == nil {
		return targets, nil
	}
	values, err := client.Do(ctx, client.B().Smembers().Key(trackedPlayerSnapshotTargetsKey).Build()).AsStrSlice()
	if err != nil {
		return nil, err
	}
	for _, tag := range values {
		if tag != "" {
			targets[tag] = struct{}{}
		}
	}
	return targets, nil
}

func reconcileTrackedPlayerSnapshotTargets(
	ctx context.Context,
	client valkey.Client,
	previous map[string]struct{},
	current map[string]struct{},
) error {
	if client == nil {
		return nil
	}
	added := make([]string, 0)
	removed := make([]string, 0)
	for tag := range current {
		if _, exists := previous[tag]; !exists {
			added = append(added, tag)
		}
	}
	for tag := range previous {
		if _, exists := current[tag]; !exists {
			removed = append(removed, tag)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)

	commands := make([]valkey.Completed, 0, 2+(2*len(removed)))
	for start := 0; start < len(added); start += trackedPlayerRegistryCommandSize {
		end := min(start+trackedPlayerRegistryCommandSize, len(added))
		commands = append(commands, client.B().Sadd().
			Key(trackedPlayerSnapshotTargetsKey).
			Member(added[start:end]...).
			Build())
	}
	for start := 0; start < len(removed); start += trackedPlayerRegistryCommandSize {
		end := min(start+trackedPlayerRegistryCommandSize, len(removed))
		commands = append(commands, client.B().Srem().
			Key(trackedPlayerSnapshotTargetsKey).
			Member(removed[start:end]...).
			Build())
	}
	for _, tag := range removed {
		commands = append(commands,
			client.B().Expire().Key(playerSnapshotKey(tag)).
				Seconds(int64(removedPlayerSnapshotTTL/time.Second)).Lt().Build(),
			client.B().Expire().Key(playerStatPendingKey(tag)).
				Seconds(int64(removedPlayerSnapshotTTL/time.Second)).Lt().Build(),
		)
	}
	for start := 0; start < len(commands); start += trackedPlayerRegistryCommandSize {
		end := min(start+trackedPlayerRegistryCommandSize, len(commands))
		for _, result := range client.DoMulti(ctx, commands[start:end]...) {
			if err := result.Error(); err != nil {
				return err
			}
		}
	}
	return nil
}

func newTrackedPlayerStore(ctx context.Context, app *platform.App) (trackedPlayerStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return newMemoryTrackedPlayerStore(), nil
	}
	return newTimescaleTrackedPlayerStore(ctx, app.Config.TimescaleURL)
}

func (d *trackedPlayersDomain) fetchAndPreparePlayer(
	ctx context.Context,
	app *platform.App,
	target models.TrackedPlayerTarget,
) (models.TrackedPlayerIngest, error) {
	if target.Tag == "" {
		return models.TrackedPlayerIngest{}, nil
	}
	start := time.Now()
	player, err := app.Clash.GetPlayer(ctx, target.Tag)
	app.Stats.RecordRequest(trackedPlayersDomainName, time.Since(start), err)
	if err != nil {
		if isClashNotFound(err) {
			return models.TrackedPlayerIngest{}, nil
		}
		return models.TrackedPlayerIngest{}, err
	}
	if player == nil {
		return models.TrackedPlayerIngest{}, nil
	}
	ingest, err := d.doPlayer(ctx, target.Tag, *player)
	ingest.VerifiedTracking = target.Verified
	if player.Clan != nil {
		ingest.CurrentClan = player.Clan.Tag
	}
	return ingest, err
}

func (d *trackedPlayersDomain) storePlayerIngest(ctx context.Context, app *platform.App, ingest models.TrackedPlayerIngest) error {
	if ingest.VerifiedTracking {
		if err := updateVerifiedPlayerClan(ctx, app.Valkey, ingest.SnapshotTag, ingest.CurrentClan); err != nil {
			return err
		}
	}
	if len(ingest.Players) == 0 && len(ingest.ProfileChanges) == 0 && len(ingest.StatChanges) == 0 {
		return d.savePlayerSnapshot(ctx, ingest.SnapshotTag, ingest.SnapshotRaw)
	}
	if err := d.store.StoreIngest(ctx, ingest); err != nil {
		return err
	}
	app.Stats.RecordWrite(trackedPlayersDomainName,
		len(ingest.Players)+len(ingest.ProfileChanges)+len(ingest.StatChanges),
	)
	app.Stats.SetReady(trackedPlayersDomainName, true, "")
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

func (d *trackedPlayersDomain) doPlayer(
	ctx context.Context,
	tag string,
	player clashy.Player,
) (models.TrackedPlayerIngest, error) {
	raw, err := json.Marshal(player)
	if err != nil {
		return models.TrackedPlayerIngest{}, err
	}
	previousRaw, err := d.loadPlayerSnapshot(ctx, tag)
	if err != nil {
		return models.TrackedPlayerIngest{}, err
	}
	if len(previousRaw) == 0 {
		return models.TrackedPlayerIngest{
			Players:     []models.BasicPlayerRow{trackedPlayerRow(player)},
			SnapshotTag: tag,
			SnapshotRaw: raw,
		}, nil
	}
	if equalBytes(previousRaw, raw) {
		return models.TrackedPlayerIngest{SnapshotTag: tag}, nil
	}
	var previousPlayer clashy.Player
	if err := json.Unmarshal(previousRaw, &previousPlayer); err != nil {
		return models.TrackedPlayerIngest{SnapshotTag: tag, SnapshotRaw: raw}, nil
	}
	current := playerMap(player)
	previous := playerMap(previousPlayer)
	if equalJSON(previous, current) {
		return models.TrackedPlayerIngest{SnapshotTag: tag}, nil
	}
	now := time.Now().UTC()
	changes, activityDetected := playerChanges(tag, previous, current, now)
	activityDetected = activityDetected || playerAchievementActivityDetected(previousPlayer, player)
	statChanges := playerStatChanges(tag, previousPlayer, player, now)
	if len(statChanges) > 0 || activityDetected {
		statEventTime, err := d.reservePlayerStatEventTime(ctx, tag, previousRaw, now)
		if err != nil {
			return models.TrackedPlayerIngest{}, err
		}
		for index := range statChanges {
			statChanges[index].EventTime = statEventTime
		}
		now = statEventTime
	}
	clan := clanTag(current)
	var lastOnline *time.Time
	if activityDetected {
		lastOnline = &now
	}
	ingest := models.TrackedPlayerIngest{
		Players:        []models.BasicPlayerRow{trackedPlayerRow(player)},
		ProfileChanges: changes,
		StatChanges:    statChanges,
		LastOnlineAt:   lastOnline,
		SnapshotTag:    tag,
		SnapshotRaw:    raw,
	}
	eventChanges, eventLogTypes := d.interestedPlayerChanges(clan, changes)
	if len(eventChanges) > 0 {
		ingest.Event = models.Event{
			Topic: "player",
			Key:   clan,
			Type:  "player_update",
			Value: map[string]any{
				"tag":             tag,
				"name":            player.Name,
				"clan_tag":        clan,
				"town_hall_level": player.TownHall,
				"changed_types":   playerChangeTypes(eventChanges),
				"log_types":       eventLogTypes,
				"changes":         playerEventChanges(eventChanges),
			},
			CreatedAt: now,
		}
	}
	return ingest, nil
}

func playerEventChanges(changes []models.PlayerProfileChangeRow) []map[string]any {
	out := make([]map[string]any, 0, len(changes))
	for _, change := range changes {
		out = append(out, map[string]any{
			"type":     change.ChangeType,
			"previous": change.PreviousValue,
			"current":  change.CurrentValue,
		})
	}
	return out
}

func (d *trackedPlayersDomain) interestedPlayerChanges(
	clanTag string,
	changes []models.PlayerProfileChangeRow,
) ([]models.PlayerProfileChangeRow, []string) {
	if clanTag == "" || len(changes) == 0 {
		return nil, nil
	}
	interests := d.eventInterests[clanTag]
	if len(interests) == 0 {
		return nil, nil
	}
	filtered := make([]models.PlayerProfileChangeRow, 0, len(changes))
	matchedLogs := make(map[string]struct{})
	for _, change := range changes {
		matched := false
		for _, logType := range playerLogTypesForChange(change.ChangeType) {
			if _, exists := interests[logType]; exists {
				matched = true
				matchedLogs[logType] = struct{}{}
			}
		}
		if matched {
			filtered = append(filtered, change)
		}
	}
	logTypes := make([]string, 0, len(matchedLogs))
	for logType := range matchedLogs {
		logTypes = append(logTypes, logType)
	}
	sort.Strings(logTypes)
	return filtered, logTypes
}

func playerLogTypesForChange(changeType string) []string {
	switch changeType {
	case "troops":
		return []string{"super_troop_boost", "troop_upgrade"}
	case "heroes":
		return []string{"hero_upgrade"}
	case "spells":
		return []string{"spell_upgrade"}
	case "heroEquipment":
		return []string{"hero_equipment_upgrade"}
	case "townHallLevel":
		return []string{"th_upgrade"}
	case "leagueTier":
		return []string{"league_change"}
	case "name":
		return []string{"name_change"}
	default:
		return nil
	}
}

func trackedPlayerRow(player clashy.Player) models.BasicPlayerRow {
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

type trackedPlayerSnapshotStore interface {
	Load(context.Context, string) ([]byte, bool, error)
	ReserveStatEventTime(context.Context, string, string, time.Time) (time.Time, error)
	StoreAndClear(context.Context, string, string, []byte) error
}

type valkeyTrackedPlayerSnapshotStore struct {
	client valkey.Client
}

type memoryTrackedPlayerSnapshotStore struct {
	mu             sync.Mutex
	values         map[string][]byte
	statEventTimes map[string]memoryPlayerStatEventTime
}

type memoryPlayerStatEventTime struct {
	SnapshotHash string
	EventTime    time.Time
}

func newTrackedPlayerSnapshotStore(client valkey.Client) trackedPlayerSnapshotStore {
	if client != nil {
		return valkeyTrackedPlayerSnapshotStore{client: client}
	}
	return &memoryTrackedPlayerSnapshotStore{
		values:         make(map[string][]byte),
		statEventTimes: make(map[string]memoryPlayerStatEventTime),
	}
}

func (s valkeyTrackedPlayerSnapshotStore) Load(ctx context.Context, key string) ([]byte, bool, error) {
	value, err := loadAndRefreshPlayerSnapshotScript.Exec(
		ctx,
		s.client,
		[]string{key},
		[]string{
			strconv.FormatInt(int64(playerSnapshotRefreshThreshold/time.Second), 10),
			strconv.FormatInt(int64(playerSnapshotTTL/time.Second), 10),
		},
	).ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	raw, err := utils.Decompress([]byte(value))
	return raw, err == nil, err
}

func (s valkeyTrackedPlayerSnapshotStore) ReserveStatEventTime(
	ctx context.Context,
	key string,
	snapshotHash string,
	proposed time.Time,
) (time.Time, error) {
	value, err := reservePlayerStatEventTimeScript.Exec(
		ctx,
		s.client,
		[]string{key},
		[]string{
			snapshotHash,
			strconv.FormatInt(proposed.UTC().UnixNano(), 10),
			strconv.FormatInt(int64(playerSnapshotTTL/time.Second), 10),
		},
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

func (s valkeyTrackedPlayerSnapshotStore) StoreAndClear(
	ctx context.Context,
	key string,
	pendingKey string,
	raw []byte,
) error {
	return storePlayerSnapshotScript.Exec(
		ctx,
		s.client,
		[]string{key, pendingKey},
		[]string{
			valkey.BinaryString(utils.Compress(raw)),
			strconv.FormatInt(int64(playerSnapshotTTL/time.Second), 10),
		},
	).Error()
}

func (s *memoryTrackedPlayerSnapshotStore) Load(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.values[key]
	return append([]byte(nil), raw...), ok, nil
}

func (s *memoryTrackedPlayerSnapshotStore) ReserveStatEventTime(
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

func (s *memoryTrackedPlayerSnapshotStore) StoreAndClear(
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

func (d *trackedPlayersDomain) loadPlayerSnapshot(ctx context.Context, tag string) ([]byte, error) {
	if d.snapshots == nil {
		return nil, nil
	}
	raw, _, err := d.snapshots.Load(ctx, playerSnapshotKey(tag))
	return raw, err
}

func (d *trackedPlayersDomain) savePlayerSnapshot(ctx context.Context, tag string, raw []byte) error {
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

func (d *trackedPlayersDomain) reservePlayerStatEventTime(
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
) ([]models.PlayerProfileChangeRow, bool) {
	var profileChanges []models.PlayerProfileChangeRow
	activityDetected := false
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
			activityDetected = true
		}
	}
	return profileChanges, activityDetected
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
		{
			statType: playerStatSeasonPass,
			previous: playerAchievementValue(previous, playerSeasonPassAchievement),
			current:  playerAchievementValue(current, playerSeasonPassAchievement),
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
	return playerAchievementValue(player, playerClanGamesAchievement)
}

func playerAchievementValue(player clashy.Player, name string) int64 {
	achievement := player.GetAchievement(name)
	if achievement == nil {
		return 0
	}
	return int64(achievement.Value)
}

func playerAchievementActivityDetected(previous, current clashy.Player) bool {
	for _, name := range [...]string{
		"Gold Grab", "Most Valuable Clanmate", "War League Legend", "Wall Buster",
		"Well Seasoned", "Games Champion", "Elixir Escapade", "Heroic Heist",
		"Nice and Tidy", "Anti-Artillery", "Firefighter", "X-Bow Exterminator",
	} {
		if playerAchievementValue(previous, name) != playerAchievementValue(current, name) {
			return true
		}
	}
	return false
}

func isHistoricalField(key string) bool {
	switch key {
	case "name", "troops", "heroes", "spells", "heroEquipment", "townHallLevel",
		"leagueTier", "warStars", "warPreference", "bestBuilderBaseTrophies", "bestTrophies",
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

type timescaleTrackedPlayerStore struct {
	pool *pgxpool.Pool
}

func newTimescaleTrackedPlayerStore(
	ctx context.Context,
	dsn string,
) (*timescaleTrackedPlayerStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleTrackedPlayerStore{pool: pool}, nil
}

func (s *timescaleTrackedPlayerStore) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *timescaleTrackedPlayerStore) NextTargetPage(
	ctx context.Context,
	cursor string,
	limit int,
) (trackedPlayerTargetPage, error) {
	if limit <= 0 {
		return trackedPlayerTargetPage{}, nil
	}
	rows, err := s.pool.Query(ctx, trackedPlayerTargetsSQL, cursor, limit+1)
	if err != nil {
		return trackedPlayerTargetPage{}, err
	}
	defer rows.Close()
	var targets []models.TrackedPlayerTarget
	for rows.Next() {
		var target models.TrackedPlayerTarget
		if err := rows.Scan(&target.Tag); err != nil {
			return trackedPlayerTargetPage{}, err
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return trackedPlayerTargetPage{}, err
	}
	nextCursor := ""
	if len(targets) > limit {
		nextCursor = targets[limit-1].Tag
		targets = targets[:limit]
	}
	return trackedPlayerTargetPage{Targets: targets, NextCursor: nextCursor}, nil
}

func (s *timescaleTrackedPlayerStore) ListEventInterests(
	ctx context.Context,
) (map[string]map[string]struct{}, error) {
	rows, err := s.pool.Query(ctx, trackedPlayerEventInterestsSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	interests := make(map[string]map[string]struct{})
	for rows.Next() {
		var clanTag, logType string
		if err := rows.Scan(&clanTag, &logType); err != nil {
			return nil, err
		}
		if interests[clanTag] == nil {
			interests[clanTag] = make(map[string]struct{})
		}
		interests[clanTag][logType] = struct{}{}
	}
	return interests, rows.Err()
}

func (s *timescaleTrackedPlayerStore) StoreIngest(
	ctx context.Context,
	ingest models.TrackedPlayerIngest,
) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if len(ingest.StatChanges) > 0 || ingest.LastOnlineAt != nil {
		if _, err := tx.Exec(ctx, lockPlayerStatChangesSQL, ingest.SnapshotTag); err != nil {
			return err
		}
	}
	if err := utils.UpsertBasicPlayers(ctx, tx, ingest.Players, trackedPlayersDomainName); err != nil {
		return err
	}
	if err := insertPlayerProfileChanges(ctx, tx, ingest.ProfileChanges); err != nil {
		return err
	}
	if err := insertPlayerStatChanges(ctx, tx, ingest.StatChanges); err != nil {
		return err
	}
	if ingest.LastOnlineAt != nil && len(ingest.Players) > 0 && ingest.Players[0].ClanTag != "" {
		if _, err := tx.Exec(ctx, insertPlayerOnlineEventSQL,
			*ingest.LastOnlineAt, ingest.SnapshotTag, ingest.Players[0].ClanTag); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

const trackedPlayerTargetSetSQL = `
		SELECT member->>'tag' AS tag
		FROM server_clans tracked_clan
		JOIN servers server ON server.id = tracked_clan.server_id
		JOIN basic_clan ON basic_clan.tag = tracked_clan.tag
		CROSS JOIN LATERAL jsonb_array_elements(COALESCE(members, '[]'::jsonb)) AS member
		WHERE server.last_command_at >= now() - interval '90 days'
		  AND member->>'tag' <> ''
		  AND COALESCE(NULLIF(member->>'town_hall', ''), '0')::integer >= 9
`

const trackedPlayerTargetsSQL = `
	SELECT tag
	FROM (` + trackedPlayerTargetSetSQL + `) targets
	WHERE tag > $1
	ORDER BY tag
	LIMIT $2
`

const trackedPlayerEventInterestsSQL = `
	SELECT DISTINCT log.clan_tag, log.type
	FROM server_logs log
	JOIN servers server ON server.id = log.server_id
	WHERE server.last_command_at >= now() - interval '90 days'
	  AND log.disabled = false
	  AND log.clan_tag <> ''
	  AND log.type IN (
	      'troop_upgrade', 'super_troop_boost', 'th_upgrade',
	      'league_change', 'spell_upgrade', 'hero_upgrade',
	      'hero_equipment_upgrade', 'name_change'
	  )
`

const insertPlayerOnlineEventSQL = `
	INSERT INTO player_online_events (seen_at, tag, clan_tag)
	SELECT $1, $2, $3
	WHERE NOT EXISTS (
		SELECT 1 FROM player_online_events
		WHERE seen_at = $1 AND tag = $2 AND clan_tag = $3
	)
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
		playerStatCapitalGoldDonated,
		playerStatSeasonPass:
		return true
	default:
		return false
	}
}

type memoryTrackedPlayerStore struct {
	targets []models.TrackedPlayerTarget
}

func newMemoryTrackedPlayerStore() *memoryTrackedPlayerStore {
	return &memoryTrackedPlayerStore{}
}

func (s *memoryTrackedPlayerStore) Close() {}

func (s *memoryTrackedPlayerStore) NextTargetPage(
	_ context.Context,
	cursor string,
	limit int,
) (trackedPlayerTargetPage, error) {
	if limit <= 0 || len(s.targets) == 0 {
		return trackedPlayerTargetPage{}, nil
	}
	targets := append([]models.TrackedPlayerTarget(nil), s.targets...)
	sort.Slice(targets, func(i, j int) bool { return targets[i].Tag < targets[j].Tag })
	start := sort.Search(len(targets), func(i int) bool { return targets[i].Tag > cursor })
	if start >= len(targets) {
		return trackedPlayerTargetPage{}, nil
	}
	end := start + limit
	if end > len(targets) {
		end = len(targets)
	}
	pageTargets := append([]models.TrackedPlayerTarget(nil), targets[start:end]...)
	nextCursor := ""
	if end < len(targets) {
		nextCursor = pageTargets[len(pageTargets)-1].Tag
	}
	return trackedPlayerTargetPage{Targets: pageTargets, NextCursor: nextCursor}, nil
}

func (s *memoryTrackedPlayerStore) ListEventInterests(
	context.Context,
) (map[string]map[string]struct{}, error) {
	return map[string]map[string]struct{}{}, nil
}

func (s *memoryTrackedPlayerStore) StoreIngest(context.Context, models.TrackedPlayerIngest) error {
	return nil
}
