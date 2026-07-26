package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

type botClanFetchFunc[T any] func(context.Context, string) (*T, error)

type TrackedItem[T any] struct {
	Group   string
	Kind    string
	Tag     string
	Current *T
	Raw     []byte
}

const (
	botClansDomainName        = "botclans"
	botClanReminderTTL        = time.Minute
	botClanReminderRetryDelay = time.Minute
	capitalRaidCacheGrace     = 10 * time.Minute
)

var replaceCapitalRaidCacheScript = valkey.NewLuaScript(`
	local clan_tag = ARGV[2]
	local expires_at_ms = ARGV[3]
	local member_key_prefix = ARGV[4]
	local previous_members = redis.call('SMEMBERS', KEYS[2])

	for _, player_tag in ipairs(previous_members) do
		local member_key = member_key_prefix .. player_tag
		if redis.call('GET', member_key) == clan_tag then
			redis.call('DEL', member_key)
		end
	end

	redis.call('DEL', KEYS[1], KEYS[2])
	redis.call('SET', KEYS[1], ARGV[1], 'PXAT', expires_at_ms)

	for index = 5, #ARGV do
		local player_tag = ARGV[index]
		redis.call('SADD', KEYS[2], player_tag)
		redis.call('SET', member_key_prefix .. player_tag, clan_tag, 'PXAT', expires_at_ms)
	end

	if #ARGV >= 5 then
		redis.call('PEXPIREAT', KEYS[2], expires_at_ms)
	end

	return #ARGV - 4
`)

var deleteCapitalRaidCacheScript = valkey.NewLuaScript(`
	local clan_tag = ARGV[1]
	local member_key_prefix = ARGV[2]
	local previous_members = redis.call('SMEMBERS', KEYS[2])

	for _, player_tag in ipairs(previous_members) do
		local member_key = member_key_prefix .. player_tag
		if redis.call('GET', member_key) == clan_tag then
			redis.call('DEL', member_key)
		end
	end

	return redis.call('DEL', KEYS[1], KEYS[2])
`)

type cachedWarReminders struct {
	loadedAt time.Time
	values   []warReminder
}

type cachedRaidReminders struct {
	loadedAt time.Time
	values   []raidReminder
}

type botClansDomain struct {
	mu               sync.Mutex
	scheduled        map[string]struct{}
	targetsMu        sync.RWMutex
	targets          []string
	snapshots        botClanSnapshotStore
	snapshotPrefix   string
	cwlStateSnapshot string
	store            botClanStore
	capitalRaids     capitalRaidCache
	reminderMu       sync.Mutex
	warReminders     map[string]cachedWarReminders
	raidReminders    map[string]cachedRaidReminders
}

func NewBotClansDomain() platform.Domain {
	return &botClansDomain{
		scheduled: make(map[string]struct{}),
		snapshots: &memoryBotClanSnapshotStore{
			values: make(map[string][]byte),
		},
		capitalRaids:  newMemoryCapitalRaidCache("botclans:snapshot:", time.Now),
		warReminders:  make(map[string]cachedWarReminders),
		raidReminders: make(map[string]cachedRaidReminders),
	}
}

func (d *botClansDomain) Name() string { return botClansDomainName }

func (d *botClansDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateBotClansConfig(app.Config); err != nil {
		return err
	}
	store, err := newBotClanStore(ctx, app)
	if err != nil {
		return err
	}
	defer store.Close()
	d.store = store
	targets, err := store.ListTargets(ctx)
	if err != nil {
		return err
	}
	d.replaceTargets(app, targets)
	d.snapshots = newBotClanSnapshotStore(app)
	d.snapshotPrefix = app.Config.BotClanSnapshotPrefix
	d.cwlStateSnapshot = app.Config.BotClanCWLStateSnapshot
	d.capitalRaids = newCapitalRaidCache(app, d.snapshotPrefix)

	rateLimit := app.Config.BotClanRequestsPerSecond
	limiter, err := newTrackingLimiter(rateLimit)
	if err != nil {
		return err
	}
	trackerCtx, stopTrackers := context.WithCancel(ctx)
	defer stopTrackers()
	errCh := make(chan error, 4)
	go func() {
		errCh <- runBotClanTracker(trackerCtx, app, d, "clans", "clan", fetchClan(app), d.handleClanChange, limiter, rateLimit)
	}()
	go func() {
		errCh <- runBotClanTracker(trackerCtx, app, d, "wars", "war", fetchWar(app), d.handleWarChange, limiter, rateLimit)
	}()
	go func() {
		errCh <- runBotClanTracker(trackerCtx, app, d, "raids", "raid", fetchRaid(app), d.handleRaidChange, limiter, rateLimit)
	}()
	go d.runTargetRefreshLoop(trackerCtx, app, store)
	go func() {
		errCh <- d.runCWLLoop(trackerCtx, app, limiter)
	}()
	firstErr := <-errCh
	stopTrackers()
	for range 3 {
		<-errCh
	}
	return firstErr
}

func validateBotClansConfig(cfg platform.Config) error {
	if cfg.BotClanRequestsPerSecond <= 0 {
		return errors.New("botclans.requests_per_second must be greater than zero")
	}
	if cfg.BotClanTargetRefreshSeconds <= 0 {
		return errors.New("botclans.target_refresh_seconds must be greater than zero")
	}
	if cfg.BotClanSnapshotPrefix == "" {
		return errors.New("botclans.snapshot_prefix is required")
	}
	if cfg.BotClanCWLStateSnapshot == "" {
		return errors.New("botclans.cwl_state_snapshot is required")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required for botclans server_clans targets")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.ValkeyAddr == "" {
		return errors.New("valkey_addr is required for botclans snapshots")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.EventStreamName == "" {
		return errors.New("events.stream is required for botclans event publishing")
	}
	return nil
}

type warReminder struct {
	TriggerTime      string
	MinutesRemaining int
}

type raidReminder struct {
	ID               string
	ServerID         string
	ClanTag          string
	ChannelID        string
	TriggerTime      string
	MinutesRemaining int
	CustomText       string
	TownHalls        []int
	Roles            []string
	WarTypes         []string
	AttackThreshold  int
}

func (r raidReminder) eventData() map[string]any {
	return map[string]any{
		"_id":              r.ID,
		"server":           r.ServerID,
		"type":             "Clan Capital",
		"clan":             r.ClanTag,
		"channel":          r.ChannelID,
		"time":             r.TriggerTime,
		"custom_text":      r.CustomText,
		"townhalls":        r.TownHalls,
		"roles":            r.Roles,
		"types":            r.WarTypes,
		"attack_threshold": r.AttackThreshold,
	}
}

type botClanStore interface {
	ListTargets(context.Context) ([]string, error)
	ListWarReminders(context.Context, string) ([]warReminder, error)
	ListRaidReminders(context.Context, string) ([]raidReminder, error)
	Close()
}

type timescaleBotClanStore struct {
	pool *pgxpool.Pool
}

func newBotClanStore(ctx context.Context, app *platform.App) (botClanStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return memoryBotClanStore{}, nil
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return nil, err
	}
	return &timescaleBotClanStore{pool: pool}, nil
}

func (s *timescaleBotClanStore) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *timescaleBotClanStore) ListTargets(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT tag
		FROM server_clans
		WHERE tag <> ''
		ORDER BY tag
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		out = append(out, tag)
	}
	return out, rows.Err()
}

func (s *timescaleBotClanStore) ListWarReminders(ctx context.Context, clanTag string) ([]warReminder, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT COALESCE(trigger_time, ''), minutes_remaining
		FROM reminders
		WHERE clan_tag = $1 AND type = 1
		ORDER BY minutes_remaining DESC
	`, clanTag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []warReminder
	for rows.Next() {
		var reminder warReminder
		if err := rows.Scan(&reminder.TriggerTime, &reminder.MinutesRemaining); err != nil {
			return nil, err
		}
		if reminder.TriggerTime == "" {
			reminder.TriggerTime = strconv.Itoa(reminder.MinutesRemaining) + "min"
		}
		out = append(out, reminder)
	}
	return out, rows.Err()
}

func (s *timescaleBotClanStore) ListRaidReminders(ctx context.Context, clanTag string) ([]raidReminder, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id::text, server_id, clan_tag, COALESCE(channel_id, ''),
			COALESCE(trigger_time, ''), minutes_remaining, custom_text,
			COALESCE(townhalls, '{}'::integer[]), roles, war_type_names,
			COALESCE(trigger_threshold, 1)
		FROM reminders
		WHERE clan_tag = $1 AND type = 2
		ORDER BY id
	`, clanTag)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []raidReminder
	for rows.Next() {
		var reminder raidReminder
		if err := rows.Scan(
			&reminder.ID,
			&reminder.ServerID,
			&reminder.ClanTag,
			&reminder.ChannelID,
			&reminder.TriggerTime,
			&reminder.MinutesRemaining,
			&reminder.CustomText,
			&reminder.TownHalls,
			&reminder.Roles,
			&reminder.WarTypes,
			&reminder.AttackThreshold,
		); err != nil {
			return nil, err
		}
		if reminder.TriggerTime == "" {
			reminder.TriggerTime = strconv.Itoa(reminder.MinutesRemaining) + "min"
		}
		out = append(out, reminder)
	}
	return out, rows.Err()
}

type memoryBotClanStore struct {
	tags []string
}

func (s memoryBotClanStore) Close() {}

func (s memoryBotClanStore) ListTargets(context.Context) ([]string, error) {
	return append([]string(nil), s.tags...), nil
}

func (memoryBotClanStore) ListWarReminders(context.Context, string) ([]warReminder, error) {
	return nil, nil
}

func (memoryBotClanStore) ListRaidReminders(context.Context, string) ([]raidReminder, error) {
	return nil, nil
}

func (d *botClansDomain) targetTags() []string {
	d.targetsMu.RLock()
	defer d.targetsMu.RUnlock()
	return append([]string(nil), d.targets...)
}

func (d *botClansDomain) replaceTargets(app *platform.App, targets []string) {
	d.targetsMu.Lock()
	d.targets = append(d.targets[:0], targets...)
	d.targetsMu.Unlock()
	for _, group := range []string{"clans", "wars", "raids"} {
		app.Stats.SetTrackingTargets(trackingProgressName(botClansDomainName, group), len(targets))
	}
}

func (d *botClansDomain) runTargetRefreshLoop(ctx context.Context, app *platform.App, source botClanStore) {
	interval := time.Duration(app.Config.BotClanTargetRefreshSeconds) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		targets, err := source.ListTargets(ctx)
		if err != nil {
			app.Logger.Error("bot clan target refresh failed", "err", err)
			app.Stats.SetReady(botClansDomainName, false, err.Error())
			continue
		}
		d.replaceTargets(app, targets)
		app.Logger.Info("refreshed bot clan targets", "count", len(targets))
	}
}

func runBotClanTracker[T any](
	ctx context.Context,
	app *platform.App,
	domain *botClansDomain,
	group string,
	kind string,
	fetch botClanFetchFunc[T],
	handle func(context.Context, *platform.App, TrackedItem[T]) error,
	limiter *clashy.Limiter,
	rateLimit int,
) error {
	progressName := trackingProgressName(botClansDomainName, group)
	for {
		tags := domain.targetTags()
		if len(tags) == 0 {
			if err := sleepOrDone(ctx, time.Second); err != nil {
				return err
			}
			continue
		}
		if err := runBounded(ctx, platform.RequestConcurrency(rateLimit), tags, func(workerCtx context.Context, tag string) error {
			current, err := retryLimitedClashFetch(workerCtx, limiter, func(fetchCtx context.Context) (*T, error) {
				start := time.Now()
				current, err := fetch(fetchCtx, tag)
				app.Stats.RecordRequest(botClansDomainName, time.Since(start), err)
				return current, err
			})
			app.Stats.RecordTrackedTarget(progressName)
			if err != nil {
				app.Logger.Error("bot clan fetch failed", "group", group, "tag", tag, "err", err)
				app.Stats.SetReady(botClansDomainName, false, err.Error())
				if _, ok := platform.ClashFetchRetryPolicy(err); ok || workerCtx.Err() != nil {
					return err
				}
				return nil
			}
			var raw []byte
			if current != nil {
				raw = jsonBytes(current)
			}
			return handle(workerCtx, app, TrackedItem[T]{
				Group:   group,
				Kind:    kind,
				Tag:     tag,
				Current: current,
				Raw:     raw,
			})
		}); err != nil {
			return err
		}
	}
}

func fetchClan(app *platform.App) botClanFetchFunc[clashy.Clan] {
	return func(ctx context.Context, tag string) (*clashy.Clan, error) {
		return app.Clash.GetClan(ctx, tag)
	}
}

func fetchWar(app *platform.App) botClanFetchFunc[clashy.ClanWar] {
	return func(ctx context.Context, tag string) (*clashy.ClanWar, error) {
		return app.Clash.GetClanWar(ctx, tag)
	}
}

func fetchRaid(app *platform.App) botClanFetchFunc[clashy.RaidLogEntry] {
	return func(ctx context.Context, tag string) (*clashy.RaidLogEntry, error) {
		raids, err := app.Clash.GetRaidLog(ctx, tag, clashy.PageOptions{Limit: 1})
		if err != nil {
			return nil, err
		}
		if len(raids) == 0 {
			return &clashy.RaidLogEntry{}, nil
		}
		raid := raids[0]
		return &raid, nil
	}
}

func (d *botClansDomain) handleClanChange(ctx context.Context, app *platform.App, item TrackedItem[clashy.Clan]) error {
	if item.Current == nil {
		return nil
	}
	_, raw, hasPrevious, changed, err := loadBotClanSnapshotChange(ctx, d.snapshots, d.snapshotPrefix, "clan", item.Tag, *item.Current, item.Raw)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if hasPrevious {
		app.Stats.SetReady(botClansDomainName, true, "")
		if err := app.PublishEvent(ctx, platform.Event{
			Topic:   "clan",
			ClanTag: item.Tag,
			Value:   map[string]any{"type": "clan_update", "raw": string(raw)},
		}); err != nil {
			return err
		}
	}
	return d.snapshots.StoreRaw(ctx, botClanSnapshotKey(d.snapshotPrefix, "clan", item.Tag), raw)
}

func (d *botClansDomain) handleWarChange(ctx context.Context, app *platform.App, item TrackedItem[clashy.ClanWar]) error {
	if item.Current == nil {
		return nil
	}
	current := *item.Current
	if current.Type() == "cwl" {
		return d.handleCWLWarChange(ctx, app, item.Tag, current, item.Raw, nil)
	}
	if err := d.scheduleWarReminders(ctx, app, item.Tag, current, item.Raw, "war"); err != nil {
		return err
	}
	previous, raw, hasPrevious, changed, err := loadBotClanSnapshotChange(ctx, d.snapshots, d.snapshotPrefix, "war", item.Tag, current, item.Raw)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if hasPrevious {
		app.Stats.SetReady(botClansDomainName, true, "")
		if err := app.PublishEvent(ctx, platform.Event{
			Topic:   "war",
			ClanTag: item.Tag,
			Value:   map[string]any{"type": "war_update", "raw": string(raw)},
		}); err != nil {
			return err
		}
		if previous != nil {
			if !sameWarIdentity(*previous, current) {
				if err := app.PublishEvent(ctx, platform.Event{
					Topic:   "war",
					ClanTag: item.Tag,
					Value:   map[string]any{"type": "new_war", "new_war": string(raw), "clan_tag": item.Tag},
				}); err != nil {
					return err
				}
			} else {
				if attacks := newWarAttacks(*previous, current); len(attacks) > 0 {
					if err := app.PublishEvent(ctx, platform.Event{
						Topic:   "war",
						ClanTag: item.Tag,
						Value:   map[string]any{"type": "new_attacks", "war": string(raw), "attacks": attacks, "clan_tag": item.Tag},
					}); err != nil {
						return err
					}
				}
				if previous.State != current.State {
					if err := app.PublishEvent(ctx, platform.Event{
						Topic:   "war",
						ClanTag: item.Tag,
						Value:   map[string]any{"type": "war_state", "old_state": previous.State, "new_state": current.State, "war": string(raw), "clan_tag": item.Tag},
					}); err != nil {
						return err
					}
				}
			}
		}
	}
	return d.snapshots.StoreRaw(ctx, botClanSnapshotKey(d.snapshotPrefix, "war", item.Tag), raw)
}

func (d *botClansDomain) handleRaidChange(ctx context.Context, app *platform.App, item TrackedItem[clashy.RaidLogEntry]) error {
	if item.Current == nil {
		return nil
	}
	expiresAt, ok := capitalRaidCacheExpiry(*item.Current, time.Now().UTC())
	if !ok {
		return d.capitalRaids.Delete(ctx, item.Tag)
	}
	previous, raw, hasPrevious, changed, err := loadCapitalRaidCacheChange(ctx, d.capitalRaids, item.Tag, *item.Current, item.Raw)
	if err != nil {
		return err
	}
	if err := d.scheduleRaidReminders(ctx, app, item.Tag, *item.Current); err != nil {
		return err
	}
	if !changed {
		return d.capitalRaids.Replace(ctx, item.Tag, capitalRaidParticipantTags(*item.Current), raw, expiresAt)
	}
	if hasPrevious {
		app.Stats.RecordWrite(botClansDomainName, 1)
		app.Stats.SetReady(botClansDomainName, true, "")
		if err := app.PublishEvent(ctx, platform.Event{
			Topic:   "capital",
			ClanTag: item.Tag,
			Value:   map[string]any{"type": "raid_update", "raw": string(raw)},
		}); err != nil {
			return err
		}
		if previous != nil {
			if err := d.publishRaidDiffEvents(ctx, app, item.Tag, *previous, *item.Current, raw); err != nil {
				return err
			}
		}
	}
	return d.capitalRaids.Replace(ctx, item.Tag, capitalRaidParticipantTags(*item.Current), raw, expiresAt)
}

type capitalRaidCache interface {
	LoadRaw(context.Context, string) ([]byte, bool, error)
	Replace(context.Context, string, []string, []byte, time.Time) error
	Delete(context.Context, string) error
}

type valkeyCapitalRaidCache struct {
	client valkey.Client
	prefix string
}

type memoryCapitalRaidCacheEntry struct {
	compressed []byte
	expiresAt  time.Time
	members    map[string]struct{}
}

type memoryCapitalRaidMemberMapping struct {
	clanTag   string
	expiresAt time.Time
}

type memoryCapitalRaidCache struct {
	mu       sync.Mutex
	prefix   string
	now      func() time.Time
	entries  map[string]memoryCapitalRaidCacheEntry
	mappings map[string]memoryCapitalRaidMemberMapping
}

func newCapitalRaidCache(app *platform.App, prefix string) capitalRaidCache {
	if app.Valkey != nil {
		return valkeyCapitalRaidCache{client: app.Valkey, prefix: prefix}
	}
	return newMemoryCapitalRaidCache(prefix, time.Now)
}

func newMemoryCapitalRaidCache(prefix string, now func() time.Time) *memoryCapitalRaidCache {
	return &memoryCapitalRaidCache{
		prefix:   prefix,
		now:      now,
		entries:  make(map[string]memoryCapitalRaidCacheEntry),
		mappings: make(map[string]memoryCapitalRaidMemberMapping),
	}
}

func (s valkeyCapitalRaidCache) LoadRaw(ctx context.Context, clanTag string) ([]byte, bool, error) {
	value, err := s.client.Do(ctx, s.client.B().Get().Key(capitalRaidPayloadKey(s.prefix, clanTag)).Build()).ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	raw, err := utils.Decompress([]byte(value))
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

func (s valkeyCapitalRaidCache) Replace(
	ctx context.Context,
	clanTag string,
	participantTags []string,
	raw []byte,
	expiresAt time.Time,
) error {
	if !expiresAt.After(time.Now()) {
		return errors.New("capital raid cache expiry must be in the future")
	}
	args := []string{
		string(utils.Compress(raw)),
		clanTag,
		strconv.FormatInt(expiresAt.UTC().UnixMilli(), 10),
		capitalRaidMemberKeyPrefix(s.prefix),
	}
	args = append(args, participantTags...)
	return replaceCapitalRaidCacheScript.Exec(
		ctx,
		s.client,
		[]string{
			capitalRaidPayloadKey(s.prefix, clanTag),
			capitalRaidParticipantSetKey(s.prefix, clanTag),
		},
		args,
	).Error()
}

func (s valkeyCapitalRaidCache) Delete(ctx context.Context, clanTag string) error {
	return deleteCapitalRaidCacheScript.Exec(
		ctx,
		s.client,
		[]string{
			capitalRaidPayloadKey(s.prefix, clanTag),
			capitalRaidParticipantSetKey(s.prefix, clanTag),
		},
		[]string{clanTag, capitalRaidMemberKeyPrefix(s.prefix)},
	).Error()
}

func (s *memoryCapitalRaidCache) LoadRaw(_ context.Context, clanTag string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeExpiredLocked(s.now().UTC())
	entry, ok := s.entries[clanTag]
	if !ok {
		return nil, false, nil
	}
	raw, err := utils.Decompress(entry.compressed)
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

func (s *memoryCapitalRaidCache) Replace(
	_ context.Context,
	clanTag string,
	participantTags []string,
	raw []byte,
	expiresAt time.Time,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	expiresAt = expiresAt.UTC()
	s.removeExpiredLocked(now)
	if !expiresAt.After(now) {
		return errors.New("capital raid cache expiry must be in the future")
	}
	if previous, ok := s.entries[clanTag]; ok {
		for playerTag := range previous.members {
			if mapping, exists := s.mappings[playerTag]; exists && mapping.clanTag == clanTag {
				delete(s.mappings, playerTag)
			}
		}
	}
	members := make(map[string]struct{}, len(participantTags))
	for _, playerTag := range participantTags {
		members[playerTag] = struct{}{}
		s.mappings[playerTag] = memoryCapitalRaidMemberMapping{
			clanTag:   clanTag,
			expiresAt: expiresAt,
		}
	}
	s.entries[clanTag] = memoryCapitalRaidCacheEntry{
		compressed: utils.Compress(raw),
		expiresAt:  expiresAt,
		members:    members,
	}
	return nil
}

func (s *memoryCapitalRaidCache) Delete(_ context.Context, clanTag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.removeExpiredLocked(s.now().UTC())
	entry, ok := s.entries[clanTag]
	if !ok {
		return nil
	}
	delete(s.entries, clanTag)
	for playerTag := range entry.members {
		if mapping, exists := s.mappings[playerTag]; exists && mapping.clanTag == clanTag {
			delete(s.mappings, playerTag)
		}
	}
	return nil
}

func (s *memoryCapitalRaidCache) removeExpiredLocked(now time.Time) {
	for clanTag, entry := range s.entries {
		if entry.expiresAt.After(now) {
			continue
		}
		delete(s.entries, clanTag)
		for playerTag := range entry.members {
			if mapping, ok := s.mappings[playerTag]; ok && mapping.clanTag == clanTag {
				delete(s.mappings, playerTag)
			}
		}
	}
	for playerTag, mapping := range s.mappings {
		if !mapping.expiresAt.After(now) {
			delete(s.mappings, playerTag)
		}
	}
}

func capitalRaidPayloadKey(prefix, clanTag string) string {
	return botClanSnapshotKey(prefix, "raid", clanTag)
}

func capitalRaidParticipantSetKey(prefix, clanTag string) string {
	return prefix + "raid-members:" + clanTag
}

func capitalRaidMemberKeyPrefix(prefix string) string {
	return prefix + "raid-member:"
}

func capitalRaidMemberKey(prefix, playerTag string) string {
	return capitalRaidMemberKeyPrefix(prefix) + playerTag
}

func capitalRaidCacheExpiry(raid clashy.RaidLogEntry, now time.Time) (time.Time, bool) {
	if raid.EndTime == nil || raid.EndTime.Time.IsZero() {
		return time.Time{}, false
	}
	expiresAt := raid.EndTime.Time.UTC().Add(capitalRaidCacheGrace)
	if !expiresAt.After(now.UTC()) {
		return time.Time{}, false
	}
	return expiresAt, true
}

func capitalRaidParticipantTags(raid clashy.RaidLogEntry) []string {
	seen := make(map[string]struct{}, len(raid.Members))
	tags := make([]string, 0, len(raid.Members))
	for _, member := range raid.Members {
		if member.Tag == "" {
			continue
		}
		if _, ok := seen[member.Tag]; ok {
			continue
		}
		seen[member.Tag] = struct{}{}
		tags = append(tags, member.Tag)
	}
	return tags
}

func loadCapitalRaidCacheChange(
	ctx context.Context,
	cache capitalRaidCache,
	clanTag string,
	current clashy.RaidLogEntry,
	raw []byte,
) (*clashy.RaidLogEntry, []byte, bool, bool, error) {
	if len(raw) == 0 {
		raw = jsonBytes(current)
	}
	previousRaw, hasPrevious, err := cache.LoadRaw(ctx, clanTag)
	if err != nil {
		return nil, raw, false, false, err
	}
	if hasPrevious && bytes.Equal(previousRaw, raw) {
		return nil, raw, true, false, nil
	}
	var previous *clashy.RaidLogEntry
	if hasPrevious {
		var decoded clashy.RaidLogEntry
		if err := json.Unmarshal(previousRaw, &decoded); err == nil {
			previous = &decoded
		}
	}
	return previous, raw, hasPrevious, true, nil
}

type botClanSnapshotStore interface {
	LoadRaw(context.Context, string) ([]byte, bool, error)
	StoreRaw(context.Context, string, []byte) error
}

type valkeyBotClanSnapshotStore struct {
	client valkey.Client
}

type memoryBotClanSnapshotStore struct {
	mu     sync.Mutex
	values map[string][]byte
}

func newBotClanSnapshotStore(app *platform.App) botClanSnapshotStore {
	if app.Valkey != nil {
		return valkeyBotClanSnapshotStore{client: app.Valkey}
	}
	return &memoryBotClanSnapshotStore{values: make(map[string][]byte)}
}

func (s valkeyBotClanSnapshotStore) LoadRaw(ctx context.Context, key string) ([]byte, bool, error) {
	value, err := s.client.Do(ctx, s.client.B().Get().Key(key).Build()).ToString()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	raw, err := utils.Decompress([]byte(value))
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

func (s valkeyBotClanSnapshotStore) StoreRaw(ctx context.Context, key string, raw []byte) error {
	return s.client.Do(ctx, s.client.B().Set().
		Key(key).
		Value(valkey.BinaryString(utils.Compress(raw))).
		Build(),
	).Error()
}

func (s *memoryBotClanSnapshotStore) LoadRaw(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.values[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), raw...), true, nil
}

func (s *memoryBotClanSnapshotStore) StoreRaw(_ context.Context, key string, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = append([]byte(nil), raw...)
	return nil
}

func botClanSnapshotKey(prefix, kind, tag string) string {
	return prefix + kind + ":" + tag
}

func loadBotClanSnapshotChange[T any](
	ctx context.Context,
	store botClanSnapshotStore,
	prefix string,
	kind string,
	tag string,
	current T,
	raw []byte,
) (*T, []byte, bool, bool, error) {
	if len(raw) == 0 {
		raw = jsonBytes(current)
	}
	key := botClanSnapshotKey(prefix, kind, tag)
	previousRaw, hasPrevious, err := store.LoadRaw(ctx, key)
	if err != nil {
		return nil, raw, false, false, err
	}
	if hasPrevious && bytes.Equal(previousRaw, raw) {
		return nil, raw, true, false, nil
	}
	var previous *T
	if hasPrevious {
		var decoded T
		if err := json.Unmarshal(previousRaw, &decoded); err == nil {
			previous = &decoded
		}
	}
	return previous, raw, hasPrevious, true, nil
}

func loadBotClanSnapshot[T any](ctx context.Context, store botClanSnapshotStore, prefix, kind, tag string) (*T, []byte, bool, error) {
	raw, ok, err := store.LoadRaw(ctx, botClanSnapshotKey(prefix, kind, tag))
	if err != nil || !ok {
		return nil, raw, ok, err
	}
	var value T
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, raw, true, err
	}
	return &value, raw, true, nil
}

func sameWarIdentity(left, right clashy.ClanWar) bool {
	return warIdentity(left) == warIdentity(right)
}

func warIdentity(war clashy.ClanWar) string {
	if war.PreparationStartTime == nil {
		return ""
	}
	clanTag, opponentTag := "", ""
	if war.Clan != nil {
		clanTag = war.Clan.Tag
	}
	if war.Opponent != nil {
		opponentTag = war.Opponent.Tag
	}
	return clanTag + ":" + opponentTag + ":" + war.PreparationStartTime.RawTime
}

func newWarAttacks(previous, current clashy.ClanWar) []clashy.WarAttack {
	seen := make(map[string]struct{})
	for _, attack := range previous.Attacks() {
		seen[warAttackKey(attack)] = struct{}{}
	}
	var out []clashy.WarAttack
	for _, attack := range current.Attacks() {
		if _, ok := seen[warAttackKey(attack)]; !ok {
			out = append(out, attack)
		}
	}
	return out
}

func warAttackKey(attack clashy.WarAttack) string {
	return attack.AttackerTag + ":" + attack.DefenderTag + ":" + strconv.Itoa(attack.Order)
}

func (d *botClansDomain) scheduleWarReminders(ctx context.Context, app *platform.App, clanTag string, war clashy.ClanWar, raw []byte, kind string) error {
	if war.EndTime == nil || war.EndTime.Time.IsZero() || app.Scheduler == nil || d.store == nil {
		return nil
	}
	if len(raw) == 0 {
		raw = jsonBytes(war)
	}
	reminders, err := d.loadWarReminders(ctx, clanTag)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, reminder := range reminders {
		runAt := war.EndTime.Time.UTC().Add(-time.Duration(reminder.MinutesRemaining) * time.Minute)
		if !runAt.After(now) {
			continue
		}
		jobID := "war_end:" + kind + ":" + clanTag + ":" + reminder.TriggerTime + ":" + war.EndTime.RawTime
		if !d.scheduleOnce(jobID) {
			continue
		}
		reminderTime := reminder.TriggerTime
		raw := append([]byte(nil), raw...)
		d.scheduleReminderDelivery(app, jobID, runAt, func(ctx context.Context) error {
			return app.PublishEvent(ctx, platform.Event{
				Topic:   "reminder",
				ClanTag: clanTag,
				Value: map[string]any{
					"type":     "war",
					"war_type": kind,
					"time":     reminderTime,
					"data":     string(raw),
				},
			})
		})
	}
	return nil
}

func (d *botClansDomain) scheduleRaidReminders(ctx context.Context, app *platform.App, clanTag string, raid clashy.RaidLogEntry) error {
	if raid.EndTime == nil || raid.EndTime.Time.IsZero() || app.Scheduler == nil || d.store == nil {
		return nil
	}
	reminders, err := d.loadRaidReminders(ctx, clanTag)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, reminder := range reminders {
		runAt := raid.EndTime.Time.UTC().Add(-time.Duration(reminder.MinutesRemaining) * time.Minute)
		if !runAt.After(now) {
			continue
		}
		jobID := "raid_end:" + clanTag + ":" + reminder.ID + ":" + raid.EndTime.RawTime
		if !d.scheduleOnce(jobID) {
			continue
		}
		reminder := reminder
		d.scheduleReminderDelivery(app, jobID, runAt, func(ctx context.Context) error {
			return d.publishRaidReminder(ctx, app, clanTag, reminder)
		})
	}
	return nil
}

func (d *botClansDomain) scheduleOnce(jobID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.scheduled[jobID]; exists {
		return false
	}
	d.scheduled[jobID] = struct{}{}
	return true
}

func (d *botClansDomain) forgetScheduled(jobID string) {
	d.mu.Lock()
	delete(d.scheduled, jobID)
	d.mu.Unlock()
}

func (d *botClansDomain) scheduleReminderDelivery(
	app *platform.App,
	jobID string,
	when time.Time,
	publish func(context.Context) error,
) {
	app.Scheduler.Schedule(platform.Job{
		ID:   jobID,
		When: when,
		Run: func(ctx context.Context) {
			if err := publish(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				app.Logger.Error("bot clan reminder delivery failed", "job_id", jobID, "err", err)
				app.Stats.SetReady(botClansDomainName, false, err.Error())
				d.scheduleReminderDelivery(app, jobID, time.Now().UTC().Add(botClanReminderRetryDelay), publish)
				return
			}
			d.forgetScheduled(jobID)
		},
	})
}

func (d *botClansDomain) loadWarReminders(ctx context.Context, clanTag string) ([]warReminder, error) {
	now := time.Now()
	d.reminderMu.Lock()
	if cached, ok := d.warReminders[clanTag]; ok && now.Sub(cached.loadedAt) < botClanReminderTTL {
		values := append([]warReminder(nil), cached.values...)
		d.reminderMu.Unlock()
		return values, nil
	}
	d.reminderMu.Unlock()
	values, err := d.store.ListWarReminders(ctx, clanTag)
	if err != nil {
		return nil, err
	}
	d.reminderMu.Lock()
	d.warReminders[clanTag] = cachedWarReminders{loadedAt: now, values: append([]warReminder(nil), values...)}
	d.reminderMu.Unlock()
	return values, nil
}

func (d *botClansDomain) loadRaidReminders(ctx context.Context, clanTag string) ([]raidReminder, error) {
	now := time.Now()
	d.reminderMu.Lock()
	if cached, ok := d.raidReminders[clanTag]; ok && now.Sub(cached.loadedAt) < botClanReminderTTL {
		values := append([]raidReminder(nil), cached.values...)
		d.reminderMu.Unlock()
		return values, nil
	}
	d.reminderMu.Unlock()
	values, err := d.store.ListRaidReminders(ctx, clanTag)
	if err != nil {
		return nil, err
	}
	d.reminderMu.Lock()
	d.raidReminders[clanTag] = cachedRaidReminders{loadedAt: now, values: append([]raidReminder(nil), values...)}
	d.reminderMu.Unlock()
	return values, nil
}

func (d *botClansDomain) publishRaidReminder(ctx context.Context, app *platform.App, clanTag string, reminder raidReminder) error {
	clan, _, ok, err := loadBotClanSnapshot[clashy.Clan](ctx, d.snapshots, d.snapshotPrefix, "clan", clanTag)
	if err != nil || !ok {
		return err
	}
	raw, ok, err := d.capitalRaids.LoadRaw(ctx, clanTag)
	if err != nil || !ok {
		return err
	}
	var raid clashy.RaidLogEntry
	if err := json.Unmarshal(raw, &raid); err != nil {
		return err
	}
	missing := raidMissingMembers(*clan, raid, reminder)
	if len(missing) == 0 {
		return nil
	}
	return app.PublishEvent(ctx, platform.Event{
		Topic:   "reminder",
		ClanTag: clanTag,
		Value: map[string]any{
			"type":          "raid",
			"clan_data":     clan,
			"raid_data":     &raid,
			"reminder_data": reminder.eventData(),
			"missing":       missing,
		},
	})
}

func (d *botClansDomain) publishRaidDiffEvents(ctx context.Context, app *platform.App, clanTag string, previous, current clashy.RaidLogEntry, raw []byte) error {
	if previous.State != current.State {
		if err := app.PublishEvent(ctx, platform.Event{
			Topic:   "capital",
			ClanTag: clanTag,
			Value:   map[string]any{"type": "raid_state", "old_raid": previous, "raid": string(raw), "clan_tag": clanTag},
		}); err != nil {
			return err
		}
	}
	if attacked := changedRaidMemberAttacks(previous, current); len(attacked) > 0 {
		return app.PublishEvent(ctx, platform.Event{
			Topic:   "capital",
			ClanTag: clanTag,
			Value:   map[string]any{"type": "raid_attacks", "attacked": attacked, "raid": string(raw), "old_raid": previous, "clan_tag": clanTag},
		})
	}
	return nil
}

func changedRaidMemberAttacks(previous, current clashy.RaidLogEntry) []string {
	previousMembers := make(map[string]int)
	for _, member := range previous.Members {
		previousMembers[member.Tag] = member.AttackCount
	}
	var out []string
	for _, member := range current.Members {
		if previousMembers[member.Tag] != member.AttackCount {
			out = append(out, member.Tag)
		}
	}
	return out
}

func raidMissingMembers(clan clashy.Clan, raid clashy.RaidLogEntry, reminder raidReminder) []map[string]any {
	threshold := reminder.AttackThreshold
	roles := reminder.Roles
	townHalls := reminder.TownHalls
	raidMembers := make(map[string]clashy.RaidMember)
	for _, member := range raid.Members {
		raidMembers[member.Tag] = member
	}
	var missing []map[string]any
	for _, member := range clan.Members {
		if !clanMemberEligible(member, roles, townHalls) {
			continue
		}
		raidMember, ok := raidMembers[member.Tag]
		attackLimit := 5
		attacks := 0
		if ok {
			attackLimit = raidMember.AttackLimit + raidMember.BonusAttackLimit
			attacks = raidMember.AttackCount
		}
		if attacks >= attackLimit-threshold {
			continue
		}
		missing = append(missing, map[string]any{
			"name":          member.Name,
			"tag":           member.Tag,
			"townhall":      member.TownHall,
			"role":          member.Role,
			"attacks":       attacks,
			"total_attacks": attackLimit,
		})
	}
	return missing
}

func clanMemberEligible(member clashy.ClanMember, roles []string, townHalls []int) bool {
	if len(roles) > 0 && !stringContains(roles, string(member.Role)) {
		return false
	}
	if len(townHalls) > 0 && !intContains(townHalls, member.TownHall) {
		return false
	}
	return true
}

type botCWLState struct {
	Season           string `json:"season,omitempty"`
	GroupState       string `json:"group_state,omitempty"`
	GroupHash        uint64 `json:"group_hash,omitempty"`
	CurrentRoundHash string `json:"current_round_hash,omitempty"`
	CurrentWarTag    string `json:"current_war_tag,omitempty"`
	Ended            bool   `json:"ended,omitempty"`
	NoSpin           bool   `json:"no_spin,omitempty"`
}

func (d *botClansDomain) runCWLLoop(ctx context.Context, app *platform.App, limiter *clashy.Limiter) error {
	interval := time.Duration(app.Config.WarCWLSyncSeconds) * time.Second
	if interval <= 0 {
		interval = 3 * time.Minute
	}
	for {
		start := time.Now()
		err := d.runCWLCycle(ctx, app, limiter)
		app.Stats.RecordProcess(botClansDomainName, time.Since(start))
		if err != nil {
			return err
		}
		if err := sleepOrDone(ctx, interval); err != nil {
			return err
		}
	}
}

func (d *botClansDomain) runCWLCycle(ctx context.Context, app *platform.App, limiter *clashy.Limiter) error {
	tags := d.targetTags()
	now := time.Now().UTC()
	for _, tag := range tags {
		if err := d.processCWLTarget(ctx, app, limiter, tag, now); err != nil {
			return err
		}
	}
	return nil
}

func (d *botClansDomain) processCWLTarget(ctx context.Context, app *platform.App, limiter *clashy.Limiter, tag string, now time.Time) error {
	state, _, ok, err := loadBotClanSnapshot[botCWLState](ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag)
	if err != nil {
		return err
	}
	if !ok && !cwlDiscoveryWindow(now) {
		return nil
	}
	if ok && !shouldPollCWL(now, *state) {
		return nil
	}
	group, raw, err := d.fetchCWLGroup(ctx, app, limiter, tag)
	if err != nil || group == nil {
		return err
	}
	currentState := cwlStateFromGroup(group, raw, now)
	if groupNotInThisSeason(group, now) {
		if now.Day() >= 3 {
			currentState.NoSpin = true
			return storeBotClanValue(ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag, currentState)
		}
		return nil
	}
	if err := d.handleCWLGroupChange(ctx, app, tag, group, raw, state, currentState); err != nil {
		return err
	}
	if currentState.Ended {
		return storeBotClanValue(ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag, currentState)
	}
	roundTags, roundHash := latestCWLWarTags(group)
	if roundHash != "" && (!ok || state.CurrentRoundHash != roundHash) {
		warTag, war, warRaw, err := d.findClanCWLWar(ctx, app, limiter, tag, roundTags)
		if err != nil {
			return err
		}
		if warTag != "" {
			currentState.CurrentRoundHash = roundHash
			currentState.CurrentWarTag = warTag
		} else if ok {
			currentState.CurrentRoundHash = state.CurrentRoundHash
			currentState.CurrentWarTag = state.CurrentWarTag
		}
		if war != nil {
			if err := d.handleCWLWarChange(ctx, app, tag, *war, warRaw, group); err != nil {
				return err
			}
		}
	} else if ok && state.CurrentWarTag != "" {
		war, warRaw, err := d.fetchCWLWar(ctx, app, limiter, state.CurrentWarTag)
		if err != nil {
			return err
		}
		if war != nil {
			currentState.CurrentRoundHash = state.CurrentRoundHash
			currentState.CurrentWarTag = state.CurrentWarTag
			if err := d.handleCWLWarChange(ctx, app, tag, *war, warRaw, group); err != nil {
				return err
			}
		}
	}
	return storeBotClanValue(ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag, currentState)
}

func (d *botClansDomain) fetchCWLGroup(ctx context.Context, app *platform.App, limiter *clashy.Limiter, tag string) (*clashy.ClanWarLeagueGroup, []byte, error) {
	group, err := retryLimitedClashFetch(ctx, limiter, func(fetchCtx context.Context) (*clashy.ClanWarLeagueGroup, error) {
		start := time.Now()
		group, err := app.Clash.GetLeagueGroup(fetchCtx, tag)
		app.Stats.RecordRequest(botClansDomainName, time.Since(start), err)
		return group, err
	})
	if err != nil {
		var notFound *clashy.NotFound
		if errors.As(err, &notFound) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	return group, jsonBytes(group), nil
}

func (d *botClansDomain) fetchCWLWar(ctx context.Context, app *platform.App, limiter *clashy.Limiter, warTag string) (*clashy.ClanWar, []byte, error) {
	wars, err := retryLimitedClashFetch(ctx, limiter, func(fetchCtx context.Context) ([]clashy.ClanWar, error) {
		start := time.Now()
		wars, err := app.Clash.GetLeagueWars(fetchCtx, []string{warTag})
		app.Stats.RecordRequest(botClansDomainName, time.Since(start), err)
		return wars, err
	})
	if err != nil {
		var notFound *clashy.NotFound
		if errors.As(err, &notFound) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	if len(wars) == 0 {
		return nil, nil, nil
	}
	return &wars[0], jsonBytes(wars[0]), nil
}

func (d *botClansDomain) findClanCWLWar(ctx context.Context, app *platform.App, limiter *clashy.Limiter, clanTag string, warTags []string) (string, *clashy.ClanWar, []byte, error) {
	for _, warTag := range warTags {
		war, raw, err := d.fetchCWLWar(ctx, app, limiter, warTag)
		if err != nil {
			return "", nil, nil, err
		}
		if war == nil {
			continue
		}
		if warContainsClan(*war, clanTag) {
			return warTag, war, raw, nil
		}
	}
	return "", nil, nil, nil
}

func (d *botClansDomain) handleCWLGroupChange(ctx context.Context, app *platform.App, clanTag string, group *clashy.ClanWarLeagueGroup, raw []byte, previous *botCWLState, current botCWLState) error {
	if previous == nil || previous.GroupHash == current.GroupHash {
		return nil
	}
	app.Stats.SetReady(botClansDomainName, true, "")
	return app.PublishEvent(ctx, platform.Event{
		Topic:   "cwl",
		ClanTag: clanTag,
		Value: map[string]any{
			"type":         "cwl_group_update",
			"clan_tag":     clanTag,
			"state":        group.State,
			"season":       group.Season,
			"league_group": string(raw),
		},
	})
}

func (d *botClansDomain) handleCWLWarChange(ctx context.Context, app *platform.App, clanTag string, war clashy.ClanWar, raw []byte, group *clashy.ClanWarLeagueGroup) error {
	if err := d.scheduleWarReminders(ctx, app, clanTag, war, raw, "cwl"); err != nil {
		return err
	}
	previous, raw, hasPrevious, changed, err := loadBotClanSnapshotChange(ctx, d.snapshots, d.snapshotPrefix, "cwlwar", clanTag, war, raw)
	if err != nil || !changed {
		return err
	}
	if hasPrevious {
		app.Stats.SetReady(botClansDomainName, true, "")
		if err := app.PublishEvent(ctx, platform.Event{
			Topic:   "cwl",
			ClanTag: clanTag,
			Value:   map[string]any{"type": "cwl_war_update", "clan_tag": clanTag, "war": string(raw), "league_group": rawCWLGroup(group)},
		}); err != nil {
			return err
		}
		if previous != nil {
			if lineup := cwlLineupChanges(*previous, war); cwlLineupChanged(lineup) {
				lineup["clan_tag"] = clanTag
				lineup["league_group"] = rawCWLGroup(group)
				if err := app.PublishEvent(ctx, platform.Event{
					Topic:   "cwl",
					ClanTag: clanTag,
					Value:   lineup,
				}); err != nil {
					return err
				}
			}
			if attacks := newWarAttacks(*previous, war); len(attacks) > 0 {
				if err := app.PublishEvent(ctx, platform.Event{
					Topic:   "cwl",
					ClanTag: clanTag,
					Value:   map[string]any{"type": "cwl_new_attacks", "clan_tag": clanTag, "war": string(raw), "attacks": attacks, "league_group": rawCWLGroup(group)},
				}); err != nil {
					return err
				}
			}
		}
	}
	return d.snapshots.StoreRaw(ctx, botClanSnapshotKey(d.snapshotPrefix, "cwlwar", clanTag), raw)
}

func storeBotClanValue(ctx context.Context, store botClanSnapshotStore, prefix, kind, tag string, value any) error {
	return store.StoreRaw(ctx, botClanSnapshotKey(prefix, kind, tag), jsonBytes(value))
}

func shouldPollCWL(now time.Time, state botCWLState) bool {
	season := utils.CurrentSeason(now)
	if state.Season == season && (state.Ended || state.NoSpin) {
		return false
	}
	if cwlDiscoveryWindow(now) {
		return true
	}
	return state.Season == season && state.GroupState != "" && state.GroupState != "ended"
}

func cwlDiscoveryWindow(now time.Time) bool {
	day := now.UTC().Day()
	return day >= 1 && day <= 13
}

func cwlStateFromGroup(group *clashy.ClanWarLeagueGroup, raw []byte, now time.Time) botCWLState {
	state := botCWLState{Season: utils.CurrentSeason(now)}
	if group == nil {
		return state
	}
	if group.Season != "" {
		state.Season = group.Season
	}
	state.GroupState = group.State
	state.GroupHash = hashBytes(raw)
	state.Ended = group.State == "ended"
	return state
}

func groupNotInThisSeason(group *clashy.ClanWarLeagueGroup, now time.Time) bool {
	if group == nil {
		return true
	}
	if group.State == "notInWar" || group.State == "groupNotFound" {
		return true
	}
	return group.Season != "" && group.Season != utils.CurrentSeason(now)
}

func latestCWLWarTags(group *clashy.ClanWarLeagueGroup) ([]string, string) {
	if group == nil {
		return nil, ""
	}
	for i := len(group.Rounds) - 1; i >= 0; i-- {
		var tags []string
		for _, tag := range group.Rounds[i].WarTags {
			if tag != "" && tag != "#0" {
				tags = append(tags, tag)
			}
		}
		if len(tags) > 0 {
			return tags, strings.Join(tags, ",")
		}
	}
	return nil, ""
}

func warContainsClan(war clashy.ClanWar, clanTag string) bool {
	if war.Clan != nil && war.Clan.Tag == clanTag {
		return true
	}
	return war.Opponent != nil && war.Opponent.Tag == clanTag
}

func rawCWLGroup(group *clashy.ClanWarLeagueGroup) string {
	if group == nil {
		return ""
	}
	return string(jsonBytes(group))
}

func cwlLineupChanges(previous, current clashy.ClanWar) map[string]any {
	changes := map[string]any{
		"type":             "cwl_lineup_change",
		"clan_tag":         "",
		"war":              string(jsonBytes(current)),
		"added":            []clashy.ClanWarMember{},
		"removed":          []clashy.ClanWarMember{},
		"opponent_added":   []clashy.ClanWarMember{},
		"opponent_removed": []clashy.ClanWarMember{},
	}
	if current.Clan != nil {
		changes["clan_tag"] = current.Clan.Tag
	}
	if previous.Clan != nil && current.Clan != nil {
		added, removed := warMemberDiff(previous.Clan.Members, current.Clan.Members)
		changes["added"] = added
		changes["removed"] = removed
	}
	if previous.Opponent != nil && current.Opponent != nil {
		added, removed := warMemberDiff(previous.Opponent.Members, current.Opponent.Members)
		changes["opponent_added"] = added
		changes["opponent_removed"] = removed
	}
	return changes
}

func cwlLineupChanged(value map[string]any) bool {
	for _, key := range []string{"added", "removed", "opponent_added", "opponent_removed"} {
		if members, _ := value[key].([]clashy.ClanWarMember); len(members) > 0 {
			return true
		}
	}
	return false
}

func warMemberDiff(previous, current []clashy.ClanWarMember) ([]clashy.ClanWarMember, []clashy.ClanWarMember) {
	previousByTag := make(map[string]clashy.ClanWarMember)
	currentByTag := make(map[string]clashy.ClanWarMember)
	for _, member := range previous {
		previousByTag[member.Tag] = member
	}
	for _, member := range current {
		currentByTag[member.Tag] = member
	}
	var added []clashy.ClanWarMember
	for _, member := range current {
		if _, ok := previousByTag[member.Tag]; !ok {
			added = append(added, member)
		}
	}
	var removed []clashy.ClanWarMember
	for _, member := range previous {
		if _, ok := currentByTag[member.Tag]; !ok {
			removed = append(removed, member)
		}
	}
	return added, removed
}

func hashBytes(raw []byte) uint64 {
	var hash uint64 = 1469598103934665603
	for _, value := range raw {
		hash ^= uint64(value)
		hash *= 1099511628211
	}
	return hash
}

func stringContains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func intContains(values []int, target int) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func rawPayload(raw []byte, fallback any) string {
	if len(raw) > 0 {
		return string(raw)
	}
	return string(jsonBytes(fallback))
}

func jsonBytes(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}
