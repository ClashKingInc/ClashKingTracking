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

type trackedClanFetchFunc[T any] func(context.Context, string) (*T, error)

type TrackedItem[T any] struct {
	Group   string
	Kind    string
	Tag     string
	Current *T
	Raw     []byte
}

const (
	trackedClansDomainName = "trackedclans"
	capitalRaidCacheGrace  = 10 * time.Minute
	cwlGroupRefreshPeriod  = 15 * time.Minute
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

type trackedClansDomain struct {
	targetsMu        sync.RWMutex
	targets          []trackedClanTarget
	activeCWLMu      sync.RWMutex
	activeCWL        map[string]struct{}
	snapshots        trackedClanSnapshotStore
	snapshotPrefix   string
	cwlStateSnapshot string
	store            trackedClanStore
	capitalRaids     capitalRaidCache
}

type trackedClanTarget struct {
	Tag              string
	ClanEvents       bool
	WarEvents        bool
	PublishWarEvents bool
}

func NewTrackedClansDomain() platform.Domain {
	return &trackedClansDomain{
		activeCWL: make(map[string]struct{}),
		snapshots: &memoryTrackedClanSnapshotStore{
			values: make(map[string][]byte),
		},
		capitalRaids: newMemoryCapitalRaidCache("trackedclans:snapshot:", time.Now),
	}
}

func (d *trackedClansDomain) Name() string { return trackedClansDomainName }

func (d *trackedClansDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateTrackedClansConfig(app.Config); err != nil {
		return err
	}
	store, err := newTrackedClanStore(ctx, app)
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
	d.snapshots = newTrackedClanSnapshotStore(app)
	d.snapshotPrefix = app.Config.TrackedClanSnapshotPrefix
	d.cwlStateSnapshot = app.Config.TrackedClanCWLStateSnapshot
	d.capitalRaids = newCapitalRaidCache(app, d.snapshotPrefix)

	rateLimit := app.Config.TrackedClanRequestsPerSecond
	limiter, err := newTrackingLimiter(rateLimit)
	if err != nil {
		return err
	}
	trackerCtx, stopTrackers := context.WithCancel(ctx)
	defer stopTrackers()
	errCh := make(chan error, 3)
	go func() {
		errCh <- runTrackedClanTracker(trackerCtx, app, d, "clans", "clan", fetchClan(app), d.handleClanChange, limiter, rateLimit)
	}()
	go func() {
		errCh <- runTrackedClanTracker(trackerCtx, app, d, "wars", "war", fetchWar(app), d.handleWarChange, limiter, rateLimit)
	}()
	go func() {
		errCh <- d.runCWLLoop(trackerCtx, app, limiter)
	}()
	go d.runTargetRefreshLoop(trackerCtx, app, store)
	firstErr := <-errCh
	stopTrackers()
	for range 2 {
		<-errCh
	}
	return firstErr
}

func validateTrackedClansConfig(cfg platform.Config) error {
	if cfg.TrackedClanRequestsPerSecond <= 0 {
		return errors.New("trackedclans.requests_per_second must be greater than zero")
	}
	if cfg.TrackedClanTargetRefreshSeconds <= 0 {
		return errors.New("trackedclans.target_refresh_seconds must be greater than zero")
	}
	if cfg.TrackedClanSnapshotPrefix == "" {
		return errors.New("trackedclans.snapshot_prefix is required")
	}
	if cfg.TrackedClanCWLStateSnapshot == "" {
		return errors.New("trackedclans.cwl_state_snapshot is required")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_* connection variables are required for trackedclans server_clans targets")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.ValkeyAddr == "" {
		return errors.New("valkey_addr is required for trackedclans snapshots")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.EventStreamName == "" {
		return errors.New("events.stream is required for trackedclans event publishing")
	}
	return nil
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
		"id":                r.ID,
		"server_id":         r.ServerID,
		"type_name":         "Clan Capital",
		"clan_tag":          r.ClanTag,
		"channel_id":        r.ChannelID,
		"trigger_time":      r.TriggerTime,
		"minutes_remaining": r.MinutesRemaining,
		"custom_text":       r.CustomText,
		"town_halls":        r.TownHalls,
		"roles":             r.Roles,
		"war_types":         r.WarTypes,
		"trigger_threshold": r.AttackThreshold,
	}
}

type trackedClanStore interface {
	ListTargets(context.Context) ([]trackedClanTarget, error)
	UpsertCurrentWar(context.Context, string, clashy.ClanWar, string) (string, error)
	Close()
}

type timescaleTrackedClanStore struct {
	pool *pgxpool.Pool
}

func newTrackedClanStore(ctx context.Context, app *platform.App) (trackedClanStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return memoryTrackedClanStore{}, nil
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return nil, err
	}
	return &timescaleTrackedClanStore{pool: pool}, nil
}

func (s *timescaleTrackedClanStore) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

func (s *timescaleTrackedClanStore) UpsertCurrentWar(ctx context.Context, sourceTag string, war clashy.ClanWar, warTag string) (string, error) {
	ingest, err := buildWarIngest(war, sourceTag, false, warTag, "", "")
	if err != nil || len(ingest.Schedules) == 0 {
		return "", err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if err := upsertWarSchedules(ctx, tx, ingest.Schedules); err != nil {
		return "", err
	}
	if err := upsertPlayerTimers(ctx, tx, ingest.PlayerTimers); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return ingest.Schedules[0].ScheduleKey, nil
}

func (s *timescaleTrackedClanStore) ListTargets(ctx context.Context) ([]trackedClanTarget, error) {
	rows, err := s.pool.Query(ctx, `
			SELECT tag,
			       bool_or(kind = 'clan') AS clan_events,
			       bool_or(kind IN ('war_event', 'war_reminder')) AS war_events,
			       bool_or(kind = 'war_event') AS publish_war_events
			FROM (
				SELECT log.clan_tag AS tag,
				       CASE WHEN log.type IN ('join_log', 'leave_log') THEN 'clan' ELSE 'war_event' END AS kind
				FROM server_logs log
				JOIN servers server ON server.id = log.server_id
				WHERE server.last_command_at >= now() - interval '90 days'
				  AND log.disabled = false
				  AND log.clan_tag <> ''
				  AND log.type IN ('join_log', 'leave_log', 'war_log', 'war_panel')
				UNION
				SELECT reminder.clan_tag AS tag, 'war_reminder' AS kind
				FROM reminders reminder
				JOIN servers server ON server.id = reminder.server_id
				WHERE server.last_command_at >= now() - interval '90 days'
				  AND reminder.type_name = 'War'
				  AND reminder.clan_tag <> ''
			) targets
			GROUP BY tag
			ORDER BY tag
		`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []trackedClanTarget
	for rows.Next() {
		var target trackedClanTarget
		if err := rows.Scan(&target.Tag, &target.ClanEvents, &target.WarEvents, &target.PublishWarEvents); err != nil {
			return nil, err
		}
		out = append(out, target)
	}
	return out, rows.Err()
}

type memoryTrackedClanStore struct {
	tags []string
}

func (s memoryTrackedClanStore) Close() {}

func (s memoryTrackedClanStore) ListTargets(context.Context) ([]trackedClanTarget, error) {
	out := make([]trackedClanTarget, 0, len(s.tags))
	for _, tag := range s.tags {
		out = append(out, trackedClanTarget{Tag: tag, ClanEvents: true, WarEvents: true, PublishWarEvents: true})
	}
	return out, nil
}

func (memoryTrackedClanStore) UpsertCurrentWar(_ context.Context, sourceTag string, war clashy.ClanWar, warTag string) (string, error) {
	ingest, err := buildWarIngest(war, sourceTag, false, warTag, "", "")
	if err != nil || len(ingest.Schedules) == 0 {
		return "", err
	}
	return ingest.Schedules[0].ScheduleKey, nil
}

func (d *trackedClansDomain) targetTags(group string) []string {
	d.targetsMu.RLock()
	defer d.targetsMu.RUnlock()
	var out []string
	for _, target := range d.targets {
		if (group == "clans" && target.ClanEvents) || (group == "wars" && target.WarEvents) {
			out = append(out, target.Tag)
		}
	}
	return out
}

func (d *trackedClansDomain) replaceTargets(app *platform.App, targets []trackedClanTarget) {
	d.targetsMu.Lock()
	d.targets = append(d.targets[:0], targets...)
	d.targetsMu.Unlock()
	app.Stats.SetTrackingTargets(trackingProgressName(trackedClansDomainName, "clans"), len(d.targetTags("clans")))
	app.Stats.SetTrackingTargets(trackingProgressName(trackedClansDomainName, "wars"), len(d.targetTags("wars")))
}

func (d *trackedClansDomain) publishesWarEvents(tag string) bool {
	d.targetsMu.RLock()
	defer d.targetsMu.RUnlock()
	for _, target := range d.targets {
		if target.Tag == tag {
			return target.PublishWarEvents
		}
	}
	return false
}

func (d *trackedClansDomain) setCWLActive(tag string, active bool) {
	d.activeCWLMu.Lock()
	defer d.activeCWLMu.Unlock()
	if d.activeCWL == nil {
		d.activeCWL = make(map[string]struct{})
	}
	if active {
		d.activeCWL[tag] = struct{}{}
		return
	}
	delete(d.activeCWL, tag)
}

func (d *trackedClansDomain) isCWLActive(tag string) bool {
	d.activeCWLMu.RLock()
	defer d.activeCWLMu.RUnlock()
	_, active := d.activeCWL[tag]
	return active
}

func (d *trackedClansDomain) runTargetRefreshLoop(ctx context.Context, app *platform.App, source trackedClanStore) {
	interval := time.Duration(app.Config.TrackedClanTargetRefreshSeconds) * time.Second
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
			app.Logger.Error("tracked clan target refresh failed", "err", err)
			app.Stats.SetReady(trackedClansDomainName, false, err.Error())
			continue
		}
		d.replaceTargets(app, targets)
		app.Logger.Info("refreshed tracked clan targets", "count", len(targets))
	}
}

func runTrackedClanTracker[T any](
	ctx context.Context,
	app *platform.App,
	domain *trackedClansDomain,
	group string,
	kind string,
	fetch trackedClanFetchFunc[T],
	handle func(context.Context, *platform.App, TrackedItem[T]) error,
	limiter *clashy.Limiter,
	rateLimit int,
) error {
	progressName := trackingProgressName(trackedClansDomainName, group)
	for {
		tags := domain.targetTags(group)
		if len(tags) == 0 {
			if err := sleepOrDone(ctx, time.Second); err != nil {
				return err
			}
			continue
		}
		if err := runBounded(ctx, platform.RequestConcurrency(rateLimit), tags, func(workerCtx context.Context, tag string) error {
			// The regular current-war endpoint cannot return a useful regular war
			// while this clan is in CWL. The tagged CWL loop owns it until the
			// league finishes, avoiding one guaranteed-empty request per pass.
			if group == "wars" && domain.isCWLActive(tag) {
				app.Stats.RecordTrackedTarget(progressName)
				return nil
			}
			current, err := retryLimitedClashFetch(workerCtx, app, limiter, func(fetchCtx context.Context) (*T, error) {
				start := time.Now()
				current, err := fetch(fetchCtx, tag)
				app.Stats.RecordRequest(progressName, time.Since(start), err)
				return current, err
			})
			app.Stats.RecordTrackedTarget(progressName)
			if err != nil {
				if workerCtx.Err() != nil {
					return workerCtx.Err()
				}
				// A single private, throttled, or temporarily unavailable clan is
				// retried on the next pass. Process-wide availability failures remain
				// held at the shared gate, while SQL/snapshot/event errors from handle
				// still stop the process.
				app.Logger.Error("tracked clan fetch failed", "group", group, "tag", tag, "err", err)
				app.Stats.SetReady(trackedClansDomainName, false, err.Error())
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

func fetchClan(app *platform.App) trackedClanFetchFunc[clashy.Clan] {
	return func(ctx context.Context, tag string) (*clashy.Clan, error) {
		return app.Clash.GetClan(ctx, tag)
	}
}

func fetchWar(app *platform.App) trackedClanFetchFunc[clashy.ClanWar] {
	return func(ctx context.Context, tag string) (*clashy.ClanWar, error) {
		war, err := app.Clash.GetClanWar(ctx, tag)
		if closedWarLogResponse(err) {
			return nil, nil
		}
		return war, err
	}
}

func closedWarLogResponse(err error) bool {
	if err == nil {
		return false
	}
	var forbidden *clashy.Forbidden
	if errors.As(err, &forbidden) {
		return true
	}
	var notFound *clashy.NotFound
	return errors.As(err, &notFound)
}

func fetchRaid(app *platform.App) trackedClanFetchFunc[clashy.RaidLogEntry] {
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

func (d *trackedClansDomain) handleClanChange(ctx context.Context, app *platform.App, item TrackedItem[clashy.Clan]) error {
	if item.Current == nil {
		return nil
	}
	previous, raw, hasPrevious, changed, err := loadTrackedClanSnapshotChange(ctx, d.snapshots, d.snapshotPrefix, "clan", item.Tag, *item.Current, item.Raw)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if hasPrevious && previous != nil {
		app.Stats.SetReady(trackedClansDomainName, true, "")
		joined, left := trackedClanMemberChanges(*previous, *item.Current)
		for _, member := range joined {
			if err := app.PublishEvent(ctx, platform.Event{Topic: "clan", ClanTag: item.Tag, Value: map[string]any{
				"type": "member_join", "member": member, "clan": json.RawMessage(raw),
			}}); err != nil {
				return err
			}
		}
		for _, member := range left {
			if err := app.PublishEvent(ctx, platform.Event{Topic: "clan", ClanTag: item.Tag, Value: map[string]any{
				"type": "member_leave", "member": member, "clan": json.RawMessage(raw),
			}}); err != nil {
				return err
			}
		}
	}
	return d.snapshots.StoreRaw(ctx, trackedClanSnapshotKey(d.snapshotPrefix, "clan", item.Tag), raw)
}

func trackedClanMemberChanges(previous, current clashy.Clan) (joined, left []clashy.ClanMember) {
	previousByTag := make(map[string]clashy.ClanMember, len(previous.Members))
	currentByTag := make(map[string]clashy.ClanMember, len(current.Members))
	for _, member := range previous.Members {
		previousByTag[member.Tag] = member
	}
	for _, member := range current.Members {
		currentByTag[member.Tag] = member
		if _, exists := previousByTag[member.Tag]; !exists {
			joined = append(joined, member)
		}
	}
	for _, member := range previous.Members {
		if _, exists := currentByTag[member.Tag]; !exists {
			left = append(left, member)
		}
	}
	return joined, left
}

func (d *trackedClansDomain) handleWarChange(ctx context.Context, app *platform.App, item TrackedItem[clashy.ClanWar]) error {
	if item.Current == nil {
		return nil
	}
	current := *item.Current
	if current.Type() == "cwl" {
		return nil
	}
	previous, raw, hasPrevious, changed, err := loadTrackedClanSnapshotChange(ctx, d.snapshots, d.snapshotPrefix, "war", item.Tag, current, item.Raw)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	scheduleKey, err := d.store.UpsertCurrentWar(ctx, item.Tag, current, "")
	if err != nil {
		return err
	}
	if scheduleKey != "" {
		if err := app.PublishEvent(ctx, platform.Event{Topic: "war_schedule", ClanTag: item.Tag, Value: map[string]any{
			"type": "war_available", "schedule_key": scheduleKey,
		}}); err != nil {
			return err
		}
	}
	if hasPrevious && d.publishesWarEvents(item.Tag) {
		app.Stats.SetReady(trackedClansDomainName, true, "")
		if previous != nil {
			warPayload := json.RawMessage(raw)
			base := map[string]any{
				"clan_tag": item.Tag, "opponent_tag": warOpponentTag(current, item.Tag),
				"war_type": current.Type(), "war_role": "battle", "panel_target": true,
				"war": warPayload,
			}
			if !sameWarIdentity(*previous, current) {
				base["type"] = "new_war"
				if err := app.PublishEvent(ctx, platform.Event{
					Topic:   "war",
					ClanTag: item.Tag,
					Value:   base,
				}); err != nil {
					return err
				}
			} else {
				if attacks := newWarAttacks(*previous, current); len(attacks) > 0 {
					attackEvent := cloneAnyMap(base)
					attackEvent["type"] = "new_attacks"
					attackEvent["attacks"] = attacks
					if err := app.PublishEvent(ctx, platform.Event{
						Topic:   "war",
						ClanTag: item.Tag,
						Value:   attackEvent,
					}); err != nil {
						return err
					}
				}
				if previous.State != current.State {
					stateEvent := cloneAnyMap(base)
					stateEvent["type"] = "war_state"
					stateEvent["previous_war"] = json.RawMessage(jsonBytes(previous))
					if err := app.PublishEvent(ctx, platform.Event{
						Topic:   "war",
						ClanTag: item.Tag,
						Value:   stateEvent,
					}); err != nil {
						return err
					}
				}
			}
		}
	}
	return d.snapshots.StoreRaw(ctx, trackedClanSnapshotKey(d.snapshotPrefix, "war", item.Tag), raw)
}

func (d *trackedClansDomain) handleRaidChange(ctx context.Context, app *platform.App, item TrackedItem[clashy.RaidLogEntry]) error {
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
	if !changed {
		return d.capitalRaids.Replace(ctx, item.Tag, capitalRaidParticipantTags(*item.Current), raw, expiresAt)
	}
	if hasPrevious {
		app.Stats.RecordWrite(trackedClansDomainName, 1)
		app.Stats.SetReady(trackedClansDomainName, true, "")
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
	return trackedClanSnapshotKey(prefix, "raid", clanTag)
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

type trackedClanSnapshotStore interface {
	LoadRaw(context.Context, string) ([]byte, bool, error)
	StoreRaw(context.Context, string, []byte) error
}

type valkeyTrackedClanSnapshotStore struct {
	client valkey.Client
}

type memoryTrackedClanSnapshotStore struct {
	mu     sync.Mutex
	values map[string][]byte
}

func newTrackedClanSnapshotStore(app *platform.App) trackedClanSnapshotStore {
	if app.Valkey != nil {
		return valkeyTrackedClanSnapshotStore{client: app.Valkey}
	}
	return &memoryTrackedClanSnapshotStore{values: make(map[string][]byte)}
}

func (s valkeyTrackedClanSnapshotStore) LoadRaw(ctx context.Context, key string) ([]byte, bool, error) {
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

func (s valkeyTrackedClanSnapshotStore) StoreRaw(ctx context.Context, key string, raw []byte) error {
	return s.client.Do(ctx, s.client.B().Set().
		Key(key).
		Value(valkey.BinaryString(utils.Compress(raw))).
		Build(),
	).Error()
}

func (s *memoryTrackedClanSnapshotStore) LoadRaw(_ context.Context, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, ok := s.values[key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), raw...), true, nil
}

func (s *memoryTrackedClanSnapshotStore) StoreRaw(_ context.Context, key string, raw []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = append([]byte(nil), raw...)
	return nil
}

func trackedClanSnapshotKey(prefix, kind, tag string) string {
	return prefix + kind + ":" + tag
}

func loadTrackedClanSnapshotChange[T any](
	ctx context.Context,
	store trackedClanSnapshotStore,
	prefix string,
	kind string,
	tag string,
	current T,
	raw []byte,
) (*T, []byte, bool, bool, error) {
	if len(raw) == 0 {
		raw = jsonBytes(current)
	}
	key := trackedClanSnapshotKey(prefix, kind, tag)
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

func loadTrackedClanSnapshot[T any](ctx context.Context, store trackedClanSnapshotStore, prefix, kind, tag string) (*T, []byte, bool, error) {
	raw, ok, err := store.LoadRaw(ctx, trackedClanSnapshotKey(prefix, kind, tag))
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

func (d *trackedClansDomain) publishRaidDiffEvents(ctx context.Context, app *platform.App, clanTag string, previous, current clashy.RaidLogEntry, raw []byte) error {
	if previous.State != current.State {
		if err := app.PublishEvent(ctx, platform.Event{
			Topic:   "capital",
			ClanTag: clanTag,
			Value:   map[string]any{"type": "raid_state", "previous_raid": previous, "raid": json.RawMessage(raw), "clan_tag": clanTag},
		}); err != nil {
			return err
		}
	}
	if attacked := changedRaidMemberAttacks(previous, current); len(attacked) > 0 {
		return app.PublishEvent(ctx, platform.Event{
			Topic:   "capital",
			ClanTag: clanTag,
			Value:   map[string]any{"type": "raid_attacks", "attacked": attacked, "raid": json.RawMessage(raw), "previous_raid": previous, "clan_tag": clanTag},
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
	Season            string          `json:"season,omitempty"`
	GroupState        string          `json:"group_state,omitempty"`
	GroupHash         uint64          `json:"group_hash,omitempty"`
	GroupRaw          json.RawMessage `json:"group_raw,omitempty"`
	GroupCheckedAt    time.Time       `json:"group_checked_at,omitempty"`
	BattleWarTag      string          `json:"battle_war_tag,omitempty"`
	PreparationWarTag string          `json:"preparation_war_tag,omitempty"`
	Ended             bool            `json:"ended,omitempty"`
	NoSpin            bool            `json:"no_spin,omitempty"`
}

type cwlWarFetchResult struct {
	war *clashy.ClanWar
	raw []byte
	err error
}

type cwlWarFetchCall struct {
	done   chan struct{}
	result cwlWarFetchResult
}

// A CWL matchup can contain two tracked clans. This cycle-local cache makes
// those clans share the same tagged-war request without introducing durable or
// cross-process coordination.
type cwlCycleWarCache struct {
	mu    sync.Mutex
	calls map[string]*cwlWarFetchCall
}

func newCWLCycleWarCache() *cwlCycleWarCache {
	return &cwlCycleWarCache{calls: make(map[string]*cwlWarFetchCall)}
}

func (c *cwlCycleWarCache) fetch(ctx context.Context, warTag string, fetch func(context.Context) (*clashy.ClanWar, []byte, error)) (*clashy.ClanWar, []byte, error) {
	c.mu.Lock()
	if call, ok := c.calls[warTag]; ok {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-call.done:
			return call.result.war, call.result.raw, call.result.err
		}
	}
	call := &cwlWarFetchCall{done: make(chan struct{})}
	c.calls[warTag] = call
	c.mu.Unlock()

	call.result.war, call.result.raw, call.result.err = fetch(ctx)
	close(call.done)
	return call.result.war, call.result.raw, call.result.err
}

func (d *trackedClansDomain) runCWLLoop(ctx context.Context, app *platform.App, limiter *clashy.Limiter) error {
	interval := time.Duration(app.Config.WarCWLSyncSeconds) * time.Second
	if interval <= 0 {
		interval = 3 * time.Minute
	}
	for {
		start := time.Now()
		err := d.runCWLCycle(ctx, app, limiter)
		app.Stats.RecordProcess(trackedClansDomainName, time.Since(start))
		if err != nil {
			return err
		}
		if err := sleepOrDone(ctx, interval); err != nil {
			return err
		}
	}
}

func (d *trackedClansDomain) runCWLCycle(ctx context.Context, app *platform.App, limiter *clashy.Limiter) error {
	tags := d.targetTags("wars")
	now := time.Now().UTC()
	cache := newCWLCycleWarCache()
	return runBounded(ctx, platform.RequestConcurrency(app.Config.TrackedClanRequestsPerSecond), tags, func(workerCtx context.Context, tag string) error {
		if err := d.processCWLTarget(workerCtx, app, limiter, cache, tag, now); err != nil {
			if workerCtx.Err() != nil {
				return workerCtx.Err()
			}
			// One clan's private/broken CWL response must not stop live tracking
			// for every other clan. The next cycle retries it naturally.
			app.Logger.Error("live CWL tracking failed", "tag", tag, "err", err)
			app.Stats.SetReady(trackedClansDomainName, false, err.Error())
		}
		return nil
	})
}

func (d *trackedClansDomain) processCWLTarget(ctx context.Context, app *platform.App, limiter *clashy.Limiter, cache *cwlCycleWarCache, tag string, now time.Time) error {
	state, _, ok, err := loadTrackedClanSnapshot[botCWLState](ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag)
	if err != nil {
		return err
	}
	season := utils.CurrentSeason(now)
	if ok {
		d.setCWLActive(tag, state.Season == season && !state.Ended && !state.NoSpin && (state.BattleWarTag != "" || state.PreparationWarTag != "" || state.GroupState == "preparation" || state.GroupState == "inWar"))
	}
	if !ok && !cwlDiscoveryWindow(now) {
		d.setCWLActive(tag, false)
		return nil
	}
	if ok && !shouldPollCWL(now, *state) {
		d.setCWLActive(tag, false)
		return nil
	}

	current := botCWLState{Season: season}
	if ok {
		current = *state
	}
	refreshGroup := shouldRefreshCWLGroup(now, current, ok)

	if current.BattleWarTag != "" {
		war, raw, err := d.fetchCachedCWLWar(ctx, app, limiter, cache, current.BattleWarTag)
		if err != nil {
			return err
		}
		if war != nil {
			if err := d.handleCWLWarChange(ctx, app, tag, current.BattleWarTag, *war, raw, current.GroupRaw, cwlWarBattle, false, true); err != nil {
				return err
			}
			if war.State == clashy.WarStateEnded {
				current.BattleWarTag = ""
				refreshGroup = true
			}
		}
	}
	if current.PreparationWarTag != "" {
		warTag := current.PreparationWarTag
		war, raw, err := d.fetchCachedCWLWar(ctx, app, limiter, cache, warTag)
		if err != nil {
			return err
		}
		if war != nil {
			if war.State == clashy.WarStateInWar {
				becameBattle := current.BattleWarTag != warTag
				current.BattleWarTag = warTag
				current.PreparationWarTag = ""
				if err := d.handleCWLWarChange(ctx, app, tag, warTag, *war, raw, current.GroupRaw, cwlWarBattle, becameBattle, true); err != nil {
					return err
				}
				refreshGroup = true
			} else if war.State == clashy.WarStateEnded {
				current.PreparationWarTag = ""
				refreshGroup = true
			} else if err := d.handleCWLWarChange(ctx, app, tag, warTag, *war, raw, current.GroupRaw, cwlWarPreparation, false, current.BattleWarTag == ""); err != nil {
				return err
			}
		}
	}
	if !refreshGroup {
		return storeTrackedClanValue(ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag, current)
	}

	group, raw, err := d.fetchCWLGroup(ctx, app, limiter, tag)
	if err != nil {
		return err
	}
	if group == nil {
		current.GroupCheckedAt = now
		current.GroupState = "notInWar"
		current.Season = season
		current.NoSpin = cwlSignupClosed(now)
		d.setCWLActive(tag, false)
		return storeTrackedClanValue(ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag, current)
	}
	currentState := cwlStateFromGroup(group, raw, now)
	currentState.GroupCheckedAt = now
	currentState.GroupRaw = append(json.RawMessage(nil), raw...)
	if groupNotInThisSeason(group, now) {
		// Remember that this clan was checked this season. If it signs up before
		// the deadline, the later transition can be distinguished from a cold
		// process start without polling beyond the third.
		currentState.Season = season
		if cwlSignupClosed(now) {
			currentState.NoSpin = true
		}
		d.setCWLActive(tag, false)
		return storeTrackedClanValue(ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag, currentState)
	}
	if currentState.Ended {
		d.setCWLActive(tag, false)
		return storeTrackedClanValue(ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag, currentState)
	}
	resolved, err := d.resolveClanCWLWars(ctx, app, limiter, cache, tag, group)
	if err != nil {
		return err
	}
	previousBattleTag := current.BattleWarTag
	previousPreparationTag := current.PreparationWarTag
	currentState.BattleWarTag = resolved.battleTag
	currentState.PreparationWarTag = resolved.preparationTag
	if resolved.battle != nil {
		announce := ok && current.Season == season && previousBattleTag != resolved.battleTag
		if err := d.handleCWLWarChange(ctx, app, tag, resolved.battleTag, *resolved.battle.war, resolved.battle.raw, currentState.GroupRaw, cwlWarBattle, announce, true); err != nil {
			return err
		}
	}
	if resolved.preparation != nil && resolved.preparationTag != resolved.battleTag {
		announce := ok && current.Season == season && previousPreparationTag != resolved.preparationTag
		if err := d.handleCWLWarChange(ctx, app, tag, resolved.preparationTag, *resolved.preparation.war, resolved.preparation.raw, currentState.GroupRaw, cwlWarPreparation, announce, resolved.battle == nil); err != nil {
			return err
		}
	}
	d.setCWLActive(tag, true)
	return storeTrackedClanValue(ctx, d.snapshots, d.snapshotPrefix, d.cwlStateSnapshot, tag, currentState)
}

func shouldRefreshCWLGroup(now time.Time, state botCWLState, hasState bool) bool {
	if !hasState || state.GroupCheckedAt.IsZero() || now.Sub(state.GroupCheckedAt) >= cwlGroupRefreshPeriod {
		return true
	}
	waitingForWarTags := state.GroupState == "preparation" || state.GroupState == "inWar"
	return waitingForWarTags && state.BattleWarTag == "" && state.PreparationWarTag == ""
}

func (d *trackedClansDomain) fetchCachedCWLWar(ctx context.Context, app *platform.App, limiter *clashy.Limiter, cache *cwlCycleWarCache, warTag string) (*clashy.ClanWar, []byte, error) {
	return cache.fetch(ctx, warTag, func(fetchCtx context.Context) (*clashy.ClanWar, []byte, error) {
		return d.fetchCWLWar(fetchCtx, app, limiter, warTag)
	})
}

type resolvedClanCWLWars struct {
	battleTag      string
	preparationTag string
	battle         *cwlWarFetchResult
	preparation    *cwlWarFetchResult
}

func (d *trackedClansDomain) resolveClanCWLWars(ctx context.Context, app *platform.App, limiter *clashy.Limiter, cache *cwlCycleWarCache, clanTag string, group *clashy.ClanWarLeagueGroup) (resolvedClanCWLWars, error) {
	var out resolvedClanCWLWars
	if group == nil || group.State == "ended" {
		return out, nil
	}
	rounds := validCWLWarTagRounds(group)
	if len(rounds) == 0 {
		return out, nil
	}
	latestSample, _, err := d.fetchCachedCWLWar(ctx, app, limiter, cache, rounds[len(rounds)-1][0])
	if err != nil {
		return out, err
	}
	latestState := clashy.WarState(group.State)
	if latestSample != nil {
		latestState = latestSample.State
	}
	battleIndex, preparationIndex := cwlRoundRoleIndexes(len(rounds), latestState)
	if battleIndex >= 0 {
		battleRound := rounds[battleIndex]
		battleTag, battle, err := d.findClanCWLWarCached(ctx, app, limiter, cache, clanTag, battleRound)
		if err != nil {
			return out, err
		}
		if battle != nil && battle.war.State != clashy.WarStateEnded {
			out.battleTag, out.battle = battleTag, battle
		}
	}
	if preparationIndex >= 0 {
		preparationRound := rounds[preparationIndex]
		preparationTag, preparation, err := d.findClanCWLWarCached(ctx, app, limiter, cache, clanTag, preparationRound)
		if err != nil {
			return out, err
		}
		if preparation != nil && preparation.war.State != clashy.WarStateEnded {
			out.preparationTag, out.preparation = preparationTag, preparation
		}
	}
	return out, nil
}

func cwlRoundRoleIndexes(roundCount int, latestState clashy.WarState) (current, preparation int) {
	if roundCount <= 0 {
		return -1, -1
	}
	// A preparation war remains a preparation war even during round one. The
	// panel may display it only when the battle slot is empty. From round two
	// onward yesterday's round can be in battle while the newest is preparing.
	if latestState == clashy.WarStatePreparation {
		if roundCount == 1 {
			return -1, 0
		}
		return roundCount - 2, roundCount - 1
	}
	return roundCount - 1, -1
}

func validCWLWarTagRounds(group *clashy.ClanWarLeagueGroup) [][]string {
	if group == nil {
		return nil
	}
	out := make([][]string, 0, len(group.Rounds))
	for _, round := range group.Rounds {
		tags := make([]string, 0, len(round.WarTags))
		for _, tag := range round.WarTags {
			if tag != "" && tag != "#0" {
				tags = append(tags, tag)
			}
		}
		if len(tags) > 0 {
			out = append(out, tags)
		}
	}
	return out
}

func (d *trackedClansDomain) findClanCWLWarCached(ctx context.Context, app *platform.App, limiter *clashy.Limiter, cache *cwlCycleWarCache, clanTag string, warTags []string) (string, *cwlWarFetchResult, error) {
	for _, warTag := range warTags {
		war, raw, err := d.fetchCachedCWLWar(ctx, app, limiter, cache, warTag)
		if err != nil {
			return "", nil, err
		}
		if war != nil && warContainsClan(*war, clanTag) {
			return warTag, &cwlWarFetchResult{war: war, raw: raw}, nil
		}
	}
	return "", nil, nil
}

func (d *trackedClansDomain) fetchCWLGroup(ctx context.Context, app *platform.App, limiter *clashy.Limiter, tag string) (*clashy.ClanWarLeagueGroup, []byte, error) {
	group, err := retryLimitedClashFetch(ctx, app, limiter, func(fetchCtx context.Context) (*clashy.ClanWarLeagueGroup, error) {
		start := time.Now()
		group, err := app.Clash.GetLeagueGroup(fetchCtx, tag)
		app.Stats.RecordRequest(trackedClansDomainName, time.Since(start), err)
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

func (d *trackedClansDomain) fetchCWLWar(ctx context.Context, app *platform.App, limiter *clashy.Limiter, warTag string) (*clashy.ClanWar, []byte, error) {
	wars, err := retryLimitedClashFetch(ctx, app, limiter, func(fetchCtx context.Context) ([]clashy.ClanWar, error) {
		start := time.Now()
		wars, err := app.Clash.GetLeagueWars(fetchCtx, []string{warTag})
		app.Stats.RecordRequest(trackedClansDomainName, time.Since(start), err)
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

func (d *trackedClansDomain) findClanCWLWar(ctx context.Context, app *platform.App, limiter *clashy.Limiter, clanTag string, warTags []string) (string, *clashy.ClanWar, []byte, error) {
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

type cwlWarRole string

const (
	cwlWarBattle      cwlWarRole = "battle"
	cwlWarPreparation cwlWarRole = "preparation"
)

func (d *trackedClansDomain) handleCWLWarChange(
	ctx context.Context,
	app *platform.App,
	clanTag string,
	warTag string,
	war clashy.ClanWar,
	raw []byte,
	groupRaw json.RawMessage,
	role cwlWarRole,
	announce bool,
	panelTarget bool,
) error {
	// Current battle and next preparation overlap during CWL, so each war tag
	// owns an independent comparison snapshot.
	snapshotKind := "cwlwar:" + strings.TrimPrefix(warTag, "#")
	previous, raw, hasPrevious, changed, err := loadTrackedClanSnapshotChange(ctx, d.snapshots, d.snapshotPrefix, snapshotKind, clanTag, war, raw)
	if err != nil || (!changed && !announce) {
		return err
	}

	newIdentity := !hasPrevious || previous == nil || !sameWarIdentity(*previous, war)
	if newIdentity {
		scheduleKey, err := d.store.UpsertCurrentWar(ctx, clanTag, war, warTag)
		if err != nil {
			return err
		}
		if scheduleKey != "" {
			if err := app.PublishEvent(ctx, platform.Event{Topic: "war_schedule", ClanTag: clanTag, Value: map[string]any{
				"type": "war_available", "schedule_key": scheduleKey,
			}}); err != nil {
				return err
			}
		}
	}

	if d.publishesWarEvents(clanTag) {
		app.Stats.SetReady(trackedClansDomainName, true, "")
		warPayload := json.RawMessage(raw)
		groupPayload := json.RawMessage(groupRaw)
		base := cwlWarEventBase(clanTag, warTag, war, warPayload, groupPayload, role, panelTarget)
		if announce {
			base["type"] = "new_war"
			if err := app.PublishEvent(ctx, platform.Event{Topic: "war", ClanTag: clanTag, Value: base}); err != nil {
				return err
			}
		} else if role == cwlWarPreparation && hasPrevious && previous != nil {
			if lineup := cwlLineupChanges(*previous, war); cwlLineupChanged(lineup) {
				for key, value := range base {
					lineup[key] = value
				}
				if err := app.PublishEvent(ctx, platform.Event{Topic: "war", ClanTag: clanTag, Value: lineup}); err != nil {
					return err
				}
			}
		} else if hasPrevious && previous != nil {
			if attacks := newWarAttacks(*previous, war); len(attacks) > 0 {
				attackEvent := cloneAnyMap(base)
				attackEvent["type"] = "new_attacks"
				attackEvent["attacks"] = attacks
				if err := app.PublishEvent(ctx, platform.Event{Topic: "war", ClanTag: clanTag, Value: attackEvent}); err != nil {
					return err
				}
			}
			if previous.State != war.State {
				stateEvent := cloneAnyMap(base)
				stateEvent["type"] = "war_state"
				stateEvent["previous_war"] = json.RawMessage(jsonBytes(previous))
				if err := app.PublishEvent(ctx, platform.Event{Topic: "war", ClanTag: clanTag, Value: stateEvent}); err != nil {
					return err
				}
			}
		}
	}
	if changed {
		return d.snapshots.StoreRaw(ctx, trackedClanSnapshotKey(d.snapshotPrefix, snapshotKind, clanTag), raw)
	}
	return nil
}

func cwlWarEventBase(clanTag, warTag string, war clashy.ClanWar, warPayload, groupPayload json.RawMessage, role cwlWarRole, panelTarget bool) map[string]any {
	return map[string]any{
		"clan_tag": clanTag, "opponent_tag": warOpponentTag(war, clanTag),
		"war_type": "cwl", "war_role": string(role), "war_tag": warTag,
		"panel_target": panelTarget, "war": warPayload, "league_group": groupPayload,
	}
}

func cloneAnyMap(source map[string]any) map[string]any {
	clone := make(map[string]any, len(source)+2)
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func warOpponentTag(war clashy.ClanWar, clanTag string) string {
	if war.Clan != nil && war.Clan.Tag != clanTag {
		return war.Clan.Tag
	}
	if war.Opponent != nil && war.Opponent.Tag != clanTag {
		return war.Opponent.Tag
	}
	return ""
}

func storeTrackedClanValue(ctx context.Context, store trackedClanSnapshotStore, prefix, kind, tag string, value any) error {
	return store.StoreRaw(ctx, trackedClanSnapshotKey(prefix, kind, tag), jsonBytes(value))
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
	return day >= 1 && day <= 3
}

func cwlSignupClosed(now time.Time) bool {
	return now.UTC().Day() > 3
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
		"war":              json.RawMessage(jsonBytes(current)),
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

func jsonBytes(value any) []byte {
	raw, _ := json.Marshal(value)
	return raw
}
