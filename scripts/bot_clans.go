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
	clashtracker "github.com/clashkinginc/clashy.go/tracker"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.opentelemetry.io/otel/attribute"
)

type botClanFetchFunc[T any] func(context.Context, string) (*T, int, error)

type TrackedItem[T any] struct {
	Group   string
	Kind    string
	Tag     string
	Current *T
	Raw     []byte
	Retry   int
}

type botClanTrackedValue[T any] struct {
	Current *T
	Raw     []byte
	Retry   int
}

const (
	botClansDomainName = "botclans"
)

type botClansDomain struct {
	mu               sync.Mutex
	scheduled        map[string]struct{}
	targets          clashtracker.TargetStore
	snapshots        botClanSnapshotStore
	snapshotPrefix   string
	cwlStateSnapshot string
}

func NewBotClansDomain() platform.Domain {
	return &botClansDomain{
		scheduled: make(map[string]struct{}),
		snapshots: &memoryBotClanSnapshotStore{
			values: make(map[string][]byte),
		},
	}
}

func (d *botClansDomain) Name() string { return botClansDomainName }

func (d *botClansDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateBotClansConfig(app.Config); err != nil {
		return err
	}
	targetSource, err := newBotClanTargetStore(ctx, app)
	if err != nil {
		return err
	}
	defer targetSource.Close()
	targetSet, err := newScriptMemoryTargetSet(ctx, app, botClansDomainName, "targets", 15*time.Second, targetSource.List)
	if err != nil {
		return err
	}
	d.targets = targetSet.Store()
	d.snapshots = newBotClanSnapshotStore(app)
	d.snapshotPrefix = app.Config.BotClanSnapshotPrefix
	d.cwlStateSnapshot = app.Config.BotClanCWLStateSnapshot

	rateLimit := app.Config.BotClanRequestsPerSecond
	errCh := make(chan error, 4)
	go func() {
		errCh <- runBotClanTracker(ctx, app, targetSet, "clans", "clan", fetchClan(app), d.handleClanChange, rateLimit)
	}()
	go func() {
		errCh <- runBotClanTracker(ctx, app, targetSet, "wars", "war", fetchWar(app), d.handleWarChange, rateLimit)
	}()
	go func() {
		errCh <- runBotClanTracker(ctx, app, targetSet, "raids", "raid", fetchRaid(app), d.handleRaidChange, rateLimit)
	}()
	go targetSet.Run(ctx)
	cwlLimiter, err := clashtracker.NewLimiter(rateLimit, rateLimit)
	if err != nil {
		return err
	}
	go func() {
		errCh <- d.runCWLLoop(ctx, app, cwlLimiter)
	}()
	return <-errCh
}

func validateBotClansConfig(cfg platform.Config) error {
	if cfg.BotClanRequestsPerSecond <= 0 {
		return errors.New("botclans.requests_per_second must be greater than zero")
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

type botClanTargetStore interface {
	clashtracker.TargetStore
	Close() error
}

type timescaleBotClanTargetStore struct {
	pool *pgxpool.Pool
}

func newBotClanTargetStore(ctx context.Context, app *platform.App) (botClanTargetStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return memoryBotClanTargetStore{}, nil
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return nil, err
	}
	return &timescaleBotClanTargetStore{pool: pool}, nil
}

func (s *timescaleBotClanTargetStore) Close() error {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
	return nil
}

func (s *timescaleBotClanTargetStore) List(ctx context.Context) ([]clashtracker.Target, error) {
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
	var out []clashtracker.Target
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, err
		}
		out = append(out, clashtracker.Target{Key: tag})
	}
	return out, rows.Err()
}

func (s *timescaleBotClanTargetStore) Add(context.Context, ...clashtracker.Target) error {
	return nil
}

func (s *timescaleBotClanTargetStore) Remove(context.Context, ...string) error {
	return nil
}

type memoryBotClanTargetStore struct {
	tags []string
}

func (s memoryBotClanTargetStore) Close() error { return nil }

func (s memoryBotClanTargetStore) List(context.Context) ([]clashtracker.Target, error) {
	targets := make([]clashtracker.Target, 0, len(s.tags))
	for _, tag := range s.tags {
		targets = append(targets, clashtracker.Target{Key: tag})
	}
	return targets, nil
}

func (s memoryBotClanTargetStore) Add(context.Context, ...clashtracker.Target) error {
	return nil
}

func (s memoryBotClanTargetStore) Remove(context.Context, ...string) error {
	return nil
}

func runBotClanTracker[T any](
	ctx context.Context,
	app *platform.App,
	targetSet *scriptMemoryTargetSet,
	group string,
	kind string,
	fetch botClanFetchFunc[T],
	handle func(context.Context, *platform.App, TrackedItem[T]) error,
	rateLimit int,
) error {
	runner, err := clashtracker.NewRunner[botClanTrackedValue[T], botClanTrackedValue[T]](clashtracker.Config[botClanTrackedValue[T], botClanTrackedValue[T]]{
		TargetStore:       targetSet.Store(),
		Store:             botClanTrackerStore[T]{},
		RequestsPerSecond: rateLimit,
		MaxInFlight:       rateLimit,
		MinInterval:       15 * time.Second,
		EmitInitial:       true,
		Fetch: func(fetchCtx context.Context, target clashtracker.Target) (clashtracker.FetchResult[botClanTrackedValue[T]], error) {
			start := time.Now()
			current, retry, err := fetch(fetchCtx, target.Key)
			app.Stats.RecordRequest(botClansDomainName, time.Since(start), err)
			if err != nil {
				return clashtracker.FetchResult[botClanTrackedValue[T]]{}, err
			}
			value := botClanTrackedValue[T]{Current: current, Retry: retry}
			if current != nil {
				value.Raw = jsonBytes(current)
			}
			retryAfter := 15 * time.Second
			if retry > 0 {
				retryAfter = time.Duration(retry) * time.Second
			}
			return clashtracker.FetchResult[botClanTrackedValue[T]]{Value: value, RetryAfter: retryAfter}, nil
		},
		Project: func(value botClanTrackedValue[T]) (botClanTrackedValue[T], error) {
			return value, nil
		},
		Diff: func(diffCtx context.Context, target clashtracker.Target, _, current botClanTrackedValue[T]) error {
			return handle(diffCtx, app, TrackedItem[T]{
				Group:   group,
				Kind:    kind,
				Tag:     target.Key,
				Current: current.Current,
				Raw:     current.Raw,
				Retry:   current.Retry,
			})
		},
		OnError: func(_ context.Context, target clashtracker.Target, err error) error {
			app.Logger.Error("bot clan fetch failed", "group", group, "tag", target.Key, "err", err)
			app.Stats.SetReady(botClansDomainName, false, err.Error())
			return nil
		},
	})
	if err != nil {
		return err
	}
	targetSet.SetRunner(runner)
	return runner.Run(ctx)
}

type botClanTrackerStore[T any] struct{}

func (botClanTrackerStore[T]) Load(context.Context, string) (botClanTrackedValue[T], bool, error) {
	return botClanTrackedValue[T]{}, false, nil
}

func (botClanTrackerStore[T]) Store(context.Context, string, botClanTrackedValue[T], time.Duration) error {
	return nil
}

func (botClanTrackerStore[T]) LoadBatch(context.Context, []string) (map[string]clashtracker.Snapshot[botClanTrackedValue[T]], error) {
	return map[string]clashtracker.Snapshot[botClanTrackedValue[T]]{}, nil
}

func (botClanTrackerStore[T]) StoreBatch(context.Context, []clashtracker.StoredSnapshot[botClanTrackedValue[T]]) error {
	return nil
}

func fetchClan(app *platform.App) botClanFetchFunc[clashy.Clan] {
	return func(ctx context.Context, tag string) (*clashy.Clan, int, error) {
		clan, err := app.Clash.GetClan(ctx, tag)
		retry := 0
		if clan != nil {
			retry = clan.RetryAfter()
		}
		return clan, retry, err
	}
}

func fetchWar(app *platform.App) botClanFetchFunc[clashy.ClanWar] {
	return func(ctx context.Context, tag string) (*clashy.ClanWar, int, error) {
		war, err := app.Clash.GetClanWar(ctx, tag)
		retry := 0
		if war != nil {
			retry = war.RetryAfter()
		}
		return war, retry, err
	}
}

func fetchRaid(app *platform.App) botClanFetchFunc[clashy.RaidLogEntry] {
	return func(ctx context.Context, tag string) (*clashy.RaidLogEntry, int, error) {
		raids, err := app.Clash.GetRaidLog(ctx, tag, 1, "", "")
		if err != nil {
			return nil, 0, err
		}
		if len(raids) == 0 {
			return &clashy.RaidLogEntry{}, 0, nil
		}
		raid := raids[0]
		return &raid, raid.RetryAfter(), nil
	}
}

func (d *botClansDomain) handleClanChange(ctx context.Context, app *platform.App, item TrackedItem[clashy.Clan]) error {
	if item.Current == nil {
		return nil
	}
	_, diffSpan := platform.StartSpan(ctx, "tracker.diff",
		attribute.String("domain", botClansDomainName),
		attribute.String("group", item.Group),
		attribute.String("operation", "clan_compare"),
	)
	_, raw, hasPrevious, changed, err := botClanSnapshotChanged(ctx, d.snapshots, d.snapshotPrefix, "clan", item.Tag, *item.Current, item.Raw)
	platform.RecordSpanError(diffSpan, err)
	diffSpan.SetAttributes(platform.SpanErrorStatus(err))
	diffSpan.End()
	if err != nil {
		return err
	}
	if !hasPrevious || !changed {
		return nil
	}
	app.Stats.SetReady(botClansDomainName, true, "")
	if err := app.PublishEvent(ctx, platform.Event{
		Topic:   "clan",
		ClanTag: item.Tag,
		Value:   map[string]any{"type": "clan_update", "raw": string(raw)},
	}); err != nil {
		return err
	}
	return nil
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
	_, diffSpan := platform.StartSpan(ctx, "tracker.diff",
		attribute.String("domain", botClansDomainName),
		attribute.String("group", item.Group),
		attribute.String("operation", "war_compare"),
	)
	previous, raw, hasPrevious, changed, err := botClanSnapshotChanged(ctx, d.snapshots, d.snapshotPrefix, "war", item.Tag, current, item.Raw)
	platform.RecordSpanError(diffSpan, err)
	diffSpan.SetAttributes(platform.SpanErrorStatus(err))
	diffSpan.End()
	if err != nil {
		return err
	}
	if !hasPrevious || !changed {
		return nil
	}

	app.Stats.SetReady(botClansDomainName, true, "")
	if err := app.PublishEvent(ctx, platform.Event{
		Topic:   "war",
		ClanTag: item.Tag,
		Value:   map[string]any{"type": "war_update", "raw": string(raw)},
	}); err != nil {
		return err
	}
	if previous == nil {
		return nil
	}
	if !sameWarIdentity(*previous, current) {
		return app.PublishEvent(ctx, platform.Event{
			Topic:   "war",
			ClanTag: item.Tag,
			Value:   map[string]any{"type": "new_war", "new_war": string(raw), "clan_tag": item.Tag},
		})
	}
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
		return app.PublishEvent(ctx, platform.Event{
			Topic:   "war",
			ClanTag: item.Tag,
			Value:   map[string]any{"type": "war_state", "old_state": previous.State, "new_state": current.State, "war": string(raw), "clan_tag": item.Tag},
		})
	}
	return nil
}

func (d *botClansDomain) handleRaidChange(ctx context.Context, app *platform.App, item TrackedItem[clashy.RaidLogEntry]) error {
	if item.Current == nil {
		return nil
	}
	_, diffSpan := platform.StartSpan(ctx, "tracker.diff",
		attribute.String("domain", botClansDomainName),
		attribute.String("group", item.Group),
		attribute.String("operation", "raid_compare"),
	)
	previous, raw, hasPrevious, changed, err := botClanSnapshotChanged(ctx, d.snapshots, d.snapshotPrefix, "raid", item.Tag, *item.Current, item.Raw)
	platform.RecordSpanError(diffSpan, err)
	diffSpan.SetAttributes(platform.SpanErrorStatus(err))
	diffSpan.End()
	if err != nil {
		return err
	}
	if err := d.scheduleRaidReminders(ctx, app, item.Tag, *item.Current); err != nil {
		return err
	}
	if !changed {
		return nil
	}
	if err := app.Store.Stats().Collection("capital_cache").UpdateOne(ctx, bson.M{"tag": item.Tag}, bson.M{"$set": bson.M{"data": string(raw)}}, true); err != nil {
		return err
	}
	if !hasPrevious {
		return nil
	}
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
	return nil
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

func botClanSnapshotChanged[T any](
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
	if err := store.StoreRaw(ctx, key, raw); err != nil {
		return nil, raw, hasPrevious, false, err
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
	if war.EndTime == nil || war.EndTime.Time.IsZero() || app.Scheduler == nil || app.Store == nil {
		return nil
	}
	if len(raw) == 0 {
		raw = jsonBytes(war)
	}
	reminderTimes, err := app.Store.Static().Collection("reminders").DistinctStrings(ctx, "time", bson.M{"clan": clanTag, "type": "War"})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, reminder := range reminderTimes {
		hours, err := parseReminderHours(reminder)
		if err != nil {
			continue
		}
		runAt := war.EndTime.Time.UTC().Add(-time.Duration(hours * float64(time.Hour)))
		if !runAt.After(now) {
			continue
		}
		jobID := "war_end:" + kind + ":" + clanTag + ":" + reminder + ":" + war.EndTime.RawTime
		if !d.scheduleOnce(jobID) {
			continue
		}
		reminder := reminder
		raw := append([]byte(nil), raw...)
		app.Scheduler.Schedule(platform.Job{
			ID:   jobID,
			When: runAt,
			Run: func(ctx context.Context) {
				_ = app.PublishEvent(ctx, platform.Event{
					Topic:   "reminder",
					ClanTag: clanTag,
					Value: map[string]any{
						"type":     "war",
						"war_type": kind,
						"time":     reminder,
						"data":     string(raw),
					},
				})
			},
		})
	}
	return nil
}

func (d *botClansDomain) scheduleRaidReminders(ctx context.Context, app *platform.App, clanTag string, raid clashy.RaidLogEntry) error {
	if raid.EndTime == nil || raid.EndTime.Time.IsZero() || app.Scheduler == nil || app.Store == nil {
		return nil
	}
	reminders, err := app.Store.Static().Collection("reminders").FindAll(ctx, bson.M{"clan": clanTag, "type": "Clan Capital"}, platform.FindOptions{})
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, reminder := range reminders {
		if reminder["clan"] != nil && reminder["clan"] != clanTag {
			continue
		}
		if reminder["type"] != nil && reminder["type"] != "Clan Capital" {
			continue
		}
		timeValue, _ := reminder["time"].(string)
		hours, err := parseReminderHours(timeValue)
		if err != nil {
			continue
		}
		runAt := raid.EndTime.Time.UTC().Add(-time.Duration(hours * float64(time.Hour)))
		if !runAt.After(now) {
			continue
		}
		jobID := "raid_end:" + clanTag + ":" + timeValue + ":" + raid.EndTime.RawTime
		if !d.scheduleOnce(jobID) {
			continue
		}
		reminder := cloneReminder(reminder)
		app.Scheduler.Schedule(platform.Job{
			ID:   jobID,
			When: runAt,
			Run: func(ctx context.Context) {
				_ = d.publishRaidReminder(ctx, app, clanTag, reminder)
			},
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

func (d *botClansDomain) publishRaidReminder(ctx context.Context, app *platform.App, clanTag string, reminder bson.M) error {
	clan, _, ok, err := loadBotClanSnapshot[clashy.Clan](ctx, d.snapshots, d.snapshotPrefix, "clan", clanTag)
	if err != nil || !ok {
		return err
	}
	raid, _, ok, err := loadBotClanSnapshot[clashy.RaidLogEntry](ctx, d.snapshots, d.snapshotPrefix, "raid", clanTag)
	if err != nil || !ok {
		return err
	}
	missing := raidMissingMembers(*clan, *raid, reminder)
	if len(missing) == 0 {
		return nil
	}
	return app.PublishEvent(ctx, platform.Event{
		Topic:   "reminder",
		ClanTag: clanTag,
		Value: map[string]any{
			"type":          "raid",
			"clan_data":     clan,
			"raid_data":     raid,
			"reminder_data": reminder,
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

func raidMissingMembers(clan clashy.Clan, raid clashy.RaidLogEntry, reminder bson.M) []map[string]any {
	threshold := intFromAny(reminder["attack_threshold"])
	roles := stringList(reminder["roles"])
	townHalls := intList(reminder["townhalls"])
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

func (d *botClansDomain) runCWLLoop(ctx context.Context, app *platform.App, limiter *clashtracker.Limiter) error {
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

func (d *botClansDomain) runCWLCycle(ctx context.Context, app *platform.App, limiter *clashtracker.Limiter) error {
	tags, err := collectTargetStoreTags(ctx, d.targets)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, tag := range tags {
		if err := d.processCWLTarget(ctx, app, limiter, tag, now); err != nil {
			return err
		}
	}
	return nil
}

func (d *botClansDomain) processCWLTarget(ctx context.Context, app *platform.App, limiter *clashtracker.Limiter, tag string, now time.Time) error {
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

func (d *botClansDomain) fetchCWLGroup(ctx context.Context, app *platform.App, limiter *clashtracker.Limiter, tag string) (*clashy.ClanWarLeagueGroup, []byte, error) {
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	start := time.Now()
	group, err := app.Clash.GetLeagueGroup(ctx, tag)
	release()
	app.Stats.RecordRequest(botClansDomainName, time.Since(start), err)
	if err != nil {
		return nil, nil, nil
	}
	return group, jsonBytes(group), nil
}

func (d *botClansDomain) fetchCWLWar(ctx context.Context, app *platform.App, limiter *clashtracker.Limiter, warTag string) (*clashy.ClanWar, []byte, error) {
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	start := time.Now()
	wars, err := app.Clash.GetLeagueWars(ctx, []string{warTag})
	release()
	app.Stats.RecordRequest(botClansDomainName, time.Since(start), err)
	if err != nil || len(wars) == 0 {
		return nil, nil, nil
	}
	return &wars[0], jsonBytes(wars[0]), nil
}

func (d *botClansDomain) findClanCWLWar(ctx context.Context, app *platform.App, limiter *clashtracker.Limiter, clanTag string, warTags []string) (string, *clashy.ClanWar, []byte, error) {
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
	previous, raw, hasPrevious, changed, err := botClanSnapshotChanged(ctx, d.snapshots, d.snapshotPrefix, "cwlwar", clanTag, war, raw)
	if err != nil || !hasPrevious || !changed {
		return err
	}
	app.Stats.SetReady(botClansDomainName, true, "")
	if err := app.PublishEvent(ctx, platform.Event{
		Topic:   "cwl",
		ClanTag: clanTag,
		Value:   map[string]any{"type": "cwl_war_update", "clan_tag": clanTag, "war": string(raw), "league_group": rawCWLGroup(group)},
	}); err != nil {
		return err
	}
	if previous == nil {
		return nil
	}
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
		return app.PublishEvent(ctx, platform.Event{
			Topic:   "cwl",
			ClanTag: clanTag,
			Value:   map[string]any{"type": "cwl_new_attacks", "clan_tag": clanTag, "war": string(raw), "attacks": attacks, "league_group": rawCWLGroup(group)},
		})
	}
	return nil
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

func cloneReminder(in bson.M) bson.M {
	out := make(bson.M, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func stringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return typed
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return out
	case bson.A:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func intList(value any) []int {
	switch typed := value.(type) {
	case []int:
		return typed
	case []any:
		out := make([]int, 0, len(typed))
		for _, item := range typed {
			if value := intFromAny(item); value != 0 {
				out = append(out, value)
			}
		}
		return out
	case bson.A:
		out := make([]int, 0, len(typed))
		for _, item := range typed {
			if value := intFromAny(item); value != 0 {
				out = append(out, value)
			}
		}
		return out
	default:
		return nil
	}
}

func intFromAny(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int32:
		return int(typed)
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
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

func collectTargetStoreTags(ctx context.Context, targets clashtracker.TargetStore) ([]string, error) {
	if targets == nil {
		return nil, nil
	}
	list, err := targets.List(ctx)
	if err != nil {
		return nil, err
	}
	tags := make([]string, 0, len(list))
	for _, target := range list {
		if target.Key != "" {
			tags = append(tags, target.Key)
		}
	}
	return tags, nil
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

func parseReminderHours(raw string) (float64, error) {
	value := strings.TrimSpace(strings.TrimSuffix(raw, "hr"))
	return strconv.ParseFloat(value, 64)
}
