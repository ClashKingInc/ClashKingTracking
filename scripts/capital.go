package scripts

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

const (
	capitalDomainName = "capital"
	capitalCacheTTL   = 6 * time.Hour
)

type capitalTarget struct {
	Tag        string
	EmitEvents bool
}

type capitalDomain struct {
	mu      sync.RWMutex
	targets map[string]capitalTarget
	pool    *pgxpool.Pool
}

func NewCapitalDomain() platform.Domain {
	return &capitalDomain{targets: make(map[string]capitalTarget)}
}

func (d *capitalDomain) Name() string { return capitalDomainName }

func (d *capitalDomain) Run(ctx context.Context, app *platform.App) error {
	if app.Config.CapitalRequestsPerSecond <= 0 {
		return errors.New("capital.requests_per_second must be greater than zero")
	}
	if app.Config.TimescaleURL == "" || app.Valkey == nil {
		return errors.New("capital requires Timescale and Valkey")
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return err
	}
	d.pool = pool
	defer pool.Close()
	if err := d.refreshTargets(ctx, app); err != nil {
		return err
	}
	go d.runTargetRefresh(ctx, app)
	limiter, err := newTrackingLimiter(app.Config.CapitalRequestsPerSecond)
	if err != nil {
		return err
	}
	for {
		if !capitalWeekendActive(time.Now().UTC()) {
			if err := sleepOrDone(ctx, 15*time.Minute); err != nil {
				return err
			}
			continue
		}
		targets := d.targetSnapshot()
		if err := runBounded(ctx, platform.RequestConcurrency(app.Config.CapitalRequestsPerSecond), targets, func(workerCtx context.Context, target capitalTarget) error {
			return d.pollTarget(workerCtx, app, limiter, target)
		}); err != nil {
			return err
		}
		if len(targets) == 0 {
			if err := sleepOrDone(ctx, time.Minute); err != nil {
				return err
			}
		}
	}
}

func (d *capitalDomain) runTargetRefresh(ctx context.Context, app *platform.App) {
	ticker := time.NewTicker(time.Duration(app.Config.CapitalTargetRefreshSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := d.refreshTargets(ctx, app); err != nil {
				app.Logger.Error("capital target refresh failed", "err", err)
			}
		}
	}
}

func (d *capitalDomain) refreshTargets(ctx context.Context, app *platform.App) error {
	rows, err := d.pool.Query(ctx, `
		SELECT DISTINCT log.clan_tag
		FROM server_logs log
		JOIN servers server ON server.id = log.server_id
		WHERE log.clan_tag IS NOT NULL
		  AND log.disabled = false
		  AND server.last_command_at >= now() - interval '90 days'
		  AND log.type IN ('capital_donations', 'capital_attacks', 'raid_panel', 'capital_weekly_summary')
	`)
	if err != nil {
		return err
	}
	configured := make(map[string]capitalTarget)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			rows.Close()
			return err
		}
		configured[tag] = capitalTarget{Tag: tag, EmitEvents: true}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	values, err := app.Valkey.Do(ctx, app.Valkey.B().Hvals().Key(verifiedPlayerClanHashKey).Build()).ToArray()
	if err != nil {
		return err
	}
	for _, value := range values {
		tag, valueErr := value.ToString()
		if valueErr != nil || tag == "" {
			continue
		}
		if existing, ok := configured[tag]; ok {
			existing.EmitEvents = true
			configured[tag] = existing
		} else {
			configured[tag] = capitalTarget{Tag: tag}
		}
	}
	d.mu.Lock()
	previous := d.targets
	d.targets = configured
	d.mu.Unlock()
	for tag := range previous {
		if _, retained := configured[tag]; retained {
			continue
		}
		if err := app.Valkey.Do(ctx, app.Valkey.B().Del().Key(capitalCacheKey(app.Config.CapitalSnapshotPrefix, tag)).Build()).Error(); err != nil {
			return err
		}
	}
	app.Stats.SetTrackingTargets(capitalDomainName, len(configured))
	return nil
}

func (d *capitalDomain) targetSnapshot() []capitalTarget {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]capitalTarget, 0, len(d.targets))
	for _, target := range d.targets {
		out = append(out, target)
	}
	return out
}

func (d *capitalDomain) stillTargeted(tag string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	_, ok := d.targets[tag]
	return ok
}

func (d *capitalDomain) pollTarget(ctx context.Context, app *platform.App, limiter *clashy.Limiter, target capitalTarget) error {
	raids, err := retryLimitedClashFetch(ctx, app, limiter, func(fetchCtx context.Context) ([]clashy.RaidLogEntry, error) {
		start := time.Now()
		entries, fetchErr := app.Clash.GetRaidLog(fetchCtx, target.Tag, clashy.PageOptions{Limit: 1})
		app.Stats.RecordRequest(capitalDomainName, time.Since(start), fetchErr)
		return entries, fetchErr
	})
	if err != nil || len(raids) == 0 {
		return err
	}
	raid := raids[0]
	if raid.EndTime == nil || raid.EndTime.Time.IsZero() || !d.stillTargeted(target.Tag) {
		return nil
	}
	raw, err := json.Marshal(raid)
	if err != nil {
		return err
	}
	compressed, err := compressRaidPayload(raw)
	if err != nil {
		return err
	}
	key := capitalCacheKey(app.Config.CapitalSnapshotPrefix, target.Tag)
	previous, previousErr := app.Valkey.Do(ctx, app.Valkey.B().Get().Key(key).Build()).AsBytes()
	if previousErr != nil && !isValkeyMiss(previousErr) {
		return previousErr
	}
	if len(previous) > 0 && bytes.Equal(previous, compressed) {
		app.Stats.RecordTrackedTarget(capitalDomainName)
		return nil
	}
	previousRaid, hasPrevious := decodeCachedRaid(previous)
	participantTags := newRaidParticipantTags(previousRaid, raid)
	storeStart := time.Now()
	inserted, err := insertRaidPlayerTimers(ctx, d.pool, target.Tag, raid.EndTime.Time.UTC(), participantTags)
	if err != nil {
		return err
	}
	if len(participantTags) > 0 {
		app.Stats.RecordStore(capitalDomainName, time.Since(storeStart), len(participantTags), inserted)
	}
	if !d.stillTargeted(target.Tag) {
		return nil
	}
	if err := app.Valkey.Do(ctx, app.Valkey.B().Set().Key(key).Value(string(compressed)).Ex(capitalCacheTTL).Build()).Error(); err != nil {
		return err
	}
	app.Stats.RecordWrite(capitalDomainName, inserted+1)
	if target.EmitEvents && hasPrevious {
		event, eventErr := capitalRaidUpdateEvent(target.Tag, previousRaid, raw)
		if eventErr != nil {
			return eventErr
		}
		return app.PublishEvent(ctx, event)
	}
	app.Stats.RecordTrackedTarget(capitalDomainName)
	return nil
}

func capitalRaidUpdateEvent(clanTag string, previousRaw, currentRaw []byte) (platform.Event, error) {
	if !json.Valid(previousRaw) || !json.Valid(currentRaw) {
		return platform.Event{}, errors.New("capital update contains invalid raid JSON")
	}
	return platform.Event{
		Topic:   "capital",
		ClanTag: clanTag,
		Value: map[string]any{
			"type":          "raid_update",
			"clan_tag":      clanTag,
			"previous_raid": json.RawMessage(previousRaw),
			"raid":          json.RawMessage(currentRaw),
		},
	}, nil
}

func decodeCachedRaid(compressed []byte) ([]byte, bool) {
	if len(compressed) == 0 {
		return nil, false
	}
	raw, err := decompressRaidPayload(compressed)
	if err != nil || !json.Valid(raw) {
		return nil, false
	}
	return raw, true
}

func newRaidParticipantTags(previousRaw []byte, current clashy.RaidLogEntry) []string {
	previous := make(map[string]struct{})
	if len(previousRaw) > 0 {
		var raid clashy.RaidLogEntry
		if json.Unmarshal(previousRaw, &raid) == nil {
			for _, member := range raid.Members {
				if member.Tag != "" {
					previous[member.Tag] = struct{}{}
				}
			}
		}
	}
	seen := make(map[string]struct{}, len(current.Members))
	tags := make([]string, 0, len(current.Members))
	for _, member := range current.Members {
		if member.Tag == "" {
			continue
		}
		if _, exists := previous[member.Tag]; exists {
			continue
		}
		if _, duplicate := seen[member.Tag]; duplicate {
			continue
		}
		seen[member.Tag] = struct{}{}
		tags = append(tags, member.Tag)
	}
	return tags
}

func insertRaidPlayerTimers(ctx context.Context, pool *pgxpool.Pool, clanTag string, expiresAt time.Time, tags []string) (int, error) {
	if len(tags) == 0 || clanTag == "" || expiresAt.IsZero() {
		return 0, nil
	}
	result, err := pool.Exec(ctx, `
		INSERT INTO player_timers (player_tag, event_type, event_key, expires_at)
		SELECT tag, 'raid', $2, $3
		FROM unnest($1::text[]) tag
		ON CONFLICT (player_tag, event_type, event_key) DO NOTHING
	`, tags, clanTag, expiresAt)
	if err != nil {
		return 0, err
	}
	return int(result.RowsAffected()), nil
}

func capitalCacheKey(prefix, clanTag string) string { return prefix + clanTag }

func compressRaidPayload(raw []byte) ([]byte, error) {
	var out bytes.Buffer
	writer := gzip.NewWriter(&out)
	if _, err := writer.Write(raw); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func decompressRaidPayload(raw []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	var out bytes.Buffer
	_, err = out.ReadFrom(reader)
	return out.Bytes(), err
}

func capitalWeekendActive(now time.Time) bool {
	now = now.UTC()
	weekday := now.Weekday()
	if weekday == time.Friday {
		return now.Hour() >= 7
	}
	if weekday == time.Saturday || weekday == time.Sunday {
		return true
	}
	return weekday == time.Monday && now.Hour() < 7
}

func isValkeyMiss(err error) bool {
	return valkey.IsValkeyNil(err)
}
