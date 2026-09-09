package scripts

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/internal/wararchive"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	warDiscoveryDomainName = "war-discovery"
	cwlDomainName          = "cwl"
	warFinalizationGrace   = 6 * time.Hour
	warTargetCountRefresh  = 15 * time.Minute
)

type warDomainMode string

const (
	warDiscoveryMode warDomainMode = "discovery"
	cwlMode          warDomainMode = "cwl"
)

type warTargetKind string

const (
	activeWarTargets  warTargetKind = "active"
	dormantWarTargets warTargetKind = "dormant"
	cwlTargets        warTargetKind = "groups"
)

// War targets are clans whose public war logs can expose current war state. A pending
// schedule means the clan is already covered by an end-time fetch.
const activeWarTargetPredicateSQL = `
	public_war_log = true
	AND last_war_at >= now() - interval '30 days'
	AND NOT EXISTS (
	  SELECT 1
	  FROM war_schedule
	  WHERE source_clan_tag = basic_clan.tag OR opponent_tag = basic_clan.tag
	)
`

const activeWarTargetsSQL = `
	SELECT tag, name, cwl_league_id
	FROM basic_clan
	WHERE tag > $1
	  AND ` + activeWarTargetPredicateSQL + `
	ORDER BY tag
	LIMIT $2
`

const dormantWarTargetPredicateSQL = `
	public_war_log = true
	AND (last_war_at IS NULL OR last_war_at < now() - interval '30 days')
	AND NOT EXISTS (
	  SELECT 1 FROM war_schedule
	  WHERE source_clan_tag = basic_clan.tag OR opponent_tag = basic_clan.tag
	)
`

const dormantWarTargetsSQL = `
	SELECT tag, name, cwl_league_id
	FROM basic_clan
	WHERE tag > $1
	  AND ` + dormantWarTargetPredicateSQL + `
	ORDER BY tag
	LIMIT $2
`

const cwlDiscoveryTargetsSQL = `
	WITH current_clans AS MATERIALIZED (
	  SELECT DISTINCT known_clan.clan_tag
	  FROM cwl_groups known_group
	  JOIN cwl_group_clans known_clan ON known_clan.cwl_id = known_group.cwl_id
	  WHERE known_group.season >= to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM')
	    AND known_group.season < to_char(date_trunc('month', now() AT TIME ZONE 'UTC') + INTERVAL '1 month', 'YYYY-MM')
	)
	SELECT tag, name, COALESCE(cwl_league_id, 0)
	FROM basic_clan
	WHERE tag > $1
	  AND EXTRACT(DAY FROM now() AT TIME ZONE 'UTC') BETWEEN 1 AND 15
	  AND NOT EXISTS (
	    SELECT 1
	    FROM current_clans known_clan
	    WHERE known_clan.clan_tag = basic_clan.tag
	  )
	ORDER BY tag
	LIMIT $2
`

const cwlRefreshTargetsSQL = `
	WITH current_groups AS MATERIALIZED (
	  SELECT cwl_id
	  FROM cwl_groups
	  WHERE season >= to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM')
	    AND season < to_char(date_trunc('month', now() AT TIME ZONE 'UTC') + INTERVAL '1 month', 'YYYY-MM')
	    AND state <> 'ended'
	), representatives AS MATERIALIZED (
	  SELECT current_group.cwl_id, min(group_clan.clan_tag) AS clan_tag
	  FROM current_groups current_group
	  JOIN cwl_group_clans group_clan ON group_clan.cwl_id = current_group.cwl_id
	  JOIN basic_clan candidate_clan ON candidate_clan.tag = group_clan.clan_tag
	  GROUP BY current_group.cwl_id
	), group_wars AS MATERIALIZED (
	  SELECT DISTINCT current_group.cwl_id, schedule.end_time
	  FROM current_groups current_group
	  JOIN cwl_group_clans group_clan ON group_clan.cwl_id = current_group.cwl_id
	  JOIN war_schedule schedule
	    ON schedule.war_type = 'cwl'
	   AND group_clan.clan_tag IN (schedule.source_clan_tag, schedule.opponent_tag)
	   AND schedule.end_time > now()
	)
	SELECT clan.tag, clan.name, COALESCE(clan.cwl_league_id, 0)
	FROM representatives representative
	JOIN basic_clan clan ON clan.tag = representative.clan_tag
	WHERE representative.clan_tag > $1
	  AND NOT EXISTS (
	    SELECT 1
	    FROM group_wars active_war
	    WHERE active_war.cwl_id = representative.cwl_id
	      AND active_war.end_time - INTERVAL '24 hours' <= now()
	      AND EXISTS (
	        SELECT 1 FROM group_wars next_war
	        WHERE next_war.cwl_id = active_war.cwl_id
	          AND next_war.end_time > active_war.end_time
	      )
	  )
	ORDER BY representative.clan_tag
	LIMIT $2
`

const activeWarTargetCountSQL = `SELECT count(*) FROM basic_clan WHERE ` + activeWarTargetPredicateSQL
const dormantWarTargetCountSQL = `SELECT count(*) FROM basic_clan WHERE ` + dormantWarTargetPredicateSQL

type warsDomain struct {
	name    string
	mode    warDomainMode
	store   warStore
	targets warTargetSource
	limiter *clashy.Limiter
	now     func() time.Time

	mu        sync.Mutex
	scheduled map[string]time.Time
}

// warFetchRequest is the queue boundary between Run and do. StoreOnly requests are end-time
// fetches and must include the durable schedule metadata needed to finish the war safely.
type warFetchRequest struct {
	ClanTag     string
	OpponentTag string
	ScheduleKey string
	WarID       int32
	PrepTime    time.Time
	EndTime     time.Time
	WarTag      string
	StoreOnly   bool
	StatsName   string
}

// warQueue rejects incomplete work before it can reach Clash API fetches or persistence.
type warQueue struct {
	items []warFetchRequest
}

func (q *warQueue) Enqueue(req warFetchRequest) error {
	if strings.TrimSpace(req.ClanTag) == "" {
		return errors.New("war queue: clan tag is required")
	}
	if req.StoreOnly {
		if req.ScheduleKey == "" {
			return errors.New("war queue: schedule key is required for store work")
		}
		if req.WarID <= 0 {
			return errors.New("war queue: war id is required for store work")
		}
		if strings.TrimSpace(req.OpponentTag) == "" {
			return errors.New("war queue: opponent tag is required for store work")
		}
		if req.PrepTime.IsZero() || req.EndTime.IsZero() {
			return errors.New("war queue: prep and end times are required for store work")
		}
	}
	q.items = append(q.items, req)
	return nil
}

type warStore interface {
	LoadPendingSchedules(context.Context) ([]models.WarScheduleRow, error)
	LoadDueSchedules(context.Context, int) ([]models.WarScheduleRow, error)
	Reschedule(context.Context, string, time.Time) error
	DeleteSchedule(context.Context, string) error
	LoadCWLLeague(context.Context, string) (int, error)
	LoadStoredCWLGroupLeague(context.Context, string) (int, bool, error)
	ResolveCWLGroupLeague(context.Context, []string, int) (int, error)
	KnownCWLWarTags(context.Context, []string) (map[string]int, error)
	LoadActivePlayerTimers(context.Context, string) ([]models.PlayerTimerRow, error)
	DeleteExpiredPlayerTimers(context.Context) (int, error)
	Store(context.Context, models.WarIngest) error
	ShiftMaintenance(context.Context, time.Duration) error
	Close() error
}

type warTargetSource interface {
	NextTargetBatch(context.Context, int) ([]models.BasicClanRow, error)
	NextDormantTargetBatch(context.Context, int) ([]models.BasicClanRow, error)
	NextCWLDiscoveryTargetBatch(context.Context, int) ([]models.BasicClanRow, error)
	NextCWLRefreshTargetBatch(context.Context, int) ([]models.BasicClanRow, error)
	CountTargets(context.Context, warTargetKind) (int, error)
	Close() error
}

func NewWarDiscoveryDomain() platform.Domain {
	return &warsDomain{
		name:      warDiscoveryDomainName,
		mode:      warDiscoveryMode,
		now:       time.Now,
		scheduled: make(map[string]time.Time),
	}
}

func NewCWLDomain() platform.Domain {
	return &warsDomain{name: cwlDomainName, mode: cwlMode, now: time.Now, scheduled: make(map[string]time.Time)}
}

func (d *warsDomain) Name() string { return d.name }

func (d *warsDomain) currentTime() time.Time {
	if d.now != nil {
		return d.now()
	}
	return time.Now()
}

func (d *warsDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateWarConfig(app, d.mode); err != nil {
		return err
	}
	store, err := d.openStore(ctx, app)
	if err != nil {
		return err
	}
	d.store = store
	defer store.Close()
	targets, err := d.openTargetSource(ctx, app)
	if err != nil {
		return err
	}
	d.targets = targets
	defer targets.Close()
	limiter, err := newWarLimiter(app, d.mode)
	if err != nil {
		return err
	}
	d.limiter = limiter
	warLimiter := limiter
	if d.mode == cwlMode {
		warLimiter, err = clashy.NewLimiter(app.Config.CWLWarRequestsPerSecond, platform.RequestConcurrency(app.Config.CWLWarRequestsPerSecond))
		if err != nil {
			return err
		}
	}
	runCtx, stopBackground := context.WithCancel(ctx)
	var background sync.WaitGroup
	defer func() {
		stopBackground()
		background.Wait()
	}()

	if d.mode == cwlMode {
		d.runCWLLoop(runCtx, app, limiter, warLimiter)
		return runCtx.Err()
	}
	d.refreshWarTargetCounts(runCtx, app)
	dormantLimiter, err := clashy.NewLimiter(app.Config.WarDiscoveryDormantRequestsPerSecond, platform.RequestConcurrency(app.Config.WarDiscoveryDormantRequestsPerSecond))
	if err != nil {
		return err
	}
	background.Add(5)
	go func() {
		defer background.Done()
		d.runWarTargetCountLoop(runCtx, app)
	}()
	go func() {
		defer background.Done()
		d.runDueWarScheduleLoop(runCtx, app)
	}()
	go func() {
		defer background.Done()
		d.runPlayerTimerCleanupLoop(runCtx, app)
	}()
	errCh := make(chan error, 2)
	go func() {
		defer background.Done()
		errCh <- d.runDiscoveryLoop(runCtx, app, limiter, false)
	}()
	go func() {
		defer background.Done()
		errCh <- d.runDiscoveryLoop(runCtx, app, dormantLimiter, true)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		app.Stats.SetReady(d.name, false, err.Error())
		return err
	}
}

func validateWarConfig(app *platform.App, mode warDomainMode) error {
	cfg := app.Config
	if mode == cwlMode {
		if cfg.CWLRequestsPerSecond <= 0 {
			return errors.New("cwl.requests_per_second must be greater than zero when cwl is enabled")
		}
		if cfg.CWLWarRequestsPerSecond <= 0 {
			return errors.New("cwl.war_requests_per_second must be greater than zero when cwl is enabled")
		}
		if cfg.CWLSyncSeconds <= 0 {
			return errors.New("cwl.sync_seconds must be greater than zero when cwl is enabled")
		}
	} else {
		if cfg.WarDiscoveryActiveRequestsPerSecond <= 0 {
			return errors.New("war_discovery.active_requests_per_second must be greater than zero when war discovery is enabled")
		}
		if cfg.WarDiscoveryDormantRequestsPerSecond <= 0 {
			return errors.New("war_discovery.dormant_requests_per_second must be greater than zero when war discovery is enabled")
		}
	}
	if cfg.TargetPageMultiplier <= 0 {
		return errors.New("target_page_multiplier must be greater than zero when wars is enabled")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_* connection variables are required when wars is enabled")
	}
	return nil
}

func (d *warsDomain) openStore(ctx context.Context, app *platform.App) (warStore, error) {
	if app.Config.DryRun || app.Config.MockDB {
		return newMemoryWarStore(), nil
	}
	return newTimescaleWarStore(ctx, app.Config.TimescaleURL)
}

func (d *warsDomain) openTargetSource(ctx context.Context, app *platform.App) (warTargetSource, error) {
	if app.Config.DryRun || app.Config.MockDB {
		return newMemoryWarTargetSource(nil), nil
	}
	return newTimescaleWarTargetSource(ctx, app.Config.TimescaleURL)
}

type timescaleWarTargetSource struct {
	pool               *pgxpool.Pool
	mu                 sync.Mutex
	cursor             string
	dormantCursor      string
	cwlDiscoveryCursor string
	cwlRefreshCursor   string
}

func newTimescaleWarTargetSource(ctx context.Context, dsn string) (*timescaleWarTargetSource, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleWarTargetSource{pool: pool}, nil
}

func (s *timescaleWarTargetSource) Close() error {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
	return nil
}

func (s *timescaleWarTargetSource) NextTargetBatch(ctx context.Context, limit int) ([]models.BasicClanRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	targets, cursor, err := s.nextTargetBatch(ctx, activeWarTargetsSQL, limit, s.cursor)
	s.cursor = cursor
	return targets, err
}

func (s *timescaleWarTargetSource) NextDormantTargetBatch(ctx context.Context, limit int) ([]models.BasicClanRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	targets, cursor, err := s.nextTargetBatch(ctx, dormantWarTargetsSQL, limit, s.dormantCursor)
	s.dormantCursor = cursor
	return targets, err
}

func (s *timescaleWarTargetSource) NextCWLDiscoveryTargetBatch(ctx context.Context, limit int) ([]models.BasicClanRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	targets, cursor, err := s.nextTargetBatch(ctx, cwlDiscoveryTargetsSQL, limit, s.cwlDiscoveryCursor)
	s.cwlDiscoveryCursor = cursor
	return targets, err
}

func (s *timescaleWarTargetSource) NextCWLRefreshTargetBatch(ctx context.Context, limit int) ([]models.BasicClanRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	targets, cursor, err := s.nextTargetBatch(ctx, cwlRefreshTargetsSQL, limit, s.cwlRefreshCursor)
	s.cwlRefreshCursor = cursor
	return targets, err
}

func (s *timescaleWarTargetSource) CountTargets(ctx context.Context, kind warTargetKind) (int, error) {
	query := ""
	switch kind {
	case activeWarTargets:
		query = activeWarTargetCountSQL
	case dormantWarTargets:
		query = dormantWarTargetCountSQL
	default:
		return 0, fmt.Errorf("unknown war target kind %q", kind)
	}
	var count int
	if err := s.pool.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, warStoreError("count targets", err)
	}
	return count, nil
}

func (s *timescaleWarTargetSource) nextTargetBatch(ctx context.Context, query string, limit int, cursor string) ([]models.BasicClanRow, string, error) {
	if limit <= 0 {
		return nil, cursor, nil
	}
	rows, err := s.pool.Query(ctx, query, cursor, limit+1)
	if err != nil {
		return nil, cursor, warStoreError("targets", err)
	}
	defer rows.Close()
	targets := make([]models.BasicClanRow, 0, limit+1)
	for rows.Next() {
		var row models.BasicClanRow
		if err := rows.Scan(&row.Tag, &row.Name, &row.CWLLeagueID); err != nil {
			return nil, cursor, err
		}
		targets = append(targets, row)
	}
	if err := rows.Err(); err != nil {
		return nil, cursor, err
	}
	nextCursor := ""
	if len(targets) > limit {
		// Fetch one extra row so we know whether to advance or wrap without a COUNT query.
		nextCursor = targets[limit-1].Tag
		targets = targets[:limit]
	}
	return targets, nextCursor, nil
}

type memoryWarTargetSource struct {
	mu        sync.Mutex
	targets   []models.BasicClanRow
	cursor    int
	cwlCursor int
}

func newMemoryWarTargetSource(targets []models.BasicClanRow) *memoryWarTargetSource {
	return &memoryWarTargetSource{targets: append([]models.BasicClanRow(nil), targets...)}
}

func (s *memoryWarTargetSource) Close() error { return nil }

func (s *memoryWarTargetSource) NextTargetBatch(_ context.Context, limit int) ([]models.BasicClanRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, cursor := memoryWarTargetBatch(s.targets, s.cursor, limit)
	s.cursor = cursor
	return out, nil
}

func (s *memoryWarTargetSource) NextCWLDiscoveryTargetBatch(_ context.Context, limit int) ([]models.BasicClanRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, cursor := memoryWarTargetBatch(s.targets, s.cwlCursor, limit)
	s.cwlCursor = cursor
	return out, nil
}

func (s *memoryWarTargetSource) NextCWLRefreshTargetBatch(_ context.Context, _ int) ([]models.BasicClanRow, error) {
	return nil, nil
}

func (s *memoryWarTargetSource) NextDormantTargetBatch(_ context.Context, _ int) ([]models.BasicClanRow, error) {
	return nil, nil
}

func (s *memoryWarTargetSource) CountTargets(_ context.Context, kind warTargetKind) (int, error) {
	if kind == dormantWarTargets {
		return 0, nil
	}
	if kind == cwlTargets {
		return 0, fmt.Errorf("CWL target totals are intentionally not counted")
	}
	return len(s.targets), nil
}

func memoryWarTargetBatch(targets []models.BasicClanRow, cursor int, limit int) ([]models.BasicClanRow, int) {
	if limit <= 0 || len(targets) == 0 {
		return nil, cursor
	}
	if cursor >= len(targets) {
		cursor = 0
	}
	end := cursor + limit
	if end > len(targets) {
		end = len(targets)
	}
	out := append([]models.BasicClanRow(nil), targets[cursor:end]...)
	if end == len(targets) {
		cursor = 0
	} else {
		cursor = end
	}
	return out, cursor
}

func newWarLimiter(app *platform.App, mode warDomainMode) (*clashy.Limiter, error) {
	if mode == cwlMode {
		return clashy.NewLimiter(app.Config.CWLRequestsPerSecond, platform.RequestConcurrency(app.Config.CWLRequestsPerSecond))
	}
	return clashy.NewLimiter(app.Config.WarDiscoveryActiveRequestsPerSecond, app.Config.WarDiscoveryMaxInFlight)
}

func (d *warsDomain) refreshWarTargetCounts(ctx context.Context, app *platform.App) {
	kinds := []warTargetKind{activeWarTargets, dormantWarTargets}
	if d.mode == cwlMode {
		return
	}
	for _, kind := range kinds {
		count, err := d.targets.CountTargets(ctx, kind)
		statsName := trackingProgressName(d.name, string(kind))
		if err != nil {
			app.Logger.Error("count war tracking targets failed", "domain", statsName, "err", err)
			continue
		}
		app.Stats.SetTrackingTargets(statsName, count)
	}
}

func (d *warsDomain) runWarTargetCountLoop(ctx context.Context, app *platform.App) {
	ticker := time.NewTicker(warTargetCountRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.refreshWarTargetCounts(ctx, app)
		}
	}
}

func (d *warsDomain) runCycle(ctx context.Context, app *platform.App, limiter *clashy.Limiter) error {
	return d.runDiscoveryCycle(ctx, app, limiter, false)
}

func (d *warsDomain) runDiscoveryLoop(ctx context.Context, app *platform.App, limiter *clashy.Limiter, dormant bool) error {
	statsName := trackingProgressName(d.name, string(activeWarTargets))
	if dormant {
		statsName = trackingProgressName(d.name, string(dormantWarTargets))
	}
	for {
		start := time.Now()
		if err := d.runDiscoveryCycle(ctx, app, limiter, dormant); err != nil {
			return err
		}
		app.Stats.RecordProcess(statsName, time.Since(start))
		app.Stats.SetReady(statsName, true, "")
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func (d *warsDomain) runDiscoveryCycle(ctx context.Context, app *platform.App, limiter *clashy.Limiter, dormant bool) error {
	rate := app.Config.WarDiscoveryActiveRequestsPerSecond
	statsName := trackingProgressName(d.name, string(activeWarTargets))
	var targets []models.BasicClanRow
	var err error
	if dormant {
		rate = app.Config.WarDiscoveryDormantRequestsPerSecond
		statsName = trackingProgressName(d.name, string(dormantWarTargets))
		targets, err = d.targets.NextDormantTargetBatch(ctx, rate*app.Config.TargetPageMultiplier)
	} else {
		targets, err = d.targets.NextTargetBatch(ctx, rate*app.Config.TargetPageMultiplier)
	}
	if err != nil {
		return err
	}
	queue := &warQueue{}
	for _, target := range targets {
		if err := queue.Enqueue(warFetchRequest{ClanTag: target.Tag, StatsName: statsName}); err != nil {
			return err
		}
	}
	err = d.processQueue(ctx, app, limiter, queue.items)
	for range targets {
		app.Stats.RecordTrackedTarget(statsName)
	}
	return err
}

func (d *warsDomain) processQueue(ctx context.Context, app *platform.App, limiter *clashy.Limiter, requests []warFetchRequest) error {
	// The limiter caps request starts while the larger in-flight pool prevents
	// normal proxy latency from lowering the configured starts-per-second rate.
	maxInFlight := app.Config.WarDiscoveryMaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = app.Config.WarDiscoveryActiveRequestsPerSecond
	}
	slots := make(chan struct{}, maxInFlight)
	errCh := make(chan error, len(requests))
	var wg sync.WaitGroup
	for _, req := range requests {
		req := req
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			ingest, err := d.do(ctx, app, limiter, req)
			if err == nil {
				err = d.storeIngest(ctx, app, ingest)
			}
			if err == nil && req.StoreOnly {
				d.mu.Lock()
				delete(d.scheduled, req.ScheduleKey)
				d.mu.Unlock()
			}
			if err != nil && ctx.Err() == nil {
				app.Logger.Error("war processing failed", "err", err)
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *warsDomain) do(ctx context.Context, app *platform.App, limiter *clashy.Limiter, req warFetchRequest) (models.WarIngest, error) {
	war, err := d.fetchWarForRequest(ctx, app, limiter, req)
	if err != nil {
		if isSkippableWarFetchError(err) {
			if req.StoreOnly {
				return models.WarIngest{}, fmt.Errorf("scheduled war %d is not available yet: %w", req.WarID, err)
			}
			return models.WarIngest{}, nil
		}
		return models.WarIngest{}, err
	}
	if war == nil {
		if req.StoreOnly {
			return models.WarIngest{}, fmt.Errorf("scheduled war %d returned no matching war", req.WarID)
		}
		return models.WarIngest{}, nil
	}
	if req.StoreOnly && war.State != clashy.WarStateEnded {
		return models.WarIngest{}, fmt.Errorf("scheduled war %d is still in state %s", req.WarID, war.State)
	}
	ingest, err := buildWarIngest(*war, req.ClanTag, req.StoreOnly, req.WarTag, req.ScheduleKey, req.WarID)
	if err != nil {
		return models.WarIngest{}, err
	}
	if req.StoreOnly && len(ingest.IndexRows) == 0 {
		return models.WarIngest{}, fmt.Errorf("scheduled war %d produced no finished ingest", req.WarID)
	}
	return ingest, nil
}

func (d *warsDomain) fetchWarForRequest(ctx context.Context, app *platform.App, limiter *clashy.Limiter, req warFetchRequest) (*clashy.ClanWar, error) {
	statsName := req.StatsName
	if statsName == "" {
		statsName = d.name
	}
	if req.WarTag != "" {
		war, err := d.fetchOneWarWithStats(ctx, app, limiter, req.WarTag, true, statsName)
		if err != nil || war == nil || !req.StoreOnly {
			return war, err
		}
		if !scheduledWarMatches(req, *war) {
			return nil, fmt.Errorf("tagged CWL response does not match schedule %s", req.ScheduleKey)
		}
		return war, nil
	}

	clanTags := []string{req.ClanTag}
	if req.StoreOnly && req.OpponentTag != "" && req.OpponentTag != req.ClanTag {
		clanTags = append(clanTags, req.OpponentTag)
	}
	var lastErr error
	for _, clanTag := range clanTags {
		war, err := d.fetchOneWarWithStats(ctx, app, limiter, clanTag, false, statsName)
		if err != nil {
			if req.StoreOnly && isSkippableWarFetchError(err) {
				lastErr = err
				continue
			}
			return nil, err
		}
		if !req.StoreOnly {
			return war, nil
		}
		if war != nil && scheduledWarMatches(req, *war) {
			return war, nil
		}
		lastErr = fmt.Errorf("clan %s no longer exposes schedule %s", clanTag, req.ScheduleKey)
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, nil
}

func (d *warsDomain) fetchOneWarWithStats(ctx context.Context, app *platform.App, limiter *clashy.Limiter, tag string, leagueWar bool, statsName string) (*clashy.ClanWar, error) {
	return retryLimitedClashFetch(ctx, app, limiter, func(fetchCtx context.Context) (*clashy.ClanWar, error) {
		start := time.Now()
		var war *clashy.ClanWar
		var fetchErr error
		if leagueWar {
			wars, err := app.Clash.GetLeagueWars(fetchCtx, []string{tag})
			fetchErr = err
			if len(wars) > 0 {
				war = &wars[0]
			}
		} else {
			war, fetchErr = app.Clash.GetClanWar(fetchCtx, tag)
		}
		app.Stats.RecordRequest(statsName, time.Since(start), fetchErr)
		return war, fetchErr
	})
}

func scheduledWarMatches(req warFetchRequest, war clashy.ClanWar) bool {
	if !req.StoreOnly || req.ScheduleKey == "" || war.PreparationStartTime == nil || war.Clan == nil || war.Opponent == nil {
		return false
	}
	return models.ComputeWarKey(war.Clan.Tag, war.Opponent.Tag, war.PreparationStartTime.Time.UTC()) == req.ScheduleKey
}

func isSkippableWarFetchError(err error) bool {
	var forbidden *clashy.Forbidden
	var privateWarLog *clashy.PrivateWarLog
	var notFound *clashy.NotFound
	return errors.As(err, &forbidden) || errors.As(err, &privateWarLog) || errors.As(err, &notFound)
}

func (d *warsDomain) storeIngest(ctx context.Context, app *platform.App, ingest models.WarIngest) error {
	if len(ingest.IndexRows) == 0 && len(ingest.ArchivePayload) == 0 && len(ingest.Schedules) == 0 && len(ingest.PlayerTimers) == 0 && len(ingest.CWLGroups) == 0 {
		return nil
	}
	if err := d.store.Store(ctx, ingest); err != nil {
		return err
	}
	// Only arm local timers after the schedule row is durable.
	for _, schedule := range ingest.Schedules {
		if err := app.PublishEvent(ctx, platform.Event{
			Topic:   "war_schedule",
			ClanTag: schedule.SourceClanTag,
			Value: map[string]any{
				"type": "war_available", "schedule_key": schedule.ScheduleKey,
			},
		}); err != nil {
			return err
		}
	}
	app.Stats.RecordWrite(d.name, len(ingest.IndexRows)+len(ingest.Schedules)+len(ingest.PlayerTimers)+len(ingest.CWLGroups))
	app.Stats.SetQueueDepth(d.name, len(ingest.Schedules))
	return nil
}

func buildWarIngest(war clashy.ClanWar, sourceClanTag string, finished bool, warTag, scheduleKey string, warID int32) (models.WarIngest, error) {
	if war.PreparationStartTime == nil || war.EndTime == nil {
		return models.WarIngest{}, nil
	}
	if war.Clan == nil || war.Opponent == nil || war.Clan.Tag == "" || war.Opponent.Tag == "" {
		return models.WarIngest{}, nil
	}
	prepAt := war.PreparationStartTime.Time.UTC()
	endAt := war.EndTime.Time.UTC()
	if finished && warID <= 0 {
		return models.WarIngest{}, errors.New("finished war is missing its SQL war ID")
	}
	if scheduleKey == "" {
		scheduleKey = models.ComputeWarKey(war.Clan.Tag, war.Opponent.Tag, prepAt)
	}
	warType := war.Type()
	if warTag != "" {
		warType = "cwl"
	}
	if !finished {
		// Discovery creates an end-time schedule, including an immediately due
		// one when it first observes an already-ended league round. Permanent war
		// rows are always written by the fenced finalizer fetch.
		return models.WarIngest{
			Schedules: []models.WarScheduleRow{{
				ScheduleKey:   scheduleKey,
				WarID:         warID,
				SourceClanTag: sourceClanTag,
				OpponentTag:   opponentTagForSource(sourceClanTag, war),
				PrepTime:      prepAt,
				EndTime:       endAt,
				NextRunAt:     endAt,
				WarType:       warType,
				WarTag:        warTag,
			}},
			PlayerTimers: playerWarTimerRows(scheduleKey, war.Clan, war.Opponent, endAt),
		}, nil
	}
	if war.StartTime == nil {
		return models.WarIngest{}, errors.New("finished war is missing startTime")
	}
	startAt := war.StartTime.Time.UTC()
	clan, opponent := canonicalWarSides(war.Clan, war.Opponent)
	archiveWar := canonicalArchiveWar(war, clan, opponent, warType, warTag, prepAt, startAt, endAt)
	archivePayload, err := wararchive.Marshal(archiveWar)
	if err != nil {
		return models.WarIngest{}, fmt.Errorf("marshal finished war archive: %w", err)
	}
	indexRows := []models.WarLogIndexRow{
		warIndexRow(warID, clan, opponent, prepAt, startAt, endAt, war, warType, warTag),
	}
	ingest := models.WarIngest{
		IndexRows:           indexRows,
		ArchivePayload:      archivePayload,
		ArchiveParticipants: archiveParticipants(archiveWar),
	}
	ingest.FinishedScheduleKey = scheduleKey
	ingest.FinishedWarID = warID
	return ingest, nil
}

func canonicalArchiveWar(war clashy.ClanWar, clan, opponent *clashy.WarClan, warType, warTag string, prepAt, startAt, endAt time.Time) wararchive.War {
	attacksPerMember := 1
	if warType == "random" {
		attacksPerMember = 2
	}
	return wararchive.War{
		WarTag: warTag, State: strings.ToLower(string(war.State)),
		TeamSize: war.TeamSize, AttacksPerMember: attacksPerMember,
		PreparationStartTime: prepAt, StartTime: startAt, EndTime: endAt,
		BattleModifier: wararchive.NormalizeBattleModifier(string(war.BattleModifier)),
		Clan:           canonicalArchiveClan(clan), Opponent: canonicalArchiveClan(opponent),
	}
}

func canonicalArchiveClan(clan *clashy.WarClan) wararchive.Clan {
	if clan == nil {
		return wararchive.Clan{Members: []wararchive.Member{}}
	}
	members := make([]wararchive.Member, 0, len(clan.Members))
	for _, member := range clan.Members {
		attacks := make([]wararchive.Attack, 0, len(member.Attacks))
		for _, attack := range member.Attacks {
			attacks = append(attacks, wararchive.Attack{
				DefenderTag: attack.DefenderTag, Stars: attack.Stars,
				DestructionPercentage: int(attack.Destruction), Duration: attack.Duration, Order: attack.Order,
			})
		}
		members = append(members, wararchive.Member{
			Tag: member.Tag, Name: member.Name, TownhallLevel: member.Townhall,
			MapPosition: member.MapPosition, Attacks: attacks,
		})
	}
	return wararchive.Clan{
		Tag: clan.Tag, Name: clan.Name, BadgeToken: badgeToken(clan.Badge), ClanLevel: clan.Level,
		Attacks: clan.Attacks, Stars: clan.Stars, DestructionPercentage: clan.Destruction, Members: members,
	}
}

func archiveParticipants(war wararchive.War) []string {
	seen := make(map[string]struct{}, len(war.Clan.Members)+len(war.Opponent.Members))
	participants := make([]string, 0, len(seen))
	for _, side := range []wararchive.Clan{war.Clan, war.Opponent} {
		for _, member := range side.Members {
			if member.Tag == "" {
				continue
			}
			if _, exists := seen[member.Tag]; exists {
				continue
			}
			seen[member.Tag] = struct{}{}
			participants = append(participants, member.Tag)
		}
	}
	sort.Strings(participants)
	return participants
}

func playerWarTimerRows(scheduleKey string, clan, opponent *clashy.WarClan, endAt time.Time) []models.PlayerTimerRow {
	if scheduleKey == "" || clan == nil || opponent == nil || clan.Tag == "" || opponent.Tag == "" || endAt.IsZero() {
		return nil
	}
	rows := make([]models.PlayerTimerRow, 0, len(clan.Members)+len(opponent.Members))
	seen := make(map[string]struct{}, len(clan.Members)+len(opponent.Members))
	appendSide := func(members []clashy.ClanWarMember) {
		for _, member := range members {
			if member.Tag == "" {
				continue
			}
			if _, exists := seen[member.Tag]; exists {
				continue
			}
			seen[member.Tag] = struct{}{}
			rows = append(rows, models.PlayerTimerRow{PlayerTag: member.Tag, EventType: "war", EventKey: scheduleKey, ExpiresAt: endAt})
		}
	}
	appendSide(clan.Members)
	appendSide(opponent.Members)
	return rows
}

func canonicalWarSides(clan, opponent *clashy.WarClan) (*clashy.WarClan, *clashy.WarClan) {
	if clan != nil && opponent != nil && clan.Tag > opponent.Tag {
		return opponent, clan
	}
	return clan, opponent
}

func warIndexRow(warID int32, clan, opponent *clashy.WarClan, prepAt, startAt, endAt time.Time, war clashy.ClanWar, warType, warTag string) models.WarLogIndexRow {
	attacksPerMember := 1
	if warType == "random" {
		attacksPerMember = 2
	}
	return models.WarLogIndexRow{
		WarID:                         warID,
		ClanTag:                       clan.Tag,
		OpponentTag:                   opponent.Tag,
		PrepTime:                      prepAt,
		StartTime:                     startAt,
		EndTime:                       endAt,
		Size:                          war.TeamSize,
		AttacksPerMember:              attacksPerMember,
		WarType:                       warType,
		State:                         string(war.State),
		BattleModifier:                wararchive.NormalizeBattleModifier(string(war.BattleModifier)),
		WarTag:                        warTag,
		ClanName:                      clan.Name,
		OpponentName:                  opponent.Name,
		ClanBadgeToken:                badgeToken(clan.Badge),
		OpponentBadgeToken:            badgeToken(opponent.Badge),
		ClanLevel:                     clan.Level,
		OpponentClanLevel:             opponent.Level,
		ClanAttacks:                   clan.Attacks,
		OpponentAttacks:               opponent.Attacks,
		ClanStars:                     clan.Stars,
		OpponentStars:                 opponent.Stars,
		ClanDestructionPercentage:     clan.Destruction,
		OpponentDestructionPercentage: opponent.Destruction,
	}
}

func opponentTagForSource(source string, war clashy.ClanWar) string {
	source = clashy.CorrectTag(source)
	if war.Clan != nil && war.Clan.Tag == source && war.Opponent != nil {
		return war.Opponent.Tag
	}
	if war.Opponent != nil && war.Opponent.Tag == source && war.Clan != nil {
		return war.Clan.Tag
	}
	if war.Opponent != nil {
		return war.Opponent.Tag
	}
	return ""
}

func (d *warsDomain) runDueWarScheduleLoop(ctx context.Context, app *platform.App) {
	statsName := trackingProgressName(d.name, "finalization")
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	batchLimit := max(100, app.Config.WarDiscoveryActiveRequestsPerSecond*5)
	workers := platform.RequestConcurrency(app.Config.WarDiscoveryActiveRequestsPerSecond)
	for {
		started := time.Now()
		var passErr error
		schedules, err := d.store.LoadDueSchedules(ctx, batchLimit)
		if err != nil {
			app.Logger.Error("load due war schedules failed", "err", err)
			passErr = err
		} else {
			app.Stats.SetQueueDepth(statsName, len(schedules))
			var passErrMu sync.Mutex
			runErr := runBounded(ctx, workers, schedules, func(workerCtx context.Context, schedule models.WarScheduleRow) error {
				scheduleErr := d.processDueWarSchedule(workerCtx, app, statsName, schedule)
				if scheduleErr != nil {
					passErrMu.Lock()
					passErr = scheduleErr
					passErrMu.Unlock()
				}
				if workerCtx.Err() != nil {
					return workerCtx.Err()
				}
				return nil
			})
			if runErr != nil && ctx.Err() != nil {
				return
			}
		}
		app.Stats.RecordProcess(statsName, time.Since(started))
		if passErr != nil {
			app.Stats.SetReady(statsName, false, passErr.Error())
		} else {
			app.Stats.SetReady(statsName, true, "")
		}
		if len(schedules) == batchLimit && passErr == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *warsDomain) processDueWarSchedule(ctx context.Context, app *platform.App, statsName string, schedule models.WarScheduleRow) error {
	queue := &warQueue{}
	if err := queue.Enqueue(warFetchRequest{
		ClanTag: schedule.SourceClanTag, OpponentTag: schedule.OpponentTag,
		ScheduleKey: schedule.ScheduleKey, WarID: schedule.WarID,
		PrepTime: schedule.PrepTime, EndTime: schedule.EndTime,
		WarTag: schedule.WarTag, StoreOnly: true, StatsName: statsName,
	}); err != nil {
		app.Logger.Error("invalid due war schedule", "err", err)
		return err
	}
	if err := d.processQueue(ctx, app, d.limiter, queue.items); err != nil {
		now := time.Now().UTC()
		if !now.Before(schedule.EndTime.Add(warFinalizationGrace)) {
			if deleteErr := d.store.DeleteSchedule(ctx, schedule.ScheduleKey); deleteErr != nil {
				app.Logger.Error("expired unavailable war schedule cleanup failed", "schedule_key", schedule.ScheduleKey, "err", deleteErr)
			} else {
				app.Logger.Warn("abandoned unavailable ended war after finalization grace", "schedule_key", schedule.ScheduleKey, "err", err)
			}
		} else {
			app.Logger.Error("final war fetch failed; retrying in one minute", "schedule_key", schedule.ScheduleKey, "err", err)
			_ = d.store.Reschedule(ctx, schedule.ScheduleKey, now.Add(time.Minute))
		}
		return err
	}
	return nil
}

func (d *warsDomain) runCWLLoop(ctx context.Context, app *platform.App, groupLimiter, warLimiter *clashy.Limiter) {
	statsName := trackingProgressName(d.name, string(cwlTargets))
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		start := time.Now()
		if utils.IsCWL(d.currentTime()) {
			if err := d.syncCWLGroups(ctx, app, groupLimiter, warLimiter); err != nil {
				app.Logger.Error("cwl sync failed", "err", err)
				app.Stats.RecordProcess(statsName, time.Since(start))
				app.Stats.SetReady(statsName, false, err.Error())
				timer.Reset(time.Duration(app.Config.CWLSyncSeconds) * time.Second)
				continue
			}
		}
		app.Stats.RecordProcess(statsName, time.Since(start))
		app.Stats.SetReady(statsName, true, "")
		timer.Reset(time.Duration(app.Config.CWLSyncSeconds) * time.Second)
	}
}

func (d *warsDomain) syncCWLGroups(ctx context.Context, app *platform.App, groupLimiter, warLimiter *clashy.Limiter) error {
	limit := app.Config.CWLRequestsPerSecond * app.Config.TargetPageMultiplier
	refreshTargets, err := d.targets.NextCWLRefreshTargetBatch(ctx, limit)
	if err != nil {
		return err
	}
	discoveryTargets, err := d.targets.NextCWLDiscoveryTargetBatch(ctx, limit)
	if err != nil {
		return err
	}
	// Refresh known groups first so a direct or restart-time pass catches newly
	// exposed rounds before spending the request budget on first-time discovery.
	targets := append(refreshTargets, discoveryTargets...)
	return d.syncCWLTargets(ctx, app, groupLimiter, warLimiter, targets, false)
}

func (d *warsDomain) syncCWLTargets(ctx context.Context, app *platform.App, groupLimiter, warLimiter *clashy.Limiter, targets []models.BasicClanRow, strict bool) error {
	statsName := trackingProgressName(d.name, string(cwlTargets))
	season := utils.CurrentSeason(d.currentTime())
	deduper := newCWLSyncDeduper()
	return runBounded(ctx, platform.RequestConcurrency(app.Config.CWLRequestsPerSecond), targets, func(workerCtx context.Context, target models.BasicClanRow) error {
		defer app.Stats.RecordTrackedTarget(statsName)
		if deduper.covered(target.Tag) {
			return nil
		}
		group, err := retryLimitedClashFetch(workerCtx, app, groupLimiter, func(fetchCtx context.Context) (*clashy.ClanWarLeagueGroup, error) {
			start := time.Now()
			group, err := app.Clash.GetLeagueGroup(fetchCtx, target.Tag)
			app.Stats.RecordRequest(statsName, time.Since(start), err)
			return group, err
		})
		if err != nil || group == nil || cwlSeasonMonth(group.Season) != season {
			return nil
		}
		cwlID, clanTags := cwlGroupID(group)
		if cwlID == "" {
			return nil
		}
		if !deduper.claimGroup(cwlID, clanTags) {
			return nil
		}
		leagueID, err := d.resolveCWLGroupLeague(workerCtx, app, groupLimiter, cwlID, clanTags, target)
		if err != nil {
			return err
		}
		groupRow := cwlGroupRow(cwlID, group, leagueID)
		warSize, err := d.scheduleCWLWars(workerCtx, app, warLimiter, group, strict)
		if err != nil {
			return err
		}
		if warSize > 0 {
			groupRow.WarSize = intPtr(warSize)
		}
		if err := d.storeIngest(workerCtx, app, models.WarIngest{CWLGroups: []models.CWLGroupRow{groupRow}}); err != nil {
			return err
		}
		return nil
	})
}

func (d *warsDomain) resolveCWLGroupLeague(ctx context.Context, app *platform.App, limiter *clashy.Limiter, cwlID string, clanTags []string, target models.BasicClanRow) (int, error) {
	storedLeagueID, found, err := d.store.LoadStoredCWLGroupLeague(ctx, cwlID)
	if err != nil {
		return 0, err
	}
	if found {
		return storedLeagueID, nil
	}
	if !app.Config.CWLResolveLeagueFromClanProfile {
		return d.store.ResolveCWLGroupLeague(ctx, clanTags, target.CWLLeagueID)
	}

	statsName := trackingProgressName(d.name, string(cwlTargets))
	clan, err := retryLimitedClashFetch(ctx, app, limiter, func(fetchCtx context.Context) (*clashy.Clan, error) {
		start := time.Now()
		clan, err := app.Clash.GetClan(fetchCtx, target.Tag)
		app.Stats.RecordRequest(statsName, time.Since(start), err)
		return clan, err
	})
	if err != nil {
		if app.Logger != nil {
			app.Logger.Warn("CWL league profile lookup failed; group will remain unresolved and retry", "cwl_id", cwlID, "clan_tag", target.Tag, "error", err)
		}
		return 0, nil
	}
	if clan == nil || clan.WarLeague.ID == 0 || clan.WarLeague.ID == unrankedWarLeagueID {
		if app.Logger != nil {
			app.Logger.Warn("CWL league profile lookup returned no ranked league; group will remain unresolved and retry", "cwl_id", cwlID, "clan_tag", target.Tag)
		}
		return 0, nil
	}
	return clan.WarLeague.ID, nil
}

func cwlSeasonMonth(season string) string {
	if len(season) < 7 {
		return ""
	}
	return season[:7]
}

type cwlSyncDeduper struct {
	mu           sync.Mutex
	seenGroups   map[string]struct{}
	coveredClans map[string]struct{}
}

func newCWLSyncDeduper() *cwlSyncDeduper {
	return &cwlSyncDeduper{
		seenGroups:   make(map[string]struct{}),
		coveredClans: make(map[string]struct{}),
	}
}

func (d *cwlSyncDeduper) covered(clanTag string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, covered := d.coveredClans[clanTag]
	return covered
}

func (d *cwlSyncDeduper) claimGroup(cwlID string, clanTags []string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, clanTag := range clanTags {
		d.coveredClans[clanTag] = struct{}{}
	}
	if _, seen := d.seenGroups[cwlID]; seen {
		return false
	}
	d.seenGroups[cwlID] = struct{}{}
	return true
}

func cwlGroupRow(cwlID string, group *clashy.ClanWarLeagueGroup, leagueID int) models.CWLGroupRow {
	row := models.CWLGroupRow{
		CWLID:  cwlID,
		Season: group.Season,
		State:  group.State,
		Rounds: cwlRounds(group),
		Clans:  cwlGroupClanRows(group),
	}
	if leagueID != 0 {
		row.CWLLeagueID = intPtr(leagueID)
	}
	return row
}

func cwlGroupClanRows(group *clashy.ClanWarLeagueGroup) []models.CWLGroupClanRow {
	if group == nil {
		return nil
	}
	rows := make([]models.CWLGroupClanRow, 0, len(group.Clans))
	seen := make(map[string]struct{}, len(group.Clans))
	for _, clan := range group.Clans {
		if clan.Tag == "" {
			continue
		}
		if _, exists := seen[clan.Tag]; exists {
			continue
		}
		seen[clan.Tag] = struct{}{}
		members := make([]models.BasicClanMember, 0, len(clan.Members))
		for _, member := range clan.Members {
			members = append(members, models.BasicClanMember{
				Tag:      member.Tag,
				Name:     member.Name,
				TownHall: member.TownHallLevel,
			})
		}
		rows = append(rows, models.CWLGroupClanRow{
			ClanTag: clan.Tag, Name: clan.Name, ClanLevel: clan.Level,
			BadgeToken: badgeToken(clan.Badge), Members: members,
		})
	}
	return rows
}

func (d *warsDomain) scheduleCWLWars(ctx context.Context, app *platform.App, limiter *clashy.Limiter, group *clashy.ClanWarLeagueGroup, strict bool) (int, error) {
	statsName := trackingProgressName(d.name, string(cwlTargets))
	warSize := 0
	tags := warTags(group)
	known, err := d.store.KnownCWLWarTags(ctx, tags)
	if err != nil {
		return 0, err
	}
	for _, size := range known {
		if size > 0 {
			warSize = size
			break
		}
	}
	for _, warTag := range tags {
		if _, exists := known[warTag]; exists {
			continue
		}
		wars, err := retryLimitedClashFetch(ctx, app, limiter, func(fetchCtx context.Context) ([]clashy.ClanWar, error) {
			start := time.Now()
			wars, err := app.Clash.GetLeagueWars(fetchCtx, []string{warTag})
			app.Stats.RecordRequest(statsName, time.Since(start), err)
			return wars, err
		})
		if err != nil {
			if strict {
				return 0, fmt.Errorf("fetch scoped CWL war %s: %w", warTag, err)
			}
			continue
		}
		if len(wars) == 0 {
			if strict {
				return 0, fmt.Errorf("fetch scoped CWL war %s returned no war", warTag)
			}
			continue
		}
		source := ""
		if wars[0].Clan != nil {
			source = wars[0].Clan.Tag
		}
		ingest, err := buildWarIngest(wars[0], source, false, warTag, "", 0)
		if err != nil {
			return 0, err
		}
		if wars[0].TeamSize > 0 {
			warSize = wars[0].TeamSize
		}
		if err := d.storeIngest(ctx, app, ingest); err != nil {
			return 0, err
		}
	}
	return warSize, nil
}

func cwlGroupID(group *clashy.ClanWarLeagueGroup) (string, []string) {
	if group == nil {
		return "", nil
	}
	tags := make([]string, 0, len(group.Clans))
	for _, clan := range group.Clans {
		if clan.Tag != "" {
			tags = append(tags, clan.Tag)
		}
	}
	sort.Strings(tags)
	if len(tags) == 0 || group.Season == "" {
		return "", tags
	}
	keys := make([]string, 0, len(tags))
	for _, tag := range tags {
		keys = append(keys, strings.TrimPrefix(tag, "#"))
	}
	identity := group.Season + "-" + strings.Join(keys, "-")
	return stableCWLID(identity), tags
}

func stableCWLID(identity string) string {
	sum := sha256.Sum256([]byte(identity))
	return base64.RawURLEncoding.EncodeToString(sum[:9])
}

func cwlRounds(group *clashy.ClanWarLeagueGroup) [][]string {
	if group == nil {
		return nil
	}
	out := make([][]string, 0, len(group.Rounds))
	for _, round := range group.Rounds {
		tags := make([]string, 0, len(round.WarTags))
		for _, warTag := range round.WarTags {
			if warTag != "" && warTag != "#0" {
				tags = append(tags, warTag)
			}
		}
		out = append(out, tags)
	}
	return out
}

func warTags(group *clashy.ClanWarLeagueGroup) []string {
	var out []string
	seen := make(map[string]struct{})
	for _, round := range cwlRounds(group) {
		for _, warTag := range round {
			if _, ok := seen[warTag]; ok {
				continue
			}
			seen[warTag] = struct{}{}
			out = append(out, warTag)
		}
	}
	return out
}

const playerTimerCleanupInterval = 5 * time.Minute

func (d *warsDomain) runPlayerTimerCleanupLoop(ctx context.Context, app *platform.App) {
	ticker := time.NewTicker(playerTimerCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted, err := d.store.DeleteExpiredPlayerTimers(ctx)
			if err != nil {
				app.Logger.Error("player timer cleanup failed", "err", err)
				continue
			}
			if deleted > 0 {
				app.Stats.RecordWrite(d.name, deleted)
			}
		}
	}
}

func warStoreError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("war store %s: %w", operation, err)
}
