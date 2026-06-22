package scripts

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	clashtracker "github.com/clashkinginc/clashy.go/tracker"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
)

const (
	globalClansDomainName = "globalclans"
	unrankedWarLeagueID   = 48000000

	globalClanAsyncWriteBatchSize     = 200
	globalClanAsyncWriteQueueSize     = 2000
	globalClanAsyncWriteFlushInterval = 500 * time.Millisecond
	globalClanTargetRefreshInterval   = 24 * time.Hour
)

const activeGlobalClanTargetSQL = `
	SELECT tag
	FROM basic_clan
	WHERE tag > $1
	  AND member_count > 5
	  AND last_active >= now() - interval '7 days'
	ORDER BY tag
	LIMIT $2
`

const inactiveGlobalClanTargetSQL = `
	SELECT tag
	FROM basic_clan
	WHERE tag > $1
	  AND member_count > 0
	  AND (
	    member_count <= 5
	    OR last_active IS NULL
	    OR last_active < now() - interval '7 days'
	  )
	ORDER BY tag
	LIMIT $2
`

const upsertGlobalClanBasicPlayerSQL = `
	INSERT INTO basic_player (
		tag, name, league_id, clan_tag, townhall_level
	)
	VALUES ($1, $2, NULLIF($3, 0), $4, $5)
	ON CONFLICT (tag) DO UPDATE SET
		name = EXCLUDED.name,
		league_id = EXCLUDED.league_id,
		clan_tag = EXCLUDED.clan_tag,
		townhall_level = EXCLUDED.townhall_level
	WHERE
		basic_player.name IS DISTINCT FROM EXCLUDED.name OR
		basic_player.league_id IS DISTINCT FROM EXCLUDED.league_id OR
		basic_player.clan_tag IS DISTINCT FROM EXCLUDED.clan_tag OR
		basic_player.townhall_level IS DISTINCT FROM EXCLUDED.townhall_level
`

type globalClansDomain struct {
	store globalClanStore
}

type globalClanGroup struct {
	Name              string
	Bucket            string
	RequestsPerSecond int
	MaxInFlight       int
}

type globalClanStore interface {
	ListTargetTags(context.Context, string, int) ([]string, error)
	Load(context.Context, []string) (map[string]models.BasicClanRow, error)
	Store(context.Context, models.GlobalClanIngest) (globalClanStoreResult, error)
	Close() error
}

type globalClanStoreResult struct {
	WriteCount int
	EventClans []models.BasicClanRow
}

type globalClanSnapshot struct {
	Clan      clashy.Clan
	Row       models.BasicClanRow
	FetchedAt time.Time
}

type globalClanTargetStores struct {
	active   *scriptMemoryTargetSet
	inactive *scriptMemoryTargetSet
}

type globalClanTargetMove struct {
	Tag      string
	ToBucket string
	Remove   bool
}

type globalClanTrackerStore struct {
	store   globalClanStore
	app     *platform.App
	group   string
	targets *globalClanTargetStores
	writer  *globalClanAsyncWriter
}

type globalClanAsyncWriter struct {
	app     *platform.App
	store   globalClanStore
	targets *globalClanTargetStores
	jobs    chan globalClanWriteJob
}

type globalClanWriteJob struct {
	Group  string
	Ingest models.GlobalClanIngest
	Moves  []globalClanTargetMove
}

func NewGlobalClansDomain() platform.Domain { return &globalClansDomain{} }

func (d *globalClansDomain) Name() string { return globalClansDomainName }

func (d *globalClansDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateGlobalClanConfig(app.Config); err != nil {
		return err
	}
	store, err := d.openStore(ctx, app)
	if err != nil {
		return err
	}
	d.store = store
	defer store.Close()

	// Priority and non-priority clans are independent loops so low-value scans cannot starve
	// active clans. Targets are owned by the target stores; database tables only hold
	// fetched snapshots and derived history.
	groups := globalClanGroups(app.Config)

	targetStores, err := d.openTargetStores(ctx, app, groups)
	if err != nil {
		return err
	}
	writer := newGlobalClanAsyncWriter(app, store, targetStores)
	go writer.run(ctx)
	targetStores.run(ctx)
	return d.runGroups(ctx, app, groups, targetStores, writer)
}

func globalClanGroups(cfg platform.Config) []globalClanGroup {
	return []globalClanGroup{
		{
			Name:              "priority",
			Bucket:            "active",
			RequestsPerSecond: cfg.GlobalClanPriorityRequestsPerSecond,
			MaxInFlight:       cfg.GlobalClanPriorityRequestsPerSecond,
		},
		{
			Name:              "non_priority",
			Bucket:            "inactive",
			RequestsPerSecond: cfg.GlobalClanNonPriorityRequestsPerSecond,
			MaxInFlight:       cfg.GlobalClanNonPriorityRequestsPerSecond,
		},
	}
}

func (d *globalClansDomain) openStore(ctx context.Context, app *platform.App) (globalClanStore, error) {
	if app.Config.DryRun || app.Config.MockDB {
		return newMemoryGlobalClanStore(), nil
	}
	return newTimescaleGlobalClanStore(ctx, app.Config.TimescaleURL)
}

func validateGlobalClanConfig(cfg platform.Config) error {
	if cfg.GlobalClanPriorityRequestsPerSecond <= 0 {
		return errors.New("globalclans.priority_requests_per_second must be greater than zero when globalclans is enabled")
	}
	if cfg.GlobalClanNonPriorityRequestsPerSecond <= 0 {
		return errors.New("globalclans.non_priority_requests_per_second must be greater than zero when globalclans is enabled")
	}
	if cfg.TargetPageMultiplier <= 0 {
		return errors.New("target_page_multiplier must be greater than zero when globalclans is enabled")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required when globalclans is enabled")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.ValkeyAddr == "" {
		return errors.New("valkey_addr is required for globalclans event publishing")
	}
	return nil
}

func (d *globalClansDomain) openTargetStores(
	ctx context.Context,
	app *platform.App,
	groups []globalClanGroup,
) (*globalClanTargetStores, error) {
	targetStores := &globalClanTargetStores{}
	for _, group := range groups {
		bucket := group.Bucket
		targetSet, err := newScriptMemoryTargetSet(ctx, app, globalClansDomainName, group.Name, globalClanTargetRefreshInterval, func(loadCtx context.Context) ([]clashtracker.Target, error) {
			tags, err := d.store.ListTargetTags(loadCtx, bucket, group.RequestsPerSecond*app.Config.TargetPageMultiplier)
			if err != nil {
				return nil, err
			}
			targets := make([]clashtracker.Target, 0, len(tags))
			for _, tag := range tags {
				targets = append(targets, clashtracker.Target{Key: tag})
			}
			return targets, nil
		})
		if err != nil {
			return nil, err
		}
		switch group.Bucket {
		case "inactive":
			targetStores.inactive = targetSet
		default:
			targetStores.active = targetSet
		}
	}
	return targetStores, nil
}

func newGlobalClanAsyncWriter(
	app *platform.App,
	store globalClanStore,
	targets *globalClanTargetStores,
) *globalClanAsyncWriter {
	return &globalClanAsyncWriter{
		app:     app,
		store:   store,
		targets: targets,
		jobs:    make(chan globalClanWriteJob, globalClanAsyncWriteQueueSize),
	}
}

func (w *globalClanAsyncWriter) enqueue(ctx context.Context, job globalClanWriteJob) error {
	if w == nil {
		return errors.New("global clan async writer is not configured")
	}
	select {
	case w.jobs <- job:
		w.recordQueueDepth(0)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *globalClanAsyncWriter) run(ctx context.Context) {
	timer := time.NewTimer(globalClanAsyncWriteFlushInterval)
	defer timer.Stop()
	batch := make([]globalClanWriteJob, 0, globalClanAsyncWriteBatchSize)
	flush := func(flushCtx context.Context) {
		if len(batch) == 0 {
			w.recordQueueDepth(0)
			return
		}
		if err := w.writeBatch(flushCtx, batch); err != nil {
			w.app.Logger.Error("global clan async store batch failed", "jobs", len(batch), "err", err)
			w.app.Stats.SetReady(globalClansDomainName, false, err.Error())
		}
		batch = batch[:0]
		w.recordQueueDepth(0)
	}
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(globalClanAsyncWriteFlushInterval)
	}
	for {
		select {
		case <-ctx.Done():
			flush(context.WithoutCancel(ctx))
			return
		case job := <-w.jobs:
			batch = append(batch, job)
			w.recordQueueDepth(len(batch))
			if len(batch) >= globalClanAsyncWriteBatchSize {
				flush(ctx)
				resetTimer()
			}
		case <-timer.C:
			flush(ctx)
			timer.Reset(globalClanAsyncWriteFlushInterval)
		}
	}
}

func (w *globalClanAsyncWriter) recordQueueDepth(inBatch int) {
	if w == nil || w.app == nil || w.jobs == nil {
		return
	}
	w.app.Stats.SetQueueDepth(globalClansDomainName, len(w.jobs)+inBatch)
}

func (s *globalClanTargetStores) run(ctx context.Context) {
	if s == nil {
		return
	}
	go s.active.Run(ctx)
	go s.inactive.Run(ctx)
}

func (s *globalClanTargetStores) forBucket(bucket string) clashtracker.TargetStore {
	if s == nil {
		return clashtracker.NewMemoryTargetStore()
	}
	if bucket == "inactive" {
		if s.inactive != nil {
			return s.inactive.Store()
		}
		return clashtracker.NewMemoryTargetStore()
	}
	if s.active != nil {
		return s.active.Store()
	}
	return clashtracker.NewMemoryTargetStore()
}

func (s *globalClanTargetStores) setRunner(bucket string, runner *clashtracker.Runner[*clashy.Clan, globalClanSnapshot]) {
	if s == nil {
		return
	}
	if bucket == "inactive" {
		if s.inactive != nil {
			s.inactive.SetRunner(runner)
		}
		return
	}
	if s.active != nil {
		s.active.SetRunner(runner)
	}
}

func (s *globalClanTargetStores) add(ctx context.Context, bucket string, target clashtracker.Target) error {
	if s == nil {
		return nil
	}
	if bucket == "inactive" {
		if s.inactive == nil {
			return nil
		}
		return s.inactive.Add(ctx, target)
	}
	if s.active == nil {
		return nil
	}
	return s.active.Add(ctx, target)
}

func (s *globalClanTargetStores) remove(ctx context.Context, bucket string, key string) error {
	if s == nil {
		return nil
	}
	if bucket == "inactive" {
		if s.inactive == nil {
			return nil
		}
		return s.inactive.Remove(ctx, key)
	}
	if s.active == nil {
		return nil
	}
	return s.active.Remove(ctx, key)
}

func (d *globalClansDomain) runGroups(
	ctx context.Context,
	app *platform.App,
	groups []globalClanGroup,
	targetStores *globalClanTargetStores,
	writer *globalClanAsyncWriter,
) error {
	errCh := make(chan error, len(groups))
	for _, group := range groups {
		group := group
		go func() {
			errCh <- d.runGroup(ctx, app, group, targetStores, writer)
		}()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (d *globalClansDomain) runGroup(
	ctx context.Context,
	app *platform.App,
	group globalClanGroup,
	targetStores *globalClanTargetStores,
	writer *globalClanAsyncWriter,
) error {
	ctx, span := platform.StartSpan(ctx, "tracker.group",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", group.Name),
	)
	defer span.End()

	minInterval := 5 * time.Second
	store := &globalClanTrackerStore{
		store:   d.store,
		app:     app,
		group:   group.Name,
		targets: targetStores,
		writer:  writer,
	}
	runner, err := clashtracker.NewRunner[*clashy.Clan, globalClanSnapshot](clashtracker.Config[*clashy.Clan, globalClanSnapshot]{
		TargetStore:       targetStores.forBucket(group.Bucket),
		Store:             store,
		RequestsPerSecond: group.RequestsPerSecond,
		MaxInFlight:       group.MaxInFlight,
		MinInterval:       minInterval,
		EmitInitial:       true,
		Fetch: func(fetchCtx context.Context, target clashtracker.Target) (clashtracker.FetchResult[*clashy.Clan], error) {
			clan, err := fetchGlobalClan(fetchCtx, app, group.Name, target.Key)
			return clashtracker.FetchResult[*clashy.Clan]{Value: clan}, err
		},
		Project: func(clan *clashy.Clan) (globalClanSnapshot, error) {
			if clan == nil {
				return globalClanSnapshot{}, nil
			}
			if clan.Tag == "" && clan.Name == "" {
				return globalClanSnapshot{}, fmt.Errorf("empty clan payload")
			}
			return globalClanSnapshot{
				Clan:      *clan,
				Row:       basicClanRow(*clan),
				FetchedAt: time.Now().UTC(),
			}, nil
		},
		Diff: func(ctx context.Context, _ clashtracker.Target, previous, current globalClanSnapshot) error {
			ingest := buildGlobalClanIngest(current.Clan, previous.Row, current.FetchedAt)
			moves := globalClanTargetMoves(current.Clan, previous.Row, ingest, current.FetchedAt)
			return store.writer.enqueue(ctx, globalClanWriteJob{
				Group:  store.group,
				Ingest: ingest,
				Moves:  moves,
			})
		},
		OnError: func(_ context.Context, target clashtracker.Target, err error) error {
			app.Logger.Error("global clan fetch failed", "tag", target.Key, "err", err)
			app.Stats.SetReady(globalClansDomainName, false, err.Error())
			if platform.IsNonFatalClashError(err) {
				return nil
			}
			return err
		},
	})
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	targetStores.setRunner(group.Bucket, runner)
	err = runner.Run(ctx)
	if err != nil {
		app.Stats.SetReady(globalClansDomainName, false, err.Error())
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	app.Stats.SetReady(globalClansDomainName, true, "")
	span.SetAttributes(platform.SpanErrorStatus(nil))
	return nil
}

func fetchGlobalClan(ctx context.Context, app *platform.App, group, tag string) (*clashy.Clan, error) {
	start := time.Now()
	fetchCtx, span := platform.StartSpan(ctx, "clash.fetch",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", group),
		attribute.String("operation", "clan"),
	)
	clan, err := app.Clash.GetClan(fetchCtx, tag)
	platform.RecordSpanError(span, err)
	span.SetAttributes(platform.SpanErrorStatus(err))
	span.End()
	app.Stats.RecordRequest(globalClansDomainName, time.Since(start), err)
	if err != nil || clan == nil {
		return clan, err
	}
	return clan, nil
}

func (s *globalClanTrackerStore) Load(ctx context.Context, key string) (globalClanSnapshot, bool, error) {
	loaded, err := s.store.Load(ctx, []string{key})
	if err != nil {
		return globalClanSnapshot{}, false, err
	}
	row, ok := loaded[key]
	return globalClanSnapshot{Row: row}, ok, nil
}

func (s *globalClanTrackerStore) Store(context.Context, string, globalClanSnapshot, time.Duration) error {
	return nil
}

func (w *globalClanAsyncWriter) writeBatch(ctx context.Context, jobs []globalClanWriteJob) error {
	if len(jobs) == 0 {
		return nil
	}
	group := jobs[0].Group
	var ingest models.GlobalClanIngest
	var moves []globalClanTargetMove
	for _, job := range jobs {
		if job.Group != group {
			group = "mixed"
		}
		ingest = mergeGlobalClanIngest(ingest, job.Ingest)
		moves = append(moves, job.Moves...)
	}
	if globalClanIngestWriteCount(ingest) == 0 {
		if err := applyGlobalClanTargetMoves(ctx, w.targets, moves); err != nil {
			return err
		}
		return nil
	}
	ctx, span := platform.StartSpan(ctx, "globalclans.store",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", group),
		attribute.String("operation", "store_clans"),
		attribute.Int("jobs.count", len(jobs)),
		attribute.Int("rows.count", globalClanIngestWriteCount(ingest)),
	)
	defer span.End()
	requestedWrites := globalClanIngestWriteCount(ingest)
	storeStart := time.Now()
	result, err := w.store.Store(ctx, ingest)
	storeDuration := time.Since(storeStart)
	if err != nil {
		w.app.Stats.RecordStore(globalClansDomainName, storeDuration, requestedWrites, 0)
		if storeDuration > time.Second {
			w.app.Logger.Warn("slow global clan store failed",
				"group", group,
				"jobs", len(jobs),
				"requested_writes", requestedWrites,
				"duration_ms", durationMillis(storeDuration),
				"err", err,
			)
		}
		platform.RecordSpanError(span, err)
		span.SetAttributes(
			attribute.Int("write.requested", requestedWrites),
			attribute.Float64("store.duration_ms", durationMillis(storeDuration)),
			platform.SpanErrorStatus(err),
		)
		return err
	}
	w.app.Stats.RecordStore(globalClansDomainName, storeDuration, requestedWrites, result.WriteCount)
	if storeDuration > time.Second {
		w.app.Logger.Warn("slow global clan store",
			"group", group,
			"jobs", len(jobs),
			"requested_writes", requestedWrites,
			"affected_writes", result.WriteCount,
			"duration_ms", durationMillis(storeDuration),
		)
	}
	w.app.Stats.RecordWrite(globalClansDomainName, result.WriteCount)
	if err := applyGlobalClanTargetMoves(ctx, w.targets, moves); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(
			attribute.Int("write.count", result.WriteCount),
			attribute.Int("write.requested", requestedWrites),
			attribute.Float64("store.duration_ms", durationMillis(storeDuration)),
			platform.SpanErrorStatus(err),
		)
		return err
	}
	if err := publishGlobalClanEvents(ctx, w.app, group, result.EventClans); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(
			attribute.Int("write.count", result.WriteCount),
			attribute.Int("write.requested", requestedWrites),
			attribute.Float64("store.duration_ms", durationMillis(storeDuration)),
			platform.SpanErrorStatus(err),
		)
		return err
	}
	w.app.Stats.SetReady(globalClansDomainName, true, "")
	span.SetAttributes(
		attribute.Int("write.count", result.WriteCount),
		attribute.Int("write.requested", requestedWrites),
		attribute.Int("jobs.count", len(jobs)),
		attribute.Float64("store.duration_ms", durationMillis(storeDuration)),
		platform.SpanErrorStatus(nil),
	)
	return nil
}

func applyGlobalClanTargetMoves(ctx context.Context, stores *globalClanTargetStores, moves []globalClanTargetMove) error {
	if stores == nil || len(moves) == 0 {
		return nil
	}
	for _, move := range moves {
		if move.Tag == "" {
			continue
		}
		switch {
		case move.Remove:
			if err := stores.remove(ctx, "active", move.Tag); err != nil {
				return err
			}
			if err := stores.remove(ctx, "inactive", move.Tag); err != nil {
				return err
			}
		case move.ToBucket == "inactive":
			if err := stores.remove(ctx, "active", move.Tag); err != nil {
				return err
			}
			if err := stores.add(ctx, "inactive", clashtracker.Target{Key: move.Tag}); err != nil {
				return err
			}
		default:
			if err := stores.remove(ctx, "inactive", move.Tag); err != nil {
				return err
			}
			if err := stores.add(ctx, "active", clashtracker.Target{Key: move.Tag}); err != nil {
				return err
			}
		}
	}
	return nil
}

func durationMillis(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func publishGlobalClanEvents(ctx context.Context, app *platform.App, group string, clans []models.BasicClanRow) error {
	ctx, span := platform.StartSpan(ctx, "valkey.events.xadd.batch",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", group),
		attribute.String("operation", "xadd"),
		attribute.Int("event.count", len(clans)),
	)
	defer span.End()
	start := time.Now()
	publishCount := 0
	errorCount := 0
	var publishTotal time.Duration
	var publishMax time.Duration
	var publishErr error
	for _, clan := range clans {
		publishStart := time.Now()
		err := app.PublishEvent(ctx, platform.Event{
			Topic:   "clan",
			ClanTag: clan.Tag,
			Value:   map[string]any{"tag": clan.Tag, "name": clan.Name},
		})
		latency := time.Since(publishStart)
		publishCount++
		publishTotal += latency
		if latency > publishMax {
			publishMax = latency
		}
		if err != nil {
			errorCount++
			publishErr = err
			break
		}
	}
	publishAverage := 0.0
	if publishCount > 0 {
		publishAverage = durationMillis(publishTotal) / float64(publishCount)
	}
	platform.RecordSpanError(span, publishErr)
	span.SetAttributes(
		attribute.Int("event.published.count", publishCount-errorCount),
		attribute.Int("event.error.count", errorCount),
		attribute.Float64("event.duration.sum_ms", durationMillis(publishTotal)),
		attribute.Float64("event.duration.avg_ms", publishAverage),
		attribute.Float64("event.duration.max_ms", durationMillis(publishMax)),
		attribute.Float64("batch.wall_ms", durationMillis(time.Since(start))),
		platform.SpanErrorStatus(publishErr),
	)
	return publishErr
}

func globalClanIngestWriteCount(ingest models.GlobalClanIngest) int {
	return len(ingest.Clans) + len(ingest.Players) + len(ingest.ActiveClanTags) +
		len(ingest.DeletedClanTags) + len(ingest.ClanChanges) + len(ingest.JoinLeaves)
}

func buildGlobalClanIngest(current clashy.Clan, previous models.BasicClanRow, now time.Time) models.GlobalClanIngest {
	row := basicClanRow(current)
	ingest := models.GlobalClanIngest{
		Clans:   []models.BasicClanRow{row},
		Players: basicPlayerRows(current.Tag, current.Members),
	}
	if previous.Tag == "" {
		return ingest
	}
	// Joins are the only member movement that marks a clan active. Leave-only changes are
	// still persisted, but they should not promote a clan into the priority polling bucket.
	ingest.JoinLeaves = joinLeaveRows(previous, current, now)
	if hasJoin(ingest.JoinLeaves) {
		ingest.ActiveClanTags = []string{current.Tag}
	}
	ingest.ClanChanges = clanChangeRows(previous, row, now)
	return ingest
}

func globalClanTargetMoves(
	current clashy.Clan,
	previous models.BasicClanRow,
	ingest models.GlobalClanIngest,
	now time.Time,
) []globalClanTargetMove {
	tag := current.Tag
	if tag == "" {
		return nil
	}
	if len(ingest.DeletedClanTags) > 0 {
		return []globalClanTargetMove{{Tag: tag, Remove: true}}
	}

	previousBucket := ""
	if previous.Tag != "" {
		previousBucket = globalClanBucket(previous, now)
	}
	currentBucket := globalClanCurrentBucket(current, previous, hasJoin(ingest.JoinLeaves), now)
	if currentBucket == "" {
		return []globalClanTargetMove{{Tag: tag, Remove: true}}
	}
	if currentBucket == previousBucket {
		return nil
	}
	return []globalClanTargetMove{{Tag: tag, ToBucket: currentBucket}}
}

func globalClanCurrentBucket(current clashy.Clan, previous models.BasicClanRow, joined bool, now time.Time) string {
	if current.MemberCount <= 0 {
		return ""
	}
	if current.MemberCount <= 5 {
		return "inactive"
	}
	if joined {
		return "active"
	}
	if previous.Tag == "" {
		return "inactive"
	}
	return globalClanBucket(previous, now)
}

func globalClanBucket(row models.BasicClanRow, now time.Time) string {
	cutoff := now.Add(-7 * 24 * time.Hour)
	if isActiveGlobalClan(row, cutoff) {
		return "active"
	}
	return "inactive"
}

func basicClanRow(clan clashy.Clan) models.BasicClanRow {
	row := models.BasicClanRow{
		Tag:            clan.Tag,
		Name:           clan.Name,
		Description:    clan.Description,
		ClanLevel:      clan.Level,
		PublicWarLog:   clan.PublicWarLog,
		WarWins:        clan.WarWins,
		MemberCount:    clan.MemberCount,
		BadgeURL:       clan.Badge.URL(),
		MemberTags:     memberTags(clan.Members),
		TroopsDonated:  totalDonated(clan.Members),
		TroopsReceived: totalReceived(clan.Members),
		CWLLeagueID:    unrankedWarLeagueID,
	}
	if clan.Location != nil {
		row.LocationID = intPtr(clan.Location.ID)
	}
	if clan.WarLeague.ID != 0 {
		row.CWLLeagueID = clan.WarLeague.ID
	}
	if clan.CapitalLeague != nil {
		row.CapitalLeagueID = intPtr(clan.CapitalLeague.ID)
	}
	return row
}

func joinLeaveRows(previous models.BasicClanRow, current clashy.Clan, now time.Time) []models.JoinLeaveRow {
	currentMembers := clanMembersByTag(current.Members)
	currentTags := stringSet(memberTags(current.Members))
	previousTags := stringSet(previous.MemberTags)
	var out []models.JoinLeaveRow
	for tag := range currentTags {
		if _, ok := previousTags[tag]; ok {
			continue
		}
		member := currentMembers[tag]
		out = append(out, models.JoinLeaveRow{
			EventTime:     now,
			EventType:     "join",
			ClanTag:       current.Tag,
			PlayerTag:     tag,
			TownHallLevel: member.TownHall,
		})
	}
	for tag := range previousTags {
		if _, ok := currentTags[tag]; ok {
			continue
		}
		out = append(out, models.JoinLeaveRow{
			EventTime: now,
			EventType: "leave",
			ClanTag:   current.Tag,
			PlayerTag: tag,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PlayerTag == out[j].PlayerTag {
			return out[i].EventType < out[j].EventType
		}
		return out[i].PlayerTag < out[j].PlayerTag
	})
	return out
}

func clanChangeRows(previous, current models.BasicClanRow, now time.Time) []models.ClanChangeRow {
	var out []models.ClanChangeRow
	add := func(changeType string, previousValue, currentValue any) {
		out = append(out, models.ClanChangeRow{
			EventTime:     now,
			ClanTag:       current.Tag,
			ChangeType:    changeType,
			PreviousValue: previousValue,
			CurrentValue:  currentValue,
		})
	}
	if previous.Description != current.Description {
		add("description", previous.Description, current.Description)
	}
	if previous.ClanLevel != current.ClanLevel {
		add("clan_level", previous.ClanLevel, current.ClanLevel)
	}
	if previous.CWLLeagueID != current.CWLLeagueID {
		add("cwl_league_id", previous.CWLLeagueID, current.CWLLeagueID)
	}
	if !optionalIntEqual(previous.CapitalLeagueID, current.CapitalLeagueID) {
		add("capital_league_id", optionalIntValue(previous.CapitalLeagueID), optionalIntValue(current.CapitalLeagueID))
	}
	return out
}

func mergeGlobalClanIngest(left, right models.GlobalClanIngest) models.GlobalClanIngest {
	left.Clans = append(left.Clans, right.Clans...)
	left.Players = append(left.Players, right.Players...)
	left.ActiveClanTags = append(left.ActiveClanTags, right.ActiveClanTags...)
	left.DeletedClanTags = append(left.DeletedClanTags, right.DeletedClanTags...)
	left.ClanChanges = append(left.ClanChanges, right.ClanChanges...)
	left.JoinLeaves = append(left.JoinLeaves, right.JoinLeaves...)
	return left
}

func hasJoin(rows []models.JoinLeaveRow) bool {
	for _, row := range rows {
		if row.EventType == "join" {
			return true
		}
	}
	return false
}

func basicPlayerRows(clanTag string, members []clashy.ClanMember) []models.BasicPlayerRow {
	out := make([]models.BasicPlayerRow, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		if member.Tag == "" || member.Name == "" || member.TownHall <= 0 {
			continue
		}
		if _, ok := seen[member.Tag]; ok {
			continue
		}
		seen[member.Tag] = struct{}{}
		out = append(out, models.BasicPlayerRow{
			Tag:      member.Tag,
			Name:     member.Name,
			LeagueID: member.LeagueTier.ID,
			ClanTag:  clanTag,
			TownHall: member.TownHall,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tag < out[j].Tag })
	return out
}

func memberTags(members []clashy.ClanMember) []string {
	out := make([]string, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		if member.Tag == "" {
			continue
		}
		if _, ok := seen[member.Tag]; ok {
			continue
		}
		seen[member.Tag] = struct{}{}
		out = append(out, member.Tag)
	}
	sort.Strings(out)
	return out
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func clanMembersByTag(members []clashy.ClanMember) map[string]clashy.ClanMember {
	out := make(map[string]clashy.ClanMember, len(members))
	for _, member := range members {
		if member.Tag != "" {
			out[member.Tag] = member
		}
	}
	return out
}

func totalDonated(members []clashy.ClanMember) int {
	total := 0
	for _, member := range members {
		total += member.Donations
	}
	return total
}

func totalReceived(members []clashy.ClanMember) int {
	total := 0
	for _, member := range members {
		total += member.Received
	}
	return total
}

func intPtr(value int) *int {
	return &value
}

func optionalIntEqual(left, right *int) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func optionalIntValue(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func optionalIntFromSQL(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	out := int(value.Int64)
	return &out
}

type timescaleGlobalClanStore struct {
	pool *pgxpool.Pool
}

func newTimescaleGlobalClanStore(ctx context.Context, dsn string) (*timescaleGlobalClanStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleGlobalClanStore{pool: pool}, nil
}

func (s *timescaleGlobalClanStore) Close() error {
	if s == nil || s.pool == nil {
		return nil
	}
	s.pool.Close()
	return nil
}

func (s *timescaleGlobalClanStore) ListTargetTags(ctx context.Context, bucket string, pageSize int) ([]string, error) {
	if pageSize <= 0 {
		pageSize = 1000
	}
	ctx, span := platform.StartSpan(ctx, "timescale.basic_clan.targets",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", bucket),
		attribute.String("operation", "seed_basic_clan_targets"),
		attribute.Int("batch.size", pageSize),
	)
	defer span.End()

	var out []string
	cursor := ""
	for {
		tags, nextCursor, err := s.scanClanBucket(ctx, bucket, cursor, pageSize)
		if err != nil {
			platform.RecordSpanError(span, err)
			span.SetAttributes(platform.SpanErrorStatus(err))
			return nil, err
		}
		out = append(out, tags...)
		if nextCursor == "" {
			break
		}
		cursor = nextCursor
	}
	span.SetAttributes(
		attribute.Int("target.count", len(out)),
		platform.SpanErrorStatus(nil),
	)
	return out, nil
}

func (s *timescaleGlobalClanStore) scanClanBucket(ctx context.Context, bucket, cursor string, limit int) ([]string, string, error) {
	if limit <= 0 {
		return nil, cursor, nil
	}
	query := activeGlobalClanTargetSQL
	if bucket == "inactive" {
		query = inactiveGlobalClanTargetSQL
	}
	rows, err := s.pool.Query(ctx, query, cursor, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	tags := make([]string, 0, limit+1)
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, "", err
		}
		if tag == "" {
			continue
		}
		tags = append(tags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if len(tags) == 0 {
		return nil, "", nil
	}
	if len(tags) > limit {
		// Read one extra row to decide whether this bucket has more work or should wrap.
		nextCursor := tags[limit-1]
		return tags[:limit], nextCursor, nil
	}
	return tags, "", nil
}

func (s *timescaleGlobalClanStore) Load(ctx context.Context, tags []string) (map[string]models.BasicClanRow, error) {
	out := make(map[string]models.BasicClanRow)
	if len(tags) == 0 {
		return out, nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.basic_clan.load",
		attribute.String("domain", globalClansDomainName),
		attribute.String("operation", "load_basic_clans"),
		attribute.Int("target.count", len(tags)),
	)
	defer span.End()
	rows, err := s.pool.Query(ctx, `
		SELECT
			tag, name, description, clan_level, location_id, cwl_league_id, capital_league_id,
			public_war_log, war_wins, member_count, badge_url, troops_donated,
			troops_received, member_tags, last_active
		FROM basic_clan
		WHERE tag = ANY($1)
	`, tags)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row models.BasicClanRow
		var locationID sql.NullInt64
		var cwlLeagueID int
		var capitalLeagueID sql.NullInt64
		var lastActive sql.NullTime
		if err := rows.Scan(
			&row.Tag, &row.Name, &row.Description, &row.ClanLevel, &locationID, &cwlLeagueID, &capitalLeagueID,
			&row.PublicWarLog, &row.WarWins, &row.MemberCount, &row.BadgeURL, &row.TroopsDonated,
			&row.TroopsReceived, &row.MemberTags, &lastActive,
		); err != nil {
			platform.RecordSpanError(span, err)
			span.SetAttributes(platform.SpanErrorStatus(err))
			return nil, err
		}
		row.LocationID = optionalIntFromSQL(locationID)
		row.CWLLeagueID = cwlLeagueID
		row.CapitalLeagueID = optionalIntFromSQL(capitalLeagueID)
		if lastActive.Valid {
			row.LastActive = &lastActive.Time
		}
		out[row.Tag] = row
	}
	err = rows.Err()
	platform.RecordSpanError(span, err)
	span.SetAttributes(attribute.Int("rows.count", len(out)), platform.SpanErrorStatus(err))
	return out, err
}

func (s *timescaleGlobalClanStore) Store(ctx context.Context, ingest models.GlobalClanIngest) (globalClanStoreResult, error) {
	requestedWrites := globalClanIngestWriteCount(ingest)
	if requestedWrites == 0 {
		return globalClanStoreResult{}, nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.globalclans.store",
		attribute.String("domain", globalClansDomainName),
		attribute.String("operation", "store_global_clans"),
		attribute.Int("write.count", requestedWrites),
	)
	defer span.End()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanStoreResult{}, err
	}
	defer tx.Rollback(ctx)

	// Players are written before join/leave history because that insert joins basic_player.
	result := globalClanStoreResult{}
	count, err := upsertGlobalClanBasicPlayers(ctx, tx, ingest.Players)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	count, err = insertClanChanges(ctx, tx, ingest.ClanChanges)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	count, err = insertJoinLeaves(ctx, tx, ingest.JoinLeaves)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	count, err = deleteBasicClans(ctx, tx, ingest.DeletedClanTags)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	changedClans, count, err := upsertBasicClans(ctx, tx, ingest.Clans)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	result.EventClans = changedClans
	count, err = updateActiveClans(ctx, tx, ingest.ActiveClanTags)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	if err := tx.Commit(ctx); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanStoreResult{}, err
	}
	span.SetAttributes(attribute.Int("rows.affected", result.WriteCount), platform.SpanErrorStatus(nil))
	return result, nil
}

const upsertBasicClanSQL = `
	INSERT INTO basic_clan (
		tag, name, description, clan_level, location_id, cwl_league_id, capital_league_id,
		public_war_log, war_wins, member_count, badge_url, troops_donated,
		troops_received, member_tags
	)
	VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
	)
	ON CONFLICT (tag) DO UPDATE SET
		name = EXCLUDED.name,
		description = EXCLUDED.description,
		clan_level = EXCLUDED.clan_level,
		location_id = EXCLUDED.location_id,
		cwl_league_id = EXCLUDED.cwl_league_id,
		capital_league_id = EXCLUDED.capital_league_id,
		public_war_log = EXCLUDED.public_war_log,
		war_wins = EXCLUDED.war_wins,
		member_count = EXCLUDED.member_count,
		badge_url = EXCLUDED.badge_url,
		troops_donated = EXCLUDED.troops_donated,
		troops_received = EXCLUDED.troops_received,
		member_tags = EXCLUDED.member_tags
	WHERE
		basic_clan.name IS DISTINCT FROM EXCLUDED.name OR
		basic_clan.description IS DISTINCT FROM EXCLUDED.description OR
		basic_clan.clan_level IS DISTINCT FROM EXCLUDED.clan_level OR
		basic_clan.location_id IS DISTINCT FROM EXCLUDED.location_id OR
		basic_clan.cwl_league_id IS DISTINCT FROM EXCLUDED.cwl_league_id OR
		basic_clan.capital_league_id IS DISTINCT FROM EXCLUDED.capital_league_id OR
		basic_clan.public_war_log IS DISTINCT FROM EXCLUDED.public_war_log OR
		basic_clan.war_wins IS DISTINCT FROM EXCLUDED.war_wins OR
		basic_clan.member_count IS DISTINCT FROM EXCLUDED.member_count OR
		basic_clan.badge_url IS DISTINCT FROM EXCLUDED.badge_url OR
		basic_clan.troops_donated IS DISTINCT FROM EXCLUDED.troops_donated OR
		basic_clan.troops_received IS DISTINCT FROM EXCLUDED.troops_received OR
		basic_clan.member_tags IS DISTINCT FROM EXCLUDED.member_tags
`

func upsertBasicClans(ctx context.Context, tx pgx.Tx, clans []models.BasicClanRow) ([]models.BasicClanRow, int, error) {
	if len(clans) == 0 {
		return nil, 0, nil
	}
	batch := &pgx.Batch{}
	for _, clan := range clans {
		batch.Queue(upsertBasicClanSQL,
			clan.Tag, clan.Name, clan.Description, clan.ClanLevel,
			optionalIntValue(clan.LocationID),
			clan.CWLLeagueID,
			optionalIntValue(clan.CapitalLeagueID),
			clan.PublicWarLog, clan.WarWins, clan.MemberCount, clan.BadgeURL, clan.TroopsDonated,
			clan.TroopsReceived, clan.MemberTags,
		)
	}
	results := tx.SendBatch(ctx, batch)
	changed := make([]models.BasicClanRow, 0, len(clans))
	affected := 0
	var err error
	for i := 0; i < batch.Len(); i++ {
		var tag pgconn.CommandTag
		tag, err = results.Exec()
		if err != nil {
			break
		}
		if tag.RowsAffected() > 0 {
			changed = append(changed, clans[i])
			affected += int(tag.RowsAffected())
		}
	}
	closeErr := results.Close()
	if err == nil {
		err = closeErr
	}
	return changed, affected, err
}

func updateActiveClans(ctx context.Context, tx pgx.Tx, tags []string) (int, error) {
	tags = uniqueStrings(tags)
	if len(tags) == 0 {
		return 0, nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE basic_clan
		SET last_active = now()
		WHERE tag = ANY($1)
	`, tags)
	return int(tag.RowsAffected()), err
}

func deleteBasicClans(ctx context.Context, tx pgx.Tx, tags []string) (int, error) {
	if len(tags) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, tag := range tags {
		if tag != "" {
			batch.Queue(`DELETE FROM basic_clan WHERE tag = $1`, tag)
		}
	}
	return utils.SendBatchCount(ctx, tx, batch)
}

func upsertGlobalClanBasicPlayers(ctx context.Context, tx pgx.Tx, players []models.BasicPlayerRow) (int, error) {
	if len(players) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, player := range players {
		batch.Queue(upsertGlobalClanBasicPlayerSQL,
			player.Tag,
			player.Name,
			player.LeagueID,
			player.ClanTag,
			player.TownHall,
		)
	}
	return utils.SendBatchCount(ctx, tx, batch)
}

func insertJoinLeaves(ctx context.Context, tx pgx.Tx, rows []models.JoinLeaveRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.EventType == "" || row.ClanTag == "" || row.PlayerTag == "" {
			continue
		}
		batch.Queue(`
			INSERT INTO join_leave_history (
				event_time, event_type, clan_tag, player_tag, townhall_level
			)
			SELECT $1, $2, $3, p.tag, p.townhall_level
			FROM basic_player p
			WHERE p.tag = $4
			`, row.EventTime, row.EventType, row.ClanTag, row.PlayerTag)
	}
	return utils.SendBatchCount(ctx, tx, batch)
}

func insertClanChanges(ctx context.Context, tx pgx.Tx, rows []models.ClanChangeRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.ChangeType == "" || row.ClanTag == "" {
			continue
		}
		previousValue, err := json.Marshal(row.PreviousValue)
		if err != nil {
			return 0, err
		}
		currentValue, err := json.Marshal(row.CurrentValue)
		if err != nil {
			return 0, err
		}
		batch.Queue(`
			INSERT INTO clan_change_history (
				event_time, clan_tag, change_type, previous_value, current_value
			)
			VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
			`, row.EventTime, row.ClanTag, row.ChangeType, string(previousValue), string(currentValue))
	}
	return utils.SendBatchCount(ctx, tx, batch)
}

type memoryGlobalClanStore struct {
	mu   sync.RWMutex
	rows map[string]models.BasicClanRow
}

func newMemoryGlobalClanStore() *memoryGlobalClanStore {
	return &memoryGlobalClanStore{rows: make(map[string]models.BasicClanRow)}
}

func (s *memoryGlobalClanStore) Close() error { return nil }

func (s *memoryGlobalClanStore) ListTargetTags(_ context.Context, bucket string, _ int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	active := bucket != "inactive"
	tags := make([]string, 0, len(s.rows))
	cutoff := time.Now().UTC().Add(-7 * 24 * time.Hour)
	for tag, row := range s.rows {
		if tag == "" || row.MemberCount <= 0 || active != isActiveGlobalClan(row, cutoff) {
			continue
		}
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	return tags, nil
}

func (s *memoryGlobalClanStore) Load(_ context.Context, tags []string) (map[string]models.BasicClanRow, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]models.BasicClanRow)
	for _, tag := range tags {
		if row, ok := s.rows[tag]; ok {
			out[tag] = row
		}
	}
	return out, nil
}

func (s *memoryGlobalClanStore) Store(_ context.Context, ingest models.GlobalClanIngest) (globalClanStoreResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := globalClanStoreResult{}
	for _, tag := range ingest.DeletedClanTags {
		if _, ok := s.rows[tag]; ok {
			delete(s.rows, tag)
			result.WriteCount++
		}
	}
	for _, row := range ingest.Clans {
		if existing, ok := s.rows[row.Tag]; ok && row.LastActive == nil {
			row.LastActive = existing.LastActive
			if !basicClanRowsEqual(existing, row) {
				result.EventClans = append(result.EventClans, row)
				result.WriteCount++
			}
		} else {
			result.EventClans = append(result.EventClans, row)
			result.WriteCount++
		}
		s.rows[row.Tag] = row
	}
	now := time.Now().UTC()
	for _, tag := range uniqueStrings(ingest.ActiveClanTags) {
		row, ok := s.rows[tag]
		if !ok {
			continue
		}
		row.LastActive = &now
		s.rows[tag] = row
		result.WriteCount++
	}
	result.WriteCount += len(ingest.Players) + len(ingest.ClanChanges) + len(ingest.JoinLeaves)
	return result, nil
}

func basicClanRowsEqual(left, right models.BasicClanRow) bool {
	return left.Tag == right.Tag &&
		left.Name == right.Name &&
		left.Description == right.Description &&
		left.ClanLevel == right.ClanLevel &&
		optionalIntEqual(left.LocationID, right.LocationID) &&
		left.CWLLeagueID == right.CWLLeagueID &&
		optionalIntEqual(left.CapitalLeagueID, right.CapitalLeagueID) &&
		left.PublicWarLog == right.PublicWarLog &&
		left.WarWins == right.WarWins &&
		left.MemberCount == right.MemberCount &&
		left.BadgeURL == right.BadgeURL &&
		left.TroopsDonated == right.TroopsDonated &&
		left.TroopsReceived == right.TroopsReceived &&
		stringSlicesEqual(left.MemberTags, right.MemberTags)
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func isActiveGlobalClan(row models.BasicClanRow, cutoff time.Time) bool {
	return row.MemberCount > 5 && row.LastActive != nil && !row.LastActive.Before(cutoff)
}
