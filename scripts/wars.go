package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
)

const (
	warsDomainName      = "wars"
	warSchedulePending  = "pending"
	warScheduleComplete = "complete"
)

// War targets are clans whose public war logs can expose current war state. A pending
// schedule means the clan is already covered by an end-time fetch.
const warTargetsSQL = `
	SELECT tag, name, cwl_league_id
	FROM basic_clan
	WHERE tag > $1
	  AND public_war_log = true
	  AND NOT EXISTS (
	    SELECT 1
	    FROM war_schedule
	    WHERE status IN ('pending', 'storing')
	      AND next_run_at > now()
	      AND (
	        source_clan_tag = basic_clan.tag
	        OR opponent_tag = basic_clan.tag
	      )
	  )
	ORDER BY tag
	LIMIT $2
`

type warsDomain struct {
	store   warStore
	targets warTargetSource

	mu        sync.Mutex
	scheduled map[string]time.Time
}

// warFetchRequest is the queue boundary between Run and do. StoreOnly requests are end-time
// fetches and must include the durable schedule metadata needed to finish the war safely.
type warFetchRequest struct {
	ClanTag     string
	OpponentTag string
	WarID       string
	PrepTime    time.Time
	EndTime     time.Time
	CWLWarTag   string
	StoreOnly   bool
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
		if req.WarID == "" {
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
	LoadCWLLeague(context.Context, string) (int, error)
	Store(context.Context, models.WarIngest) error
	ShiftMaintenance(context.Context, time.Duration) error
	Close() error
}

type warTargetSource interface {
	NextTargetBatch(context.Context, int) ([]models.BasicClanRow, error)
	NextCWLTargetBatch(context.Context, int) ([]models.BasicClanRow, error)
	Close() error
}

func NewWarsDomain() platform.Domain {
	return &warsDomain{
		scheduled: make(map[string]time.Time),
	}
}

func (d *warsDomain) Name() string { return warsDomainName }

func (d *warsDomain) Run(ctx context.Context, app *platform.App) error {
	if err := validateWarConfig(app); err != nil {
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

	if err := d.reloadSchedules(ctx, app); err != nil {
		return err
	}
	// Schedules live in Postgres, but the process-local scheduler owns the actual timers.
	// Reloading first keeps restarts from requiring a full clan scan to rediscover war ends.
	go d.runMaintenanceLoop(ctx, app)
	go d.runCWLLoop(ctx, app)

	limiter := platform.NewRequestLimiter(app.Config.WarRequestsPerSecond)
	for {
		start := time.Now()
		cycleCtx, span := platform.StartSpan(ctx, "tracker.cycle", attribute.String("domain", warsDomainName))
		err := d.runCycle(cycleCtx, app, limiter)
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		span.End()
		if err != nil {
			app.Stats.SetReady(warsDomainName, false, err.Error())
			return err
		}
		app.Stats.RecordProcess(warsDomainName, time.Since(start))
		app.Stats.SetReady(warsDomainName, true, "")
		if app.Config.RunOnce {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func validateWarConfig(app *platform.App) error {
	cfg := app.Config
	if cfg.WarRequestsPerSecond <= 0 {
		return errors.New("wars.requests_per_second must be greater than zero when wars is enabled")
	}
	if cfg.TargetPageMultiplier <= 0 {
		return errors.New("target_page_multiplier must be greater than zero when wars is enabled")
	}
	if cfg.WarCWLSyncSeconds <= 0 {
		return errors.New("wars.cwl_sync_seconds must be greater than zero when wars is enabled")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.TimescaleURL == "" {
		return errors.New("TIMESCALE_URL is required when wars is enabled")
	}
	if !cfg.DryRun && !cfg.MockDB && app.R2 == nil {
		return errors.New("R2 config is required when wars is enabled")
	}
	return nil
}

func (d *warsDomain) openStore(ctx context.Context, app *platform.App) (warStore, error) {
	if app.Config.DryRun || app.Config.MockDB {
		return newMemoryWarStore(), nil
	}
	return newTimescaleWarStore(ctx, app.Config.TimescaleURL, app.R2)
}

func (d *warsDomain) openTargetSource(ctx context.Context, app *platform.App) (warTargetSource, error) {
	if app.Config.DryRun || app.Config.MockDB {
		return newMemoryWarTargetSource(nil), nil
	}
	return newTimescaleWarTargetSource(ctx, app.Config.TimescaleURL)
}

type timescaleWarTargetSource struct {
	pool      *pgxpool.Pool
	mu        sync.Mutex
	cursor    string
	cwlCursor string
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
	targets, cursor, err := s.nextTargetBatch(ctx, limit, s.cursor)
	s.cursor = cursor
	return targets, err
}

func (s *timescaleWarTargetSource) NextCWLTargetBatch(ctx context.Context, limit int) ([]models.BasicClanRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	targets, cursor, err := s.nextTargetBatch(ctx, limit, s.cwlCursor)
	s.cwlCursor = cursor
	return targets, err
}

func (s *timescaleWarTargetSource) nextTargetBatch(ctx context.Context, limit int, cursor string) ([]models.BasicClanRow, string, error) {
	if limit <= 0 {
		return nil, cursor, nil
	}
	ctx, span := platform.StartSpan(ctx, "wars.targets",
		attribute.String("domain", warsDomainName),
		attribute.String("operation", "scan_war_targets"),
		attribute.Int("batch.size", limit),
	)
	defer span.End()
	rows, err := s.pool.Query(ctx, warTargetsSQL, cursor, limit+1)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return nil, cursor, warStoreError("targets", err)
	}
	defer rows.Close()
	targets := make([]models.BasicClanRow, 0, limit+1)
	for rows.Next() {
		var row models.BasicClanRow
		if err := rows.Scan(&row.Tag, &row.Name, &row.CWLLeagueID); err != nil {
			platform.RecordSpanError(span, err)
			span.SetAttributes(platform.SpanErrorStatus(err))
			return nil, cursor, err
		}
		targets = append(targets, row)
	}
	if err := rows.Err(); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return nil, cursor, err
	}
	nextCursor := ""
	if len(targets) > limit {
		// Fetch one extra row so we know whether to advance or wrap without a COUNT query.
		nextCursor = targets[limit-1].Tag
		targets = targets[:limit]
	}
	span.SetAttributes(attribute.Int("target.count", len(targets)), platform.SpanErrorStatus(nil))
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

func (s *memoryWarTargetSource) NextCWLTargetBatch(_ context.Context, limit int) ([]models.BasicClanRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, cursor := memoryWarTargetBatch(s.targets, s.cwlCursor, limit)
	s.cwlCursor = cursor
	return out, nil
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

func (d *warsDomain) runCycle(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter) error {
	targetPageSize := app.Config.WarRequestsPerSecond * app.Config.TargetPageMultiplier
	targets, err := d.targets.NextTargetBatch(ctx, targetPageSize)
	if err != nil {
		return err
	}
	queue := &warQueue{}
	for _, target := range targets {
		if err := queue.Enqueue(warFetchRequest{ClanTag: target.Tag}); err != nil {
			return err
		}
	}
	return d.processQueue(ctx, app, limiter, queue.items)
}

func (d *warsDomain) processQueue(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter, requests []warFetchRequest) error {
	// Max in-flight and request starts/sec are intentionally tied for this script.
	// The limiter controls starts; the slots channel caps concurrently running requests.
	slots := make(chan struct{}, app.Config.WarMaxInFlight)
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
			if err != nil {
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

func (d *warsDomain) do(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter, req warFetchRequest) (models.WarIngest, error) {
	ctx, span := platform.StartSpan(ctx, "wars.do",
		attribute.String("domain", warsDomainName),
		attribute.String("operation", "do"),
	)
	defer span.End()

	release, err := limiter.Acquire(ctx)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return models.WarIngest{}, err
	}
	start := time.Now()
	var war *clashy.ClanWar
	// CWL wars are fetched by war tag; regular wars are fetched from the clan public war log.
	if req.CWLWarTag != "" {
		wars, fetchErr := app.Clash.GetLeagueWars(ctx, []string{req.CWLWarTag})
		err = fetchErr
		if len(wars) > 0 {
			war = &wars[0]
		}
	} else {
		war, err = app.Clash.GetClanWar(ctx, req.ClanTag)
	}
	release()
	app.Stats.RecordRequest(warsDomainName, time.Since(start), err)
	if err != nil {
		if isSkippableWarFetchError(err) {
			span.SetAttributes(platform.SpanErrorStatus(nil))
			return models.WarIngest{}, nil
		}
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return models.WarIngest{}, err
	}
	if war == nil {
		span.SetAttributes(platform.SpanErrorStatus(nil))
		return models.WarIngest{}, nil
	}
	ingest, err := buildWarIngest(*war, req.ClanTag, req.StoreOnly, req.CWLWarTag)
	platform.RecordSpanError(span, err)
	span.SetAttributes(attribute.Int("rows.count", len(ingest.AttackRows)+len(ingest.IndexRows)), platform.SpanErrorStatus(err))
	return ingest, err
}

func isSkippableWarFetchError(err error) bool {
	var forbidden *clashy.Forbidden
	var privateWarLog *clashy.PrivateWarLog
	var notFound *clashy.NotFound
	return errors.As(err, &forbidden) || errors.As(err, &privateWarLog) || errors.As(err, &notFound)
}

func (d *warsDomain) storeIngest(ctx context.Context, app *platform.App, ingest models.WarIngest) error {
	if len(ingest.IndexRows) == 0 && len(ingest.AttackRows) == 0 && len(ingest.Schedules) == 0 && len(ingest.CWLGroups) == 0 {
		return nil
	}
	ctx, span := platform.StartSpan(ctx, "wars.store",
		attribute.String("domain", warsDomainName),
		attribute.String("operation", "store"),
		attribute.Int("rows.count", len(ingest.AttackRows)+len(ingest.IndexRows)),
		attribute.Int("write.count", len(ingest.AttackRows)+len(ingest.IndexRows)+len(ingest.Players)+len(ingest.Schedules)+len(ingest.CWLGroups)),
	)
	defer span.End()
	if err := d.store.Store(ctx, ingest); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	// Only arm local timers after the schedule row is durable.
	for _, schedule := range ingest.Schedules {
		d.scheduleStore(app, schedule)
	}
	app.Stats.RecordWrite(warsDomainName, len(ingest.AttackRows)+len(ingest.IndexRows)+len(ingest.Players)+len(ingest.Schedules)+len(ingest.CWLGroups))
	app.Stats.SetQueueDepth(warsDomainName, len(ingest.Schedules))
	span.SetAttributes(platform.SpanErrorStatus(nil))
	return nil
}

func buildWarIngest(war clashy.ClanWar, sourceClanTag string, finished bool, cwlWarTag string) (models.WarIngest, error) {
	if war.PreparationStartTime == nil || war.EndTime == nil {
		return models.WarIngest{}, nil
	}
	if war.Clan == nil || war.Opponent == nil || war.Clan.Tag == "" || war.Opponent.Tag == "" {
		return models.WarIngest{}, nil
	}
	prepAt := war.PreparationStartTime.Time.UTC()
	endAt := war.EndTime.Time.UTC()
	if !finished && endAt.Before(time.Now().UTC()) {
		return models.WarIngest{}, nil
	}
	startAt := optionalWarTime(war.StartTime)
	warID := models.ComputeWarID(war.Clan.Tag, war.Opponent.Tag, prepAt)
	warType := war.Type()
	if cwlWarTag != "" {
		warType = "cwl"
	}
	indexRows := []models.WarLogIndexRow{
		// Store both perspectives so either clan can load the war log without reversing joins.
		warIndexRow(warID, war.Clan, war.Opponent, prepAt, startAt, endAt, war, warType, cwlWarTag),
		warIndexRow(warID, war.Opponent, war.Clan, prepAt, startAt, endAt, war, warType, cwlWarTag),
	}
	ingest := models.WarIngest{
		IndexRows:  indexRows,
		AttackRows: warAttackRows(warID, war, warType, endAt),
		Players:    warPlayerRows(war),
	}
	if !finished {
		// Active wars are indexed now and scheduled for a final pull at their API end time.
		ingest.Schedules = []models.WarScheduleRow{{
			WarID:         warID,
			SourceClanTag: sourceClanTag,
			OpponentTag:   opponentTagForSource(sourceClanTag, war),
			PrepTime:      prepAt,
			EndTime:       endAt,
			NextRunAt:     endAt,
			CWLWarTag:     cwlWarTag,
			Status:        warSchedulePending,
		}}
		return ingest, nil
	}
	raw, err := json.Marshal(war)
	if err != nil {
		return models.WarIngest{}, err
	}
	// Finished wars include the full raw payload for R2; active-war snapshots do not.
	ingest.FinishedWarID = warID
	ingest.R2Key = warR2Key(warID)
	ingest.RawWarJSON = raw
	return ingest, nil
}

func warIndexRow(warID string, clan, opponent *clashy.WarClan, prepAt time.Time, startAt *time.Time, endAt time.Time, war clashy.ClanWar, warType, cwlWarTag string) models.WarLogIndexRow {
	return models.WarLogIndexRow{
		WarID:            warID,
		ClanTag:          clan.Tag,
		OpponentTag:      opponent.Tag,
		PrepTime:         prepAt,
		StartTime:        startAt,
		EndTime:          endAt,
		ClanBadgeURL:     clan.Badge.URL(),
		OpponentBadgeURL: opponent.Badge.URL(),
		Size:             war.TeamSize,
		WarType:          warType,
		State:            string(war.State),
		BattleModifier:   war.BattleModifier,
		CWLWarTag:        cwlWarTag,
	}
}

func warAttackRows(warID string, war clashy.ClanWar, warType string, warEndTime time.Time) []models.WarAttackRow {
	members := warMembersByTag(war)
	clans := warClanByMemberTag(war)
	attacks := war.Attacks()
	rows := make([]models.WarAttackRow, 0, len(attacks))
	for _, attack := range attacks {
		// Clash does not expose per-attack timestamps here, so analytics partition by war end.
		attacker := members[attack.AttackerTag]
		defender := members[attack.DefenderTag]
		rows = append(rows, models.WarAttackRow{
			WarID:                 warID,
			WarEndTime:            warEndTime,
			WarType:               warType,
			WarSize:               war.TeamSize,
			AttackingClanTag:      clans[attack.AttackerTag],
			DefendingClanTag:      clans[attack.DefenderTag],
			AttackerTag:           attack.AttackerTag,
			DefenderTag:           attack.DefenderTag,
			AttackerTownHall:      attacker.Townhall,
			DefenderTownHall:      defender.Townhall,
			AttackerMapPosition:   attacker.MapPosition,
			DefenderMapPosition:   defender.MapPosition,
			Stars:                 attack.Stars,
			DestructionPercentage: int(attack.Destruction),
			Duration:              attack.Duration,
			AttackOrder:           attack.Order,
			BattleModifier:        war.BattleModifier,
		})
	}
	return rows
}

func warPlayerRows(war clashy.ClanWar) []models.BasicPlayerRow {
	players := make(map[string]models.BasicPlayerRow)
	add := func(members []clashy.ClanWarMember) {
		for _, member := range members {
			if member.Tag == "" || member.Name == "" || member.Townhall <= 0 {
				continue
			}
			players[member.Tag] = models.BasicPlayerRow{
				Tag:      member.Tag,
				Name:     member.Name,
				TownHall: member.Townhall,
			}
		}
	}
	if war.Clan != nil {
		add(war.Clan.Members)
	}
	if war.Opponent != nil {
		add(war.Opponent.Members)
	}
	out := make([]models.BasicPlayerRow, 0, len(players))
	for _, player := range players {
		out = append(out, player)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tag < out[j].Tag })
	return out
}

func warMembersByTag(war clashy.ClanWar) map[string]clashy.ClanWarMember {
	out := make(map[string]clashy.ClanWarMember)
	if war.Clan != nil {
		for _, member := range war.Clan.Members {
			out[member.Tag] = member
		}
	}
	if war.Opponent != nil {
		for _, member := range war.Opponent.Members {
			out[member.Tag] = member
		}
	}
	return out
}

func warClanByMemberTag(war clashy.ClanWar) map[string]string {
	out := make(map[string]string)
	if war.Clan != nil {
		for _, member := range war.Clan.Members {
			out[member.Tag] = war.Clan.Tag
		}
	}
	if war.Opponent != nil {
		for _, member := range war.Opponent.Members {
			out[member.Tag] = war.Opponent.Tag
		}
	}
	return out
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

func optionalWarTime(value *clashy.Timestamp) *time.Time {
	if value == nil {
		return nil
	}
	out := value.Time.UTC()
	return &out
}

func warR2Key(warID string) string {
	return "wars/" + warID + ".json.snappy"
}

func (d *warsDomain) reloadSchedules(ctx context.Context, app *platform.App) error {
	// The durable schedule table is the source of truth after restarts.
	schedules, err := d.store.LoadPendingSchedules(ctx)
	if err != nil {
		return err
	}
	for _, schedule := range schedules {
		d.scheduleStore(app, schedule)
	}
	app.Stats.SetQueueDepth(warsDomainName, len(schedules))
	return nil
}

func (d *warsDomain) scheduleStore(app *platform.App, schedule models.WarScheduleRow) {
	d.mu.Lock()
	if when, exists := d.scheduled[schedule.WarID]; exists && when.Equal(schedule.NextRunAt) {
		d.mu.Unlock()
		return
	}
	d.scheduled[schedule.WarID] = schedule.NextRunAt
	d.mu.Unlock()

	// The timer performs the final end-time fetch, then persistence marks the row complete.
	app.Scheduler.Schedule(platform.Job{
		ID:   schedule.WarID,
		When: schedule.NextRunAt,
		Run: func(ctx context.Context) {
			queue := &warQueue{}
			err := queue.Enqueue(warFetchRequest{
				ClanTag:     schedule.SourceClanTag,
				OpponentTag: schedule.OpponentTag,
				WarID:       schedule.WarID,
				PrepTime:    schedule.PrepTime,
				EndTime:     schedule.EndTime,
				CWLWarTag:   schedule.CWLWarTag,
				StoreOnly:   true,
			})
			if err != nil {
				app.Logger.Error("invalid scheduled war store request", "err", err)
				return
			}
			limiter := platform.NewRequestLimiter(app.Config.WarRequestsPerSecond)
			if err := d.processQueue(ctx, app, limiter, queue.items); err != nil {
				app.Logger.Error("scheduled war store failed", "err", err)
				retry := schedule
				retry.NextRunAt = time.Now().UTC().Add(time.Minute)
				d.scheduleStore(app, retry)
			}
		},
	})
}

func (d *warsDomain) runCWLLoop(ctx context.Context, app *platform.App) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if utils.IsCWL(time.Now()) {
			if err := d.syncCWLGroups(ctx, app); err != nil {
				app.Logger.Error("cwl sync failed", "err", err)
			}
		}
		timer.Reset(time.Duration(app.Config.WarCWLSyncSeconds) * time.Second)
	}
}

func (d *warsDomain) syncCWLGroups(ctx context.Context, app *platform.App) error {
	targets, err := d.targets.NextCWLTargetBatch(ctx, app.Config.WarRequestsPerSecond*app.Config.TargetPageMultiplier)
	if err != nil {
		return err
	}
	season := utils.CurrentSeason(time.Now())
	seen := make(map[string]struct{})
	limiter := platform.NewRequestLimiter(app.Config.WarRequestsPerSecond)
	for _, target := range targets {
		release, err := limiter.Acquire(ctx)
		if err != nil {
			return err
		}
		start := time.Now()
		group, err := app.Clash.GetLeagueGroup(ctx, target.Tag)
		release()
		app.Stats.RecordRequest(warsDomainName, time.Since(start), err)
		if err != nil || group == nil || group.Season != season {
			continue
		}
		cwlID, clanTags := cwlGroupID(group)
		if cwlID == "" {
			continue
		}
		if _, ok := seen[cwlID]; ok {
			continue
		}
		seen[cwlID] = struct{}{}
		// CWL league comes from the tracked source clan row, not extra clan discovery calls.
		leagueID := target.CWLLeagueID
		if leagueID == 0 {
			leagueID, err = d.store.LoadCWLLeague(ctx, target.Tag)
			if err != nil {
				return err
			}
		}
		ingest := models.WarIngest{
			CWLGroups: []models.CWLGroupRow{{
				CWLID:       cwlID,
				Season:      group.Season,
				CWLLeagueID: leagueID,
				ClanTags:    clanTags,
				Rounds:      cwlRounds(group),
				Data:        group,
			}},
		}
		if err := d.storeIngest(ctx, app, ingest); err != nil {
			return err
		}
		if err := d.scheduleCWLWars(ctx, app, limiter, group); err != nil {
			return err
		}
	}
	return nil
}

func (d *warsDomain) scheduleCWLWars(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter, group *clashy.ClanWarLeagueGroup) error {
	for _, warTag := range cwlWarTags(group) {
		release, err := limiter.Acquire(ctx)
		if err != nil {
			return err
		}
		start := time.Now()
		wars, err := app.Clash.GetLeagueWars(ctx, []string{warTag})
		release()
		app.Stats.RecordRequest(warsDomainName, time.Since(start), err)
		if err != nil || len(wars) == 0 {
			continue
		}
		source := ""
		if wars[0].Clan != nil {
			source = wars[0].Clan.Tag
		}
		ingest, err := buildWarIngest(wars[0], source, false, warTag)
		if err != nil {
			return err
		}
		if err := d.storeIngest(ctx, app, ingest); err != nil {
			return err
		}
	}
	return nil
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
	return group.Season + "-" + strings.Join(keys, "-"), tags
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

func cwlWarTags(group *clashy.ClanWarLeagueGroup) []string {
	var out []string
	for _, round := range cwlRounds(group) {
		out = append(out, round...)
	}
	return out
}

func (d *warsDomain) runMaintenanceLoop(ctx context.Context, app *platform.App) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
		}
		if err := d.checkMaintenance(ctx, app); err != nil {
			app.Logger.Error("maintenance check failed", "err", err)
		}
	}
}

func (d *warsDomain) checkMaintenance(ctx context.Context, app *platform.App) error {
	start := time.Now().UTC()
	inMaintenance := false
	for {
		// Gold pass season is a cheap global endpoint; failures here are used as the
		// maintenance signal so pending war end times can be shifted by observed downtime.
		_, err := app.Clash.GetCurrentGoldPassSeason(ctx)
		if err == nil {
			break
		}
		if !inMaintenance {
			inMaintenance = true
			_ = app.PublishEvent(ctx, platform.Event{Topic: "maintenance", Value: map[string]any{"status": "start"}})
		}
		timer := time.NewTimer(15 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	if !inMaintenance {
		return nil
	}
	duration := time.Since(start)
	if duration <= 0 {
		return nil
	}
	if err := d.store.ShiftMaintenance(ctx, duration); err != nil {
		return err
	}
	if err := d.reloadSchedules(ctx, app); err != nil {
		return err
	}
	_ = app.PublishEvent(ctx, platform.Event{Topic: "maintenance", Value: map[string]any{"status": "end", "duration_seconds": int(duration.Seconds())}})
	return nil
}

func warStoreError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("war store %s: %w", operation, err)
}
