package scripts

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
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
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const warsDomainName = "wars"

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
	    WHERE (
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
	limiter *clashy.Limiter

	mu        sync.Mutex
	scheduled map[string]time.Time
}

// warFetchRequest is the queue boundary between Run and do. StoreOnly requests are end-time
// fetches and must include the durable schedule metadata needed to finish the war safely.
type warFetchRequest struct {
	ClanTag     string
	OpponentTag string
	ScheduleKey string
	WarID       string
	PrepTime    time.Time
	EndTime     time.Time
	WarTag      string
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
		if req.ScheduleKey == "" {
			return errors.New("war queue: schedule key is required for store work")
		}
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
	LoadActiveCurrentWarTimer(context.Context, string) (models.CurrentWarTimerRow, bool, error)
	DeleteExpiredCurrentWarTimers(context.Context) (int, error)
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
	limiter, err := newWarLimiter(app)
	if err != nil {
		return err
	}
	d.limiter = limiter
	runCtx, stopBackground := context.WithCancel(ctx)
	var background sync.WaitGroup
	defer func() {
		stopBackground()
		background.Wait()
	}()

	if err := d.reloadSchedules(runCtx, app); err != nil {
		return err
	}
	// Schedules live in Postgres, but the process-local scheduler owns the actual timers.
	// Reloading first keeps restarts from requiring a full clan scan to rediscover war ends.
	background.Add(3)
	go func() {
		defer background.Done()
		d.runMaintenanceLoop(runCtx, app)
	}()
	go func() {
		defer background.Done()
		d.runCurrentWarTimerCleanupLoop(runCtx, app)
	}()
	go func() {
		defer background.Done()
		d.runCWLLoop(runCtx, app, limiter)
	}()
	for {
		start := time.Now()
		err := d.runCycle(runCtx, app, limiter)
		if err != nil {
			app.Stats.SetReady(warsDomainName, false, err.Error())
			return err
		}
		app.Stats.RecordProcess(warsDomainName, time.Since(start))
		app.Stats.SetReady(warsDomainName, true, "")
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
	rows, err := s.pool.Query(ctx, warTargetsSQL, cursor, limit+1)
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

func newWarLimiter(app *platform.App) (*clashy.Limiter, error) {
	return clashy.NewLimiter(app.Config.WarRequestsPerSecond, app.Config.WarMaxInFlight)
}

func (d *warsDomain) runCycle(ctx context.Context, app *platform.App, limiter *clashy.Limiter) error {
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

func (d *warsDomain) processQueue(ctx context.Context, app *platform.App, limiter *clashy.Limiter, requests []warFetchRequest) error {
	// The limiter caps request starts while the larger in-flight pool prevents
	// normal proxy latency from lowering the configured starts-per-second rate.
	maxInFlight := app.Config.WarMaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = app.Config.WarRequestsPerSecond
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

func (d *warsDomain) do(ctx context.Context, app *platform.App, limiter *clashy.Limiter, req warFetchRequest) (models.WarIngest, error) {
	war, err := retryLimitedClashFetch(ctx, limiter, func(fetchCtx context.Context) (*clashy.ClanWar, error) {
		start := time.Now()
		var war *clashy.ClanWar
		var fetchErr error
		// CWL wars are fetched by war tag; regular wars are fetched from the clan public war log.
		if req.WarTag != "" {
			wars, err := app.Clash.GetLeagueWars(fetchCtx, []string{req.WarTag})
			fetchErr = err
			if len(wars) > 0 {
				war = &wars[0]
			}
		} else {
			war, fetchErr = app.Clash.GetClanWar(fetchCtx, req.ClanTag)
		}
		app.Stats.RecordRequest(warsDomainName, time.Since(start), fetchErr)
		return war, fetchErr
	})
	if err != nil {
		if isSkippableWarFetchError(err) {
			if req.StoreOnly {
				return models.WarIngest{}, fmt.Errorf("scheduled war %s is not available yet: %w", req.WarID, err)
			}
			return models.WarIngest{}, nil
		}
		return models.WarIngest{}, err
	}
	if war == nil {
		if req.StoreOnly {
			return models.WarIngest{}, fmt.Errorf("scheduled war %s returned no war", req.WarID)
		}
		return models.WarIngest{}, nil
	}
	if req.StoreOnly && war.State != clashy.WarStateEnded {
		return models.WarIngest{}, fmt.Errorf("scheduled war %s is still in state %s", req.WarID, war.State)
	}
	ingest, err := buildWarIngest(*war, req.ClanTag, req.StoreOnly, req.WarTag, req.ScheduleKey, req.WarID)
	if err != nil {
		return models.WarIngest{}, err
	}
	if req.StoreOnly && len(ingest.IndexRows) == 0 {
		return models.WarIngest{}, fmt.Errorf("scheduled war %s produced no finished ingest", req.WarID)
	}
	return ingest, nil
}

func isSkippableWarFetchError(err error) bool {
	var forbidden *clashy.Forbidden
	var privateWarLog *clashy.PrivateWarLog
	var notFound *clashy.NotFound
	return errors.As(err, &forbidden) || errors.As(err, &privateWarLog) || errors.As(err, &notFound)
}

func (d *warsDomain) storeIngest(ctx context.Context, app *platform.App, ingest models.WarIngest) error {
	if len(ingest.IndexRows) == 0 && len(ingest.AttackRows) == 0 && len(ingest.Schedules) == 0 && len(ingest.CurrentWarTimers) == 0 && len(ingest.CWLGroups) == 0 {
		return nil
	}
	if err := d.store.Store(ctx, ingest); err != nil {
		return err
	}
	// Only arm local timers after the schedule row is durable.
	for _, schedule := range ingest.Schedules {
		d.scheduleStore(app, schedule)
	}
	app.Stats.RecordWrite(warsDomainName, len(ingest.AttackRows)+len(ingest.IndexRows)+len(ingest.Players)+len(ingest.Schedules)+len(ingest.CurrentWarTimers)+len(ingest.CWLGroups))
	app.Stats.SetQueueDepth(warsDomainName, len(ingest.Schedules))
	return nil
}

func buildWarIngest(war clashy.ClanWar, sourceClanTag string, finished bool, warTag, scheduleKey, warID string) (models.WarIngest, error) {
	if war.PreparationStartTime == nil || war.EndTime == nil {
		return models.WarIngest{}, nil
	}
	if war.Clan == nil || war.Opponent == nil || war.Clan.Tag == "" || war.Opponent.Tag == "" {
		return models.WarIngest{}, nil
	}
	prepAt := war.PreparationStartTime.Time.UTC()
	endAt := war.EndTime.Time.UTC()
	if !finished && !endAt.After(time.Now().UTC()) {
		return models.WarIngest{}, nil
	}
	startAt := optionalWarTime(war.StartTime)
	if scheduleKey == "" {
		scheduleKey = models.ComputeWarKey(war.Clan.Tag, war.Opponent.Tag, prepAt)
	}
	if warID == "" {
		id, err := uuid.NewV7()
		if err != nil {
			return models.WarIngest{}, err
		}
		warID = id.String()
	}
	warType := war.Type()
	if warTag != "" {
		warType = "cwl"
	}
	if !finished {
		// Active wars only create an end-time schedule. Permanent war rows and R2
		// payloads are written by the scheduler after the war has ended.
		return models.WarIngest{
			Schedules: []models.WarScheduleRow{{
				ScheduleKey:   scheduleKey,
				WarID:         warID,
				SourceClanTag: sourceClanTag,
				OpponentTag:   opponentTagForSource(sourceClanTag, war),
				PrepTime:      prepAt,
				EndTime:       endAt,
				NextRunAt:     endAt,
				WarTag:        warTag,
			}},
			CurrentWarTimers: currentWarTimerRows(warID, war.Clan, war.Opponent, endAt),
		}, nil
	}
	clan, opponent := canonicalWarSides(war.Clan, war.Opponent)
	indexRows := []models.WarLogIndexRow{
		warIndexRow(warID, clan, opponent, prepAt, startAt, endAt, war, warType, warTag),
	}
	ingest := models.WarIngest{
		IndexRows:  indexRows,
		AttackRows: warAttackRows(warID, war, warType, endAt),
		Players:    warPlayerRows(war),
	}
	raw, err := json.Marshal(war)
	if err != nil {
		return models.WarIngest{}, err
	}
	// Finished wars include the full raw payload for R2; active-war snapshots do not.
	ingest.FinishedScheduleKey = scheduleKey
	ingest.FinishedWarID = warID
	ingest.RawWarJSON = raw
	return ingest, nil
}

func currentWarTimerRows(warID string, clan, opponent *clashy.WarClan, endAt time.Time) []models.CurrentWarTimerRow {
	if warID == "" || clan == nil || opponent == nil || clan.Tag == "" || opponent.Tag == "" || endAt.IsZero() {
		return nil
	}
	rows := make([]models.CurrentWarTimerRow, 0, len(clan.Members)+len(opponent.Members))
	seen := make(map[string]struct{}, len(clan.Members)+len(opponent.Members))
	appendSide := func(members []clashy.ClanWarMember, clanTag, opponentTag string) {
		for _, member := range members {
			if member.Tag == "" {
				continue
			}
			if _, exists := seen[member.Tag]; exists {
				continue
			}
			seen[member.Tag] = struct{}{}
			rows = append(rows, models.CurrentWarTimerRow{PlayerTag: member.Tag, WarID: warID, ClanTag: clanTag, OpponentTag: opponentTag, EndTime: endAt})
		}
	}
	appendSide(clan.Members, clan.Tag, opponent.Tag)
	appendSide(opponent.Members, opponent.Tag, clan.Tag)
	return rows
}

func canonicalWarSides(clan, opponent *clashy.WarClan) (*clashy.WarClan, *clashy.WarClan) {
	if clan != nil && opponent != nil && clan.Tag > opponent.Tag {
		return opponent, clan
	}
	return clan, opponent
}

func warIndexRow(warID string, clan, opponent *clashy.WarClan, prepAt time.Time, startAt *time.Time, endAt time.Time, war clashy.ClanWar, warType, warTag string) models.WarLogIndexRow {
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
		BattleModifier:                string(war.BattleModifier),
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
			DefenderName:          defender.Name,
			AttackerTownHall:      attacker.Townhall,
			DefenderTownHall:      defender.Townhall,
			AttackerMapPosition:   attacker.MapPosition,
			DefenderMapPosition:   defender.MapPosition,
			Stars:                 attack.Stars,
			DestructionPercentage: int(attack.Destruction),
			Duration:              attack.Duration,
			AttackOrder:           attack.Order,
			BattleModifier:        string(war.BattleModifier),
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
	return warID
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
	if when, exists := d.scheduled[schedule.ScheduleKey]; exists && when.Equal(schedule.NextRunAt) {
		d.mu.Unlock()
		return
	}
	d.scheduled[schedule.ScheduleKey] = schedule.NextRunAt
	d.mu.Unlock()

	// The timer performs the final end-time fetch, then persistence marks the row complete.
	app.Scheduler.Schedule(platform.Job{
		ID:   schedule.ScheduleKey,
		When: schedule.NextRunAt,
		Run: func(ctx context.Context) {
			queue := &warQueue{}
			err := queue.Enqueue(warFetchRequest{
				ClanTag:     schedule.SourceClanTag,
				OpponentTag: schedule.OpponentTag,
				ScheduleKey: schedule.ScheduleKey,
				WarID:       schedule.WarID,
				PrepTime:    schedule.PrepTime,
				EndTime:     schedule.EndTime,
				WarTag:      schedule.WarTag,
				StoreOnly:   true,
			})
			if err != nil {
				app.Logger.Error("invalid scheduled war store request", "err", err)
				return
			}
			if d.limiter == nil {
				app.Logger.Error("scheduled war limiter is not initialized")
				return
			}
			if err := d.processQueue(ctx, app, d.limiter, queue.items); err != nil {
				app.Logger.Error("scheduled war store failed", "err", err)
				retry := schedule
				retry.NextRunAt = time.Now().UTC().Add(time.Minute)
				d.scheduleStore(app, retry)
			}
		},
	})
}

func (d *warsDomain) runCWLLoop(ctx context.Context, app *platform.App, limiter *clashy.Limiter) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if utils.IsCWL(time.Now()) {
			if err := d.syncCWLGroups(ctx, app, limiter); err != nil {
				app.Logger.Error("cwl sync failed", "err", err)
			}
		}
		timer.Reset(time.Duration(app.Config.WarCWLSyncSeconds) * time.Second)
	}
}

func (d *warsDomain) syncCWLGroups(ctx context.Context, app *platform.App, limiter *clashy.Limiter) error {
	targets, err := d.targets.NextCWLTargetBatch(ctx, app.Config.WarRequestsPerSecond*app.Config.TargetPageMultiplier)
	if err != nil {
		return err
	}
	season := utils.CurrentSeason(time.Now())
	seenGroups := make(map[string]struct{})
	coveredClans := make(map[string]struct{})
	for _, target := range targets {
		if _, covered := coveredClans[target.Tag]; covered {
			continue
		}
		group, err := retryLimitedClashFetch(ctx, limiter, func(fetchCtx context.Context) (*clashy.ClanWarLeagueGroup, error) {
			start := time.Now()
			group, err := app.Clash.GetLeagueGroup(fetchCtx, target.Tag)
			app.Stats.RecordRequest(warsDomainName, time.Since(start), err)
			return group, err
		})
		if err != nil || group == nil || group.Season != season {
			continue
		}
		cwlID, clanTags := cwlGroupID(group)
		if cwlID == "" {
			continue
		}
		for _, clanTag := range clanTags {
			coveredClans[clanTag] = struct{}{}
		}
		if _, ok := seenGroups[cwlID]; ok {
			continue
		}
		seenGroups[cwlID] = struct{}{}
		// CWL league comes from the tracked source clan row, not extra clan discovery calls.
		leagueID := target.CWLLeagueID
		if leagueID == 0 {
			leagueID, err = d.store.LoadCWLLeague(ctx, target.Tag)
			if err != nil {
				return err
			}
		}
		groupRow := cwlGroupRow(cwlID, group, leagueID)
		warSize, err := d.scheduleCWLWars(ctx, app, limiter, group)
		if err != nil {
			return err
		}
		if warSize > 0 {
			groupRow.WarSize = intPtr(warSize)
		}
		if err := d.storeIngest(ctx, app, models.WarIngest{CWLGroups: []models.CWLGroupRow{groupRow}}); err != nil {
			return err
		}
	}
	return nil
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
		rows = append(rows, models.CWLGroupClanRow{ClanTag: clan.Tag, Name: clan.Name, ClanLevel: clan.Level, BadgeToken: badgeToken(clan.Badge)})
	}
	return rows
}

func (d *warsDomain) scheduleCWLWars(ctx context.Context, app *platform.App, limiter *clashy.Limiter, group *clashy.ClanWarLeagueGroup) (int, error) {
	warSize := 0
	for _, warTag := range warTags(group) {
		wars, err := retryLimitedClashFetch(ctx, limiter, func(fetchCtx context.Context) ([]clashy.ClanWar, error) {
			start := time.Now()
			wars, err := app.Clash.GetLeagueWars(fetchCtx, []string{warTag})
			app.Stats.RecordRequest(warsDomainName, time.Since(start), err)
			return wars, err
		})
		if err != nil || len(wars) == 0 {
			continue
		}
		source := ""
		if wars[0].Clan != nil {
			source = wars[0].Clan.Tag
		}
		ingest, err := buildWarIngest(wars[0], source, false, warTag, "", "")
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
	legacyIdentity := group.Season + "-" + strings.Join(keys, "-")
	return stableCWLID(legacyIdentity), tags
}

func stableCWLID(legacyIdentity string) string {
	sum := sha256.Sum256([]byte(legacyIdentity))
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

const currentWarTimerCleanupInterval = 5 * time.Minute

func (d *warsDomain) runCurrentWarTimerCleanupLoop(ctx context.Context, app *platform.App) {
	ticker := time.NewTicker(currentWarTimerCleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted, err := d.store.DeleteExpiredCurrentWarTimers(ctx)
			if err != nil {
				app.Logger.Error("current war timer cleanup failed", "err", err)
				continue
			}
			if deleted > 0 {
				app.Stats.RecordWrite(warsDomainName, deleted)
			}
		}
	}
}

func (d *warsDomain) checkMaintenance(ctx context.Context, app *platform.App) error {
	var start time.Time
	inMaintenance := false
	for {
		// Gold pass season is a cheap global endpoint; failures here are used as the
		// maintenance signal so pending war end times can be shifted by observed downtime.
		_, err := app.Clash.GetCurrentGoldPassSeason(ctx)
		if err == nil {
			break
		}
		if !isOfficialMaintenance500(err) {
			return err
		}
		if !inMaintenance {
			inMaintenance = true
			start = time.Now().UTC()
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
	duration, ok := maintenanceShiftDuration(start, time.Now().UTC())
	if !ok {
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

func isOfficialMaintenance500(err error) bool {
	var gateway *clashy.GatewayError
	return errors.As(err, &gateway) && gateway.HTTPException != nil && gateway.Status == 500
}

func maintenanceShiftDuration(start, recovered time.Time) (time.Duration, bool) {
	if start.IsZero() || !recovered.After(start) {
		return 0, false
	}
	return recovered.Sub(start), true
}

func warStoreError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("war store %s: %w", operation, err)
}
