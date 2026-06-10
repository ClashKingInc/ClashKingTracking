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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel/attribute"
)

const (
	globalClansDomainName = "globalclans"
	unrankedWarLeagueID   = 48000000

	globalClanActiveCursorKey   = "globalclans:cursor:active"
	globalClanInactiveCursorKey = "globalclans:cursor:inactive"
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
	  AND (
	    member_count <= 5
	    OR last_active IS NULL
	    OR last_active < now() - interval '7 days'
	  )
	ORDER BY tag
	LIMIT $2
`

type globalClansDomain struct {
	store globalClanStore
}

type globalClanGroup struct {
	Name              string
	Bucket            string
	RequestsPerSecond int
	MaxInFlight       int
	TargetPageSize    int
}

type globalClanStore interface {
	NextTargetBatch(context.Context, string, int) (globalClanTargetBatch, error)
	CommitTargetBatch(context.Context, globalClanTargetBatch) error
	Load(context.Context, []string) (map[string]models.BasicClanRow, error)
	Store(context.Context, models.GlobalClanIngest) (globalClanStoreResult, error)
	Close() error
}

type globalClanStoreResult struct {
	WriteCount int
	EventClans []models.BasicClanRow
}

type globalClanTargetBatch struct {
	Bucket         string
	Tags           []string
	ActiveCursor   string
	InactiveCursor string
	ActiveCount    int
	InactiveCount  int
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
	// active clans, and each loop can keep its own durable cursor.
	groups := []globalClanGroup{
		{
			Name:              "priority",
			Bucket:            "active",
			RequestsPerSecond: app.Config.GlobalClanPriorityRequestsPerSecond,
			MaxInFlight:       app.Config.GlobalClanPriorityRequestsPerSecond,
			TargetPageSize: app.Config.GlobalClanPriorityRequestsPerSecond *
				app.Config.TargetPageMultiplier,
		},
		{
			Name:              "non_priority",
			Bucket:            "inactive",
			RequestsPerSecond: app.Config.GlobalClanNonPriorityRequestsPerSecond,
			MaxInFlight:       app.Config.GlobalClanNonPriorityRequestsPerSecond,
			TargetPageSize: app.Config.GlobalClanNonPriorityRequestsPerSecond *
				app.Config.TargetPageMultiplier,
		},
	}

	return d.runGroups(ctx, app, groups)
}

func (d *globalClansDomain) openStore(ctx context.Context, app *platform.App) (globalClanStore, error) {
	if app.Config.DryRun || app.Config.MockDB {
		return newMemoryGlobalClanStore(), nil
	}
	return newTimescaleGlobalClanStore(ctx, app.Config.TimescaleURL, app.Valkey)
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
		return errors.New("valkey_addr is required for globalclans cursor persistence")
	}
	return nil
}

func (d *globalClansDomain) runGroups(ctx context.Context, app *platform.App, groups []globalClanGroup) error {
	if app.Config.RunOnce {
		for _, group := range groups {
			if err := d.runGroup(ctx, app, group); err != nil {
				return err
			}
		}
		return nil
	}
	errCh := make(chan error, len(groups))
	for _, group := range groups {
		group := group
		go func() {
			errCh <- d.runGroupLoop(ctx, app, group)
		}()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (d *globalClansDomain) runGroupLoop(ctx context.Context, app *platform.App, group globalClanGroup) error {
	for {
		start := time.Now()
		cycleCtx, span := platform.StartSpan(ctx, "tracker.cycle",
			attribute.String("domain", globalClansDomainName),
			attribute.String("group", group.Name),
		)
		err := d.runGroup(cycleCtx, app, group)
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		span.End()
		if err != nil {
			return err
		}
		if app.Stats.Domain(globalClansDomainName).Healthy {
			app.Stats.RecordProcess(globalClansDomainName, time.Since(start))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (d *globalClansDomain) runGroup(ctx context.Context, app *platform.App, group globalClanGroup) error {
	ctx, span := platform.StartSpan(ctx, "tracker.group",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", group.Name),
	)
	defer span.End()

	limiter := platform.NewRequestLimiter(group.RequestsPerSecond)
	batch, err := d.store.NextTargetBatch(ctx, group.Bucket, group.TargetPageSize)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	if len(batch.Tags) > 0 {
		ingest, processErr := d.do(ctx, app, limiter, group, batch.Tags)
		if globalClanIngestWriteCount(ingest) > 0 {
			err = d.storeBatch(ctx, app, group.Name, ingest)
		}
		if err == nil && processErr == nil {
			// Advance the cursor only after the API results and SQL writes complete.
			err = d.store.CommitTargetBatch(ctx, batch)
		}
		if err == nil {
			err = processErr
		}
		if err != nil {
			app.Stats.SetReady(globalClansDomainName, false, err.Error())
			platform.RecordSpanError(span, err)
			span.SetAttributes(
				attribute.Int("target.count", len(batch.Tags)),
				attribute.Int("active.count", batch.ActiveCount),
				attribute.Int("inactive.count", batch.InactiveCount),
				platform.SpanErrorStatus(err),
			)
			return err
		}
	} else if err := d.store.CommitTargetBatch(ctx, batch); err != nil {
		app.Stats.SetReady(globalClansDomainName, false, err.Error())
		platform.RecordSpanError(span, err)
		span.SetAttributes(
			attribute.Int("target.count", 0),
			attribute.Int("active.count", batch.ActiveCount),
			attribute.Int("inactive.count", batch.InactiveCount),
			platform.SpanErrorStatus(err),
		)
		return err
	}
	app.Stats.SetReady(globalClansDomainName, true, "")
	span.SetAttributes(
		attribute.Int("target.count", len(batch.Tags)),
		attribute.Int("active.count", batch.ActiveCount),
		attribute.Int("inactive.count", batch.InactiveCount),
		platform.SpanErrorStatus(nil),
	)
	return nil
}

func (d *globalClansDomain) do(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter, group globalClanGroup, tags []string) (models.GlobalClanIngest, error) {
	ctx, span := platform.StartSpan(ctx, "globalclans.do",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", group.Name),
		attribute.String("operation", "prepare_clans"),
		attribute.Int("batch.size", len(tags)),
	)
	defer span.End()

	previous, err := d.store.Load(ctx, tags)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return models.GlobalClanIngest{}, err
	}

	type result struct {
		tag     string
		clan    *clashy.Clan
		latency time.Duration
		error   error
	}
	fetchCtx, fetchSpan := platform.StartSpan(ctx, "clash.fetch.batch",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", group.Name),
		attribute.String("operation", "clan"),
		attribute.Int("batch.size", len(tags)),
	)
	fetchBatchStart := time.Now()
	results := make(chan result, len(tags))
	slots := make(chan struct{}, group.MaxInFlight)
	var wg sync.WaitGroup
	for _, tag := range tags {
		if tag == "" {
			continue
		}
		select {
		case slots <- struct{}{}:
		case <-fetchCtx.Done():
			err := fetchCtx.Err()
			platform.RecordSpanError(fetchSpan, err)
			fetchSpan.SetAttributes(platform.SpanErrorStatus(err))
			fetchSpan.End()
			return models.GlobalClanIngest{}, err
		}
		wg.Add(1)
		go func(tag string) {
			defer wg.Done()
			defer func() { <-slots }()
			start := time.Now()
			clan, err := fetchGlobalClan(fetchCtx, app, limiter, group.Name, tag)
			results <- result{tag: tag, clan: clan, latency: time.Since(start), error: err}
		}(tag)
	}
	wg.Wait()
	close(results)

	now := time.Now().UTC()
	var ingest models.GlobalClanIngest
	fetchCount := 0
	fetchFailures := 0
	var fetchTotal time.Duration
	var fetchMax time.Duration
	for result := range results {
		fetchCount++
		fetchTotal += result.latency
		if result.latency > fetchMax {
			fetchMax = result.latency
		}
		app.Stats.RecordRequest(globalClansDomainName, result.latency, result.error)
		if result.error != nil {
			app.Logger.Error("global clan fetch failed", "tag", result.tag, "err", result.error)
			fetchFailures++
			continue
		}
		if result.clan == nil {
			continue
		}
		if result.clan.Tag == "" {
			result.clan.Tag = result.tag
		}
		prev, ok := previous[result.clan.Tag]
		var previousClan *models.BasicClanRow
		if ok {
			previousClan = &prev
		}
		ingest = mergeGlobalClanIngest(ingest, buildGlobalClanIngest(*result.clan, previousClan, now))
	}
	fetchAverage := 0.0
	if fetchCount > 0 {
		fetchAverage = durationMillis(fetchTotal) / float64(fetchCount)
	}
	var processErr error
	if fetchFailures > 0 {
		processErr = errors.New("one or more global clan fetches failed")
	}
	platform.RecordSpanError(fetchSpan, processErr)
	fetchSpan.SetAttributes(
		attribute.Int("request.count", fetchCount),
		attribute.Int("request.success.count", fetchCount-fetchFailures),
		attribute.Int("request.error.count", fetchFailures),
		attribute.Float64("request.duration.sum_ms", durationMillis(fetchTotal)),
		attribute.Float64("request.duration.avg_ms", fetchAverage),
		attribute.Float64("request.duration.max_ms", durationMillis(fetchMax)),
		attribute.Float64("batch.wall_ms", durationMillis(time.Since(fetchBatchStart))),
		platform.SpanErrorStatus(processErr),
	)
	fetchSpan.End()
	span.SetAttributes(
		attribute.Int("rows.count", globalClanIngestWriteCount(ingest)),
		attribute.Int("error.count", fetchFailures),
		platform.SpanErrorStatus(processErr),
	)
	return ingest, processErr
}

func fetchGlobalClan(ctx context.Context, app *platform.App, limiter *platform.RequestLimiter, group, tag string) (*clashy.Clan, error) {
	release, err := limiter.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	fetchCtx, span := platform.StartSpan(ctx, "clash.fetch",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", group),
		attribute.String("operation", "clan"),
	)
	clan, err := app.Clash.GetClan(fetchCtx, tag)
	platform.RecordSpanError(span, err)
	span.SetAttributes(platform.SpanErrorStatus(err))
	span.End()
	if err != nil || clan == nil {
		return clan, err
	}
	if clan.Tag == "" && clan.Name == "" {
		return nil, fmt.Errorf("empty clan payload for %s", tag)
	}
	return clan, nil
}

func durationMillis(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func (d *globalClansDomain) storeBatch(ctx context.Context, app *platform.App, group string, ingest models.GlobalClanIngest) error {
	ctx, span := platform.StartSpan(ctx, "globalclans.store",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", group),
		attribute.String("operation", "store_clans"),
		attribute.Int("rows.count", len(ingest.Clans)+len(ingest.Players)+len(ingest.ActiveClanTags)+len(ingest.DeletedClanTags)+len(ingest.ClanChanges)+len(ingest.JoinLeaves)),
	)
	defer span.End()

	result, err := d.store.Store(ctx, ingest)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	writeCount := result.WriteCount
	app.Stats.RecordWrite(globalClansDomainName, writeCount)
	if err := publishGlobalClanEvents(ctx, app, group, result.EventClans); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(attribute.Int("write.count", writeCount), platform.SpanErrorStatus(err))
		return err
	}
	span.SetAttributes(attribute.Int("write.count", writeCount), platform.SpanErrorStatus(nil))
	return nil
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

func buildGlobalClanIngest(current clashy.Clan, previous *models.BasicClanRow, now time.Time) models.GlobalClanIngest {
	if current.MemberCount == 0 {
		return models.GlobalClanIngest{DeletedClanTags: []string{current.Tag}}
	}
	row := basicClanRow(current)
	ingest := models.GlobalClanIngest{
		Clans:   []models.BasicClanRow{row},
		Players: basicPlayerRows(current.Members),
	}
	if previous == nil {
		return ingest
	}
	// Joins are the only member movement that marks a clan active. Leave-only changes are
	// still persisted, but they should not promote a clan into the priority polling bucket.
	ingest.JoinLeaves = joinLeaveRows(*previous, current, now)
	if hasJoin(ingest.JoinLeaves) {
		ingest.ActiveClanTags = []string{current.Tag}
	}
	ingest.ClanChanges = clanChangeRows(*previous, row, now)
	return ingest
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

func basicPlayerRows(members []clashy.ClanMember) []models.BasicPlayerRow {
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
	pool   *pgxpool.Pool
	valkey valkey.Client
}

func newTimescaleGlobalClanStore(ctx context.Context, dsn string, client valkey.Client) (*timescaleGlobalClanStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &timescaleGlobalClanStore{pool: pool, valkey: client}, nil
}

func (s *timescaleGlobalClanStore) Close() error {
	if s == nil || s.pool == nil {
		return nil
	}
	s.pool.Close()
	return nil
}

func (s *timescaleGlobalClanStore) NextTargetBatch(ctx context.Context, bucket string, limit int) (globalClanTargetBatch, error) {
	if limit <= 0 {
		return globalClanTargetBatch{}, nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.basic_clan.targets",
		attribute.String("domain", globalClansDomainName),
		attribute.String("group", bucket),
		attribute.String("operation", "scan_basic_clan_targets"),
		attribute.Int("batch.size", limit),
	)
	defer span.End()

	cursorKey := globalClanActiveCursorKey
	if bucket == "inactive" {
		cursorKey = globalClanInactiveCursorKey
	}
	cursor, err := s.getCursor(ctx, cursorKey)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanTargetBatch{}, err
	}
	tags, nextCursor, err := s.scanClanBucket(ctx, bucket, cursor, limit)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return globalClanTargetBatch{}, err
	}

	batch := globalClanTargetBatch{Bucket: bucket, Tags: tags}
	if bucket == "inactive" {
		batch.InactiveCursor = nextCursor
		batch.InactiveCount = len(tags)
	} else {
		batch.ActiveCursor = nextCursor
		batch.ActiveCount = len(tags)
	}
	span.SetAttributes(
		attribute.Int("target.count", len(batch.Tags)),
		attribute.Int("active.count", batch.ActiveCount),
		attribute.Int("inactive.count", batch.InactiveCount),
		platform.SpanErrorStatus(nil),
	)
	return batch, nil
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

func (s *timescaleGlobalClanStore) getCursor(ctx context.Context, key string) (string, error) {
	if s.valkey == nil {
		return "", nil
	}
	ctx, span := platform.StartSpan(ctx, "valkey.globalclans_cursor.get",
		attribute.String("domain", globalClansDomainName),
		attribute.String("operation", "get"),
	)
	defer span.End()
	value, err := s.valkey.Do(ctx, s.valkey.B().Get().Key(key).Build()).ToString()
	if valkey.IsValkeyNil(err) {
		span.SetAttributes(attribute.Int("rows.count", 0), platform.SpanErrorStatus(nil))
		return "", nil
	}
	platform.RecordSpanError(span, err)
	span.SetAttributes(attribute.Int("rows.count", 1), platform.SpanErrorStatus(err))
	return value, err
}

func (s *timescaleGlobalClanStore) CommitTargetBatch(ctx context.Context, batch globalClanTargetBatch) error {
	if s.valkey == nil {
		return nil
	}
	key := globalClanActiveCursorKey
	cursor := batch.ActiveCursor
	if batch.Bucket == "inactive" {
		key = globalClanInactiveCursorKey
		cursor = batch.InactiveCursor
	}
	// Empty cursor deletes the checkpoint, causing the next scan to wrap this bucket.
	command := cursorCommand(s.valkey, key, cursor)
	ctx, span := platform.StartSpan(ctx, "valkey.globalclans_cursor.set",
		attribute.String("domain", globalClansDomainName),
		attribute.String("operation", "set"),
		attribute.Int("write.count", 1),
	)
	defer span.End()
	err := s.valkey.Do(ctx, command).Error()
	platform.RecordSpanError(span, err)
	span.SetAttributes(platform.SpanErrorStatus(err))
	return err
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
	count, err := utils.UpsertBasicPlayersCount(ctx, tx, ingest.Players, globalClansDomainName)
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
	mu             sync.RWMutex
	rows           map[string]models.BasicClanRow
	activeCursor   string
	inactiveCursor string
}

func newMemoryGlobalClanStore() *memoryGlobalClanStore {
	return &memoryGlobalClanStore{rows: make(map[string]models.BasicClanRow)}
}

func (s *memoryGlobalClanStore) Close() error { return nil }

func (s *memoryGlobalClanStore) NextTargetBatch(_ context.Context, bucket string, limit int) (globalClanTargetBatch, error) {
	s.mu.RLock()
	active := bucket != "inactive"
	cursor := s.activeCursor
	if !active {
		cursor = s.inactiveCursor
	}
	tags := memoryClanBucket(s.rows, cursor, limit, active)
	nextCursor := nextMemoryCursor(tags, limit)
	if len(tags) > limit {
		tags = tags[:limit]
	}
	s.mu.RUnlock()
	batch := globalClanTargetBatch{Bucket: bucket, Tags: tags}
	if active {
		batch.ActiveCursor = nextCursor
		batch.ActiveCount = len(tags)
	} else {
		batch.InactiveCursor = nextCursor
		batch.InactiveCount = len(tags)
	}
	return batch, nil
}

func (s *memoryGlobalClanStore) CommitTargetBatch(_ context.Context, batch globalClanTargetBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if batch.Bucket == "inactive" {
		s.inactiveCursor = batch.InactiveCursor
	} else {
		s.activeCursor = batch.ActiveCursor
	}
	return nil
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

func memoryClanBucket(rows map[string]models.BasicClanRow, cursor string, limit int, active bool) []string {
	if limit <= 0 {
		return nil
	}
	tags := make([]string, 0, limit+1)
	cutoff := time.Now().UTC().Add(-7 * 24 * time.Hour)
	for tag, row := range rows {
		if tag <= cursor {
			continue
		}
		if active != isActiveGlobalClan(row, cutoff) {
			continue
		}
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	if len(tags) > limit+1 {
		return tags[:limit+1]
	}
	return tags
}

func isActiveGlobalClan(row models.BasicClanRow, cutoff time.Time) bool {
	return row.MemberCount > 5 && row.LastActive != nil && !row.LastActive.Before(cutoff)
}

func nextMemoryCursor(tags []string, limit int) string {
	if limit <= 0 || len(tags) == 0 || len(tags) <= limit {
		return ""
	}
	return tags[limit-1]
}
