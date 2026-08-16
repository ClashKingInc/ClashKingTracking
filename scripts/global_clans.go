package scripts

import (
	"context"
	"database/sql"
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
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	globalClansDomainName = "globalclans"
	unrankedWarLeagueID   = 48000000

	globalClanDiffBatchSize           = 100
	globalClanAsyncWriteWorkers       = 1
	globalClanAsyncWriteBatchSize     = 4
	globalClanAsyncWriteQueueSize     = 16
	globalClanAsyncWriteFlushInterval = 250 * time.Millisecond
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
	NextTargetPage(context.Context, string, string, int) (globalClanTargetPage, error)
	CountTargetTags(context.Context, string) (int, error)
	Load(context.Context, []string) (map[string]models.BasicClanRow, error)
	Store(context.Context, models.GlobalClanIngest) (globalClanStoreResult, error)
	Close() error
}

type globalClanTargetPage struct {
	Tags       []string
	NextCursor string
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

type globalClanAsyncWriter struct {
	app   *platform.App
	store globalClanStore
	jobs  chan globalClanWriteJob
}

type globalClanWriteJob struct {
	Group     string
	Snapshots []globalClanSnapshot
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
	// active clans. Targets are streamed from SQL pages instead of loading every
	// inactive clan into memory.
	groups := globalClanGroups(app.Config)

	writer := newGlobalClanAsyncWriter(app, store)
	writerCtx, stopWriter := context.WithCancel(ctx)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		writer.run(writerCtx)
	}()
	defer func() {
		stopWriter()
		<-writerDone
	}()
	return d.runGroups(ctx, app, groups, writer)
}

func globalClanGroups(cfg platform.Config) []globalClanGroup {
	return []globalClanGroup{
		{
			Name:              "priority",
			Bucket:            "active",
			RequestsPerSecond: cfg.GlobalClanPriorityRequestsPerSecond,
			MaxInFlight:       platform.RequestConcurrency(cfg.GlobalClanPriorityRequestsPerSecond),
		},
		{
			Name:              "non_priority",
			Bucket:            "inactive",
			RequestsPerSecond: cfg.GlobalClanNonPriorityRequestsPerSecond,
			MaxInFlight:       platform.RequestConcurrency(cfg.GlobalClanNonPriorityRequestsPerSecond),
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
		return errors.New("TIMESCALE_* connection variables are required when globalclans is enabled")
	}
	if !cfg.DryRun && !cfg.MockDB && cfg.ValkeyAddr == "" {
		return errors.New("valkey_addr is required for globalclans event publishing")
	}
	return nil
}

func newGlobalClanAsyncWriter(
	app *platform.App,
	store globalClanStore,
) *globalClanAsyncWriter {
	return &globalClanAsyncWriter{
		app:   app,
		store: store,
		jobs:  make(chan globalClanWriteJob, globalClanAsyncWriteQueueSize),
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
	var wg sync.WaitGroup
	for i := 0; i < globalClanAsyncWriteWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.runWorker(ctx)
		}()
	}
	<-ctx.Done()
	wg.Wait()
}

func (w *globalClanAsyncWriter) runWorker(ctx context.Context) {
	timer := time.NewTimer(globalClanAsyncWriteFlushInterval)
	defer timer.Stop()
	batch := make([]globalClanWriteJob, 0, globalClanAsyncWriteBatchSize)
	flush := func(flushCtx context.Context) bool {
		if len(batch) == 0 {
			w.recordQueueDepth(0)
			return true
		}
		if err := w.writeBatch(flushCtx, batch); err != nil {
			w.app.Logger.Error("global clan async store batch failed", "jobs", len(batch), "err", err)
			w.app.Stats.SetReady(globalClansDomainName, false, err.Error())
			w.recordQueueDepth(len(batch))
			return false
		}
		batch = batch[:0]
		w.recordQueueDepth(0)
		return true
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
	flushBeforeExit := func(parent context.Context) {
		flushCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), 10*time.Second)
		defer cancel()
		for {
		drainQueue:
			for len(batch) < globalClanAsyncWriteBatchSize {
				select {
				case job := <-w.jobs:
					batch = append(batch, job)
				default:
					break drainQueue
				}
			}
			if len(batch) == 0 {
				return
			}
			if flush(flushCtx) {
				continue
			}
			retry := time.NewTimer(time.Second)
			select {
			case <-flushCtx.Done():
				retry.Stop()
				return
			case <-retry.C:
			}
		}
	}
	for {
		if len(batch) >= globalClanAsyncWriteBatchSize {
			if flush(ctx) {
				resetTimer()
				continue
			}
			select {
			case <-ctx.Done():
				flushBeforeExit(ctx)
				return
			case <-time.After(time.Second):
			}
			continue
		}
		select {
		case <-ctx.Done():
			flushBeforeExit(ctx)
			return
		case job := <-w.jobs:
			batch = append(batch, job)
			w.recordQueueDepth(len(batch))
		case <-timer.C:
			if flush(ctx) {
				timer.Reset(globalClanAsyncWriteFlushInterval)
			} else {
				timer.Reset(time.Second)
			}
		}
	}
}

func (w *globalClanAsyncWriter) recordQueueDepth(inBatch int) {
	if w == nil || w.app == nil || w.jobs == nil {
		return
	}
	w.app.Stats.SetQueueDepth(globalClansDomainName, len(w.jobs)+inBatch)
}

func (d *globalClansDomain) runGroups(
	ctx context.Context,
	app *platform.App,
	groups []globalClanGroup,
	writer *globalClanAsyncWriter,
) error {
	groupCtx, cancelGroups := context.WithCancel(ctx)
	defer cancelGroups()
	errCh := make(chan error, len(groups))
	for _, group := range groups {
		group := group
		go func() {
			errCh <- d.runGroup(groupCtx, app, group, writer)
		}()
	}
	firstErr := <-errCh
	cancelGroups()
	for range len(groups) - 1 {
		<-errCh
	}
	return firstErr
}

func (d *globalClansDomain) runGroup(
	ctx context.Context,
	app *platform.App,
	group globalClanGroup,
	writer *globalClanAsyncWriter,
) error {
	statsName := trackingProgressName(globalClansDomainName, group.Name)
	if count, err := d.store.CountTargetTags(ctx, group.Bucket); err == nil {
		app.Stats.SetTrackingTargets(statsName, count)
	} else {
		app.Logger.Error("global clan target count failed", "group", group.Name, "err", err)
	}
	limiter, err := newTrackingLimiter(group.RequestsPerSecond)
	if err != nil {
		return err
	}
	pageSize := group.RequestsPerSecond * app.Config.TargetPageMultiplier
	cursor := ""
	for {
		page, err := d.store.NextTargetPage(ctx, group.Bucket, cursor, pageSize)
		if err != nil {
			return err
		}
		if len(page.Tags) == 0 {
			cursor = ""
			if err := sleepOrDone(ctx, time.Second); err != nil {
				return err
			}
			continue
		}

		snapshots := make([]globalClanSnapshot, 0, len(page.Tags))
		var snapshotsMu sync.Mutex
		if err := runBounded(ctx, group.MaxInFlight, page.Tags, func(workerCtx context.Context, tag string) error {
			clan, err := retryLimitedClashFetch(workerCtx, app, limiter, func(fetchCtx context.Context) (*clashy.Clan, error) {
				return fetchGlobalClan(fetchCtx, app, group.Name, tag)
			})
			app.Stats.RecordTrackedTarget(statsName)
			if err != nil {
				app.Logger.Error("global clan fetch failed", "tag", tag, "err", err)
				app.Stats.SetReady(globalClansDomainName, false, err.Error())
				return err
			}
			if clan == nil || (clan.Tag == "" && clan.Name == "") {
				return fmt.Errorf("empty clan payload")
			}
			current := globalClanSnapshot{
				Clan:      *clan,
				Row:       basicClanRow(*clan),
				FetchedAt: time.Now().UTC(),
			}
			snapshotsMu.Lock()
			snapshots = append(snapshots, current)
			snapshotsMu.Unlock()
			return nil
		}); err != nil {
			app.Stats.SetReady(globalClansDomainName, false, err.Error())
			return err
		}
		for start := 0; start < len(snapshots); start += globalClanDiffBatchSize {
			end := start + globalClanDiffBatchSize
			if end > len(snapshots) {
				end = len(snapshots)
			}
			if err := writer.enqueue(ctx, globalClanWriteJob{
				Group:     group.Name,
				Snapshots: append([]globalClanSnapshot(nil), snapshots[start:end]...),
			}); err != nil {
				return err
			}
		}
		cursor = page.NextCursor
	}
}

func buildGlobalClanIngest(current globalClanSnapshot, previous *globalClanSnapshot) models.GlobalClanIngest {
	if current.Clan.MemberCount <= 0 {
		return models.GlobalClanIngest{DeletedClanTags: []string{current.Clan.Tag}}
	}

	var previousRow models.BasicClanRow
	if previous != nil {
		previousRow = previous.Row
	}
	ingest := models.GlobalClanIngest{
		Clans:       []models.BasicClanRow{current.Row},
		Players:     basicPlayerRows(current.Clan.Tag, current.Clan.Members),
		ClanRecords: clanRecordRows(current.Clan, previousRow, current.FetchedAt),
	}
	if previous != nil && !isGlobalClanFirstHydration(previous.Row, current.Clan) {
		// Joins are the only member movement that marks a clan active. Leave-only
		// changes are still persisted, but they should not promote a clan into
		// the priority polling bucket.
		ingest.JoinLeaves = joinLeaveRows(previous.Row, current.Clan, current.FetchedAt)
		if hasJoin(ingest.JoinLeaves) {
			ingest.ActiveClanTags = []string{current.Clan.Tag}
		}
		ingest.ClanChanges = clanChangeRows(previous.Row, current.Row, current.FetchedAt)
	}
	return ingest
}

func isGlobalClanFirstHydration(previous models.BasicClanRow, current clashy.Clan) bool {
	return previous.MemberCount <= 0 && current.MemberCount > 0
}

func fetchGlobalClan(ctx context.Context, app *platform.App, group, tag string) (*clashy.Clan, error) {
	start := time.Now()
	clan, err := app.Clash.GetClan(ctx, tag)
	app.Stats.RecordRequest(globalClansDomainName, time.Since(start), err)
	if err != nil || clan == nil {
		return clan, err
	}
	return clan, nil
}

func (w *globalClanAsyncWriter) writeBatch(ctx context.Context, jobs []globalClanWriteJob) error {
	if len(jobs) == 0 {
		return nil
	}
	group := jobs[0].Group
	snapshotsByTag := make(map[string]globalClanSnapshot)
	var ingest models.GlobalClanIngest
	for _, job := range jobs {
		if job.Group != group {
			group = "mixed"
		}
		for _, snapshot := range job.Snapshots {
			if snapshot.Row.Tag == "" {
				continue
			}
			previous, ok := snapshotsByTag[snapshot.Row.Tag]
			if !ok || snapshot.FetchedAt.After(previous.FetchedAt) {
				snapshotsByTag[snapshot.Row.Tag] = snapshot
			}
		}
	}
	tags := make([]string, 0, len(snapshotsByTag))
	for tag := range snapshotsByTag {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	previousRows, err := w.store.Load(ctx, tags)
	if err != nil {
		return err
	}
	for _, tag := range tags {
		snapshot := snapshotsByTag[tag]
		var previous *globalClanSnapshot
		if row, ok := previousRows[tag]; ok {
			previous = &globalClanSnapshot{Row: row}
		}
		ingest = mergeGlobalClanIngest(ingest, buildGlobalClanIngest(snapshot, previous))
	}
	if globalClanIngestWriteCount(ingest) == 0 {
		return nil
	}
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
	w.app.Stats.SetReady(globalClansDomainName, true, "")
	return nil
}

func durationMillis(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func globalClanIngestWriteCount(ingest models.GlobalClanIngest) int {
	return len(ingest.Clans) + len(ingest.Players) + len(ingest.ClanRecords) + len(ingest.ActiveClanTags) +
		len(ingest.DeletedClanTags) + len(ingest.ClanChanges) + len(ingest.JoinLeaves)
}

func basicClanRow(clan clashy.Clan) models.BasicClanRow {
	row := models.BasicClanRow{
		Tag:               clan.Tag,
		Name:              clan.Name,
		Description:       clan.Description,
		ClanLevel:         clan.Level,
		PublicWarLog:      clan.PublicWarLog,
		WarWins:           clan.WarWins,
		WarWinStreak:      clan.WarWinStreak,
		ClanPoints:        clan.Points,
		BuilderBasePoints: clan.BuilderBasePoints,
		CapitalPoints:     clan.CapitalPoints,
		MemberCount:       clan.MemberCount,
		BadgeURL:          badgeToken(clan.Badge),
		Members:           memberSnapshot(clan.Members),
		TroopsDonated:     totalDonated(clan.Members),
		TroopsReceived:    totalReceived(clan.Members),
		CWLLeagueID:       unrankedWarLeagueID,
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

func badgeToken(badge clashy.Badge) string {
	for _, raw := range []string{badge.Large, badge.Medium, badge.Small} {
		raw = strings.TrimSpace(raw)
		raw = strings.TrimSuffix(raw, ".png")
		if idx := strings.LastIndex(raw, "/"); idx >= 0 {
			raw = raw[idx+1:]
		}
		if raw != "" {
			return raw
		}
	}
	return ""
}

func joinLeaveRows(previous models.BasicClanRow, current clashy.Clan, now time.Time) []models.JoinLeaveRow {
	currentMembers := clanMembersByTag(current.Members)
	currentTags := stringSet(memberTagsFromSnapshot(memberSnapshot(current.Members)))
	previousTags := stringSet(memberTagsFromSnapshot(previous.Members))
	previousNames := memberNamesByTag(previous.Members)
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
			PlayerName:    member.Name,
			TownHallLevel: member.TownHall,
		})
	}
	for tag := range previousTags {
		if _, ok := currentTags[tag]; ok {
			continue
		}
		out = append(out, models.JoinLeaveRow{
			EventTime:  now,
			EventType:  "leave",
			ClanTag:    current.Tag,
			PlayerTag:  tag,
			PlayerName: previousNames[tag],
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
	left.ClanRecords = append(left.ClanRecords, right.ClanRecords...)
	left.ActiveClanTags = append(left.ActiveClanTags, right.ActiveClanTags...)
	left.DeletedClanTags = append(left.DeletedClanTags, right.DeletedClanTags...)
	left.ClanChanges = append(left.ClanChanges, right.ClanChanges...)
	left.JoinLeaves = append(left.JoinLeaves, right.JoinLeaves...)
	return left
}

func clanRecordRows(clan clashy.Clan, previous models.BasicClanRow, now time.Time) []models.ClanRecordRow {
	if clan.Tag == "" {
		return nil
	}
	row := models.ClanRecordRow{Tag: clan.Tag}
	if clan.Points > previous.RecordClanPoints {
		row.ClanPoints = clan.Points
		row.ClanPointsAt = &now
	}
	if clan.WarWinStreak > previous.RecordWarWinStreak {
		row.WarWinStreak = clan.WarWinStreak
		row.WarWinStreakAt = &now
	}
	if row.ClanPoints == 0 && row.WarWinStreak == 0 {
		return nil
	}
	return []models.ClanRecordRow{row}
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
			Tag:          member.Tag,
			Name:         member.Name,
			LeagueID:     member.LeagueTier.ID,
			ClanTag:      clanTag,
			ClanTagKnown: true,
			TownHall:     member.TownHall,
			Trophies:     member.Trophies,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tag < out[j].Tag })
	return out
}

func memberSnapshot(members []clashy.ClanMember) []models.BasicClanMember {
	out := make([]models.BasicClanMember, 0, len(members))
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		if member.Tag == "" {
			continue
		}
		if _, ok := seen[member.Tag]; ok {
			continue
		}
		seen[member.Tag] = struct{}{}
		out = append(out, models.BasicClanMember{Tag: member.Tag, Name: member.Name, TownHall: member.TownHall})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Tag < out[j].Tag })
	return out
}

func memberTagsFromSnapshot(members []models.BasicClanMember) []string {
	out := make([]string, 0, len(members))
	for _, member := range members {
		if member.Tag != "" {
			out = append(out, member.Tag)
		}
	}
	return out
}

func memberNamesByTag(members []models.BasicClanMember) map[string]string {
	out := make(map[string]string, len(members))
	for _, member := range members {
		if member.Tag != "" {
			out[member.Tag] = member.Name
		}
	}
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

func (s *timescaleGlobalClanStore) NextTargetPage(ctx context.Context, bucket string, cursor string, limit int) (globalClanTargetPage, error) {
	if limit <= 0 {
		limit = 1000
	}
	tags, nextCursor, err := s.scanClanBucket(ctx, bucket, cursor, limit)
	if err != nil {
		return globalClanTargetPage{}, err
	}
	return globalClanTargetPage{Tags: tags, NextCursor: nextCursor}, nil
}

func (s *timescaleGlobalClanStore) CountTargetTags(ctx context.Context, bucket string) (int, error) {
	query := `
		SELECT count(*)
		FROM basic_clan
		WHERE member_count > 5
		  AND last_active >= now() - interval '7 days'
	`
	if bucket == "inactive" {
		query = `
			SELECT count(*)
			FROM basic_clan
			WHERE member_count > 0
			  AND (
			    member_count <= 5
			    OR last_active IS NULL
			    OR last_active < now() - interval '7 days'
			  )
		`
	}
	var count int
	if err := s.pool.QueryRow(ctx, query).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
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
	rows, err := s.pool.Query(ctx, `
		SELECT
			c.tag, c.name, c.description, c.clan_level, c.location_id, c.cwl_league_id, c.capital_league_id,
			c.public_war_log, c.war_wins, c.war_win_streak, c.clan_points,
			c.builder_base_points, c.capital_points,
			COALESCE(r.war_win_streak, 0), r.war_win_streak_at,
			COALESCE(r.clan_points, 0), r.clan_points_at, c.member_count, c.badge_token, c.troops_donated,
			c.troops_received, c.members, c.last_active
		FROM basic_clan c
		LEFT JOIN clan_records r ON r.tag = c.tag
		WHERE c.tag = ANY($1)
	`, tags)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var row models.BasicClanRow
		var locationID sql.NullInt64
		var cwlLeagueID int
		var capitalLeagueID sql.NullInt64
		var lastActive sql.NullTime
		var recordWarWinStreakAt sql.NullTime
		var recordClanPointsAt sql.NullTime
		var membersPayload []byte
		if err := rows.Scan(
			&row.Tag, &row.Name, &row.Description, &row.ClanLevel, &locationID, &cwlLeagueID, &capitalLeagueID,
			&row.PublicWarLog, &row.WarWins, &row.WarWinStreak, &row.ClanPoints,
			&row.BuilderBasePoints, &row.CapitalPoints,
			&row.RecordWarWinStreak, &recordWarWinStreakAt,
			&row.RecordClanPoints, &recordClanPointsAt, &row.MemberCount, &row.BadgeURL, &row.TroopsDonated,
			&row.TroopsReceived, &membersPayload, &lastActive,
		); err != nil {
			return nil, err
		}
		if len(membersPayload) > 0 {
			if err := json.Unmarshal(membersPayload, &row.Members); err != nil {
				return nil, err
			}
		}
		row.LocationID = optionalIntFromSQL(locationID)
		row.CWLLeagueID = cwlLeagueID
		row.CapitalLeagueID = optionalIntFromSQL(capitalLeagueID)
		if recordWarWinStreakAt.Valid {
			row.RecordWarWinStreakAt = &recordWarWinStreakAt.Time
		}
		if recordClanPointsAt.Valid {
			row.RecordClanPointsAt = &recordClanPointsAt.Time
		}
		if lastActive.Valid {
			row.LastActive = &lastActive.Time
		}
		out[row.Tag] = row
	}
	return out, rows.Err()
}

func (s *timescaleGlobalClanStore) Store(ctx context.Context, ingest models.GlobalClanIngest) (globalClanStoreResult, error) {
	requestedWrites := globalClanIngestWriteCount(ingest)
	if requestedWrites == 0 {
		return globalClanStoreResult{}, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return globalClanStoreResult{}, err
	}
	defer tx.Rollback(ctx)

	result := globalClanStoreResult{}
	count, err := upsertGlobalClanBasicPlayers(ctx, tx, ingest.Players)
	if err != nil {
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	count, err = insertClanChanges(ctx, tx, ingest.ClanChanges)
	if err != nil {
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	count, err = insertJoinLeaves(ctx, tx, ingest.JoinLeaves)
	if err != nil {
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	count, err = deleteBasicClans(ctx, tx, ingest.DeletedClanTags)
	if err != nil {
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	changedClans, count, err := upsertBasicClans(ctx, tx, ingest.Clans)
	if err != nil {
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	result.EventClans = changedClans
	count, err = upsertClanRecords(ctx, tx, ingest.ClanRecords)
	if err != nil {
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	count, err = updateActiveClans(ctx, tx, ingest.ActiveClanTags)
	if err != nil {
		return globalClanStoreResult{}, err
	}
	result.WriteCount += count
	if err := tx.Commit(ctx); err != nil {
		return globalClanStoreResult{}, err
	}
	return result, nil
}

const upsertBasicClanSQL = `
	INSERT INTO basic_clan (
		tag, name, description, clan_level, location_id, cwl_league_id, capital_league_id,
		public_war_log, war_wins, war_win_streak, clan_points, builder_base_points, capital_points,
		member_count, badge_token, troops_donated, troops_received, members
	)
	VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18::jsonb
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
		war_win_streak = EXCLUDED.war_win_streak,
		clan_points = EXCLUDED.clan_points,
		builder_base_points = EXCLUDED.builder_base_points,
		capital_points = EXCLUDED.capital_points,
		member_count = EXCLUDED.member_count,
		badge_token = EXCLUDED.badge_token,
		troops_donated = EXCLUDED.troops_donated,
		troops_received = EXCLUDED.troops_received,
		members = EXCLUDED.members
	WHERE
		basic_clan.name IS DISTINCT FROM EXCLUDED.name OR
		basic_clan.description IS DISTINCT FROM EXCLUDED.description OR
		basic_clan.clan_level IS DISTINCT FROM EXCLUDED.clan_level OR
		basic_clan.location_id IS DISTINCT FROM EXCLUDED.location_id OR
		basic_clan.cwl_league_id IS DISTINCT FROM EXCLUDED.cwl_league_id OR
		basic_clan.capital_league_id IS DISTINCT FROM EXCLUDED.capital_league_id OR
		basic_clan.public_war_log IS DISTINCT FROM EXCLUDED.public_war_log OR
		basic_clan.war_wins IS DISTINCT FROM EXCLUDED.war_wins OR
		basic_clan.war_win_streak IS DISTINCT FROM EXCLUDED.war_win_streak OR
		basic_clan.clan_points IS DISTINCT FROM EXCLUDED.clan_points OR
		basic_clan.builder_base_points IS DISTINCT FROM EXCLUDED.builder_base_points OR
		basic_clan.capital_points IS DISTINCT FROM EXCLUDED.capital_points OR
		basic_clan.member_count IS DISTINCT FROM EXCLUDED.member_count OR
		basic_clan.badge_token IS DISTINCT FROM EXCLUDED.badge_token OR
		basic_clan.troops_donated IS DISTINCT FROM EXCLUDED.troops_donated OR
		basic_clan.troops_received IS DISTINCT FROM EXCLUDED.troops_received OR
		basic_clan.members IS DISTINCT FROM EXCLUDED.members
`

func upsertBasicClans(ctx context.Context, tx pgx.Tx, clans []models.BasicClanRow) ([]models.BasicClanRow, int, error) {
	if len(clans) == 0 {
		return nil, 0, nil
	}
	batch := &pgx.Batch{}
	for _, clan := range clans {
		membersPayload, err := json.Marshal(clan.Members)
		if err != nil {
			return nil, 0, err
		}
		batch.Queue(upsertBasicClanSQL,
			clan.Tag, clan.Name, clan.Description, clan.ClanLevel,
			optionalIntValue(clan.LocationID),
			clan.CWLLeagueID,
			optionalIntValue(clan.CapitalLeagueID),
			clan.PublicWarLog, clan.WarWins, clan.WarWinStreak, clan.ClanPoints,
			clan.BuilderBasePoints, clan.CapitalPoints, clan.MemberCount, clan.BadgeURL, clan.TroopsDonated,
			clan.TroopsReceived, string(membersPayload),
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

func upsertClanRecords(ctx context.Context, tx pgx.Tx, rows []models.ClanRecordRow) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.Tag == "" || (row.ClanPoints <= 0 && row.WarWinStreak <= 0) {
			continue
		}
		batch.Queue(`
			INSERT INTO clan_records (tag, clan_points, clan_points_at, war_win_streak, war_win_streak_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (tag) DO UPDATE SET
				clan_points = CASE
					WHEN EXCLUDED.clan_points > clan_records.clan_points THEN EXCLUDED.clan_points
					ELSE clan_records.clan_points
				END,
				clan_points_at = CASE
					WHEN EXCLUDED.clan_points > clan_records.clan_points THEN EXCLUDED.clan_points_at
					ELSE clan_records.clan_points_at
				END,
				war_win_streak = CASE
					WHEN EXCLUDED.war_win_streak > clan_records.war_win_streak THEN EXCLUDED.war_win_streak
					ELSE clan_records.war_win_streak
				END,
				war_win_streak_at = CASE
					WHEN EXCLUDED.war_win_streak > clan_records.war_win_streak THEN EXCLUDED.war_win_streak_at
					ELSE clan_records.war_win_streak_at
				END
			WHERE
				EXCLUDED.clan_points > clan_records.clan_points OR
				EXCLUDED.war_win_streak > clan_records.war_win_streak
			`, row.Tag, row.ClanPoints, row.ClanPointsAt, row.WarWinStreak, row.WarWinStreakAt)
	}
	return utils.SendBatchCount(ctx, tx, batch)
}

func upsertGlobalClanBasicPlayers(ctx context.Context, tx pgx.Tx, players []models.BasicPlayerRow) (int, error) {
	if len(players) == 0 {
		return 0, nil
	}
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE global_clan_basic_player_stage (
			tag text NOT NULL,
			name text NOT NULL,
			league_id integer NOT NULL,
			clan_tag text NOT NULL,
			townhall_level integer NOT NULL,
			trophies integer NOT NULL
		) ON COMMIT DROP
	`); err != nil {
		return 0, err
	}
	_, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"global_clan_basic_player_stage"},
		[]string{"tag", "name", "league_id", "clan_tag", "townhall_level", "trophies"},
		pgx.CopyFromSlice(len(players), func(i int) ([]any, error) {
			player := players[i]
			return []any{
				player.Tag,
				player.Name,
				player.LeagueID,
				player.ClanTag,
				player.TownHall,
				player.Trophies,
			}, nil
		}),
	)
	if err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO basic_player (
			tag, name, league_id, clan_tag, townhall_level, trophies
		)
		SELECT DISTINCT ON (tag)
			tag,
			name,
			NULLIF(league_id, 0),
			clan_tag,
			townhall_level,
			trophies
		FROM global_clan_basic_player_stage
		WHERE tag <> ''
		  AND name <> ''
		  AND townhall_level > 0
		ORDER BY tag
		ON CONFLICT (tag) DO UPDATE SET
			name = EXCLUDED.name,
			league_id = COALESCE(EXCLUDED.league_id, basic_player.league_id),
			clan_tag = EXCLUDED.clan_tag,
			townhall_level = EXCLUDED.townhall_level,
			trophies = CASE WHEN EXCLUDED.trophies > 0 THEN EXCLUDED.trophies ELSE basic_player.trophies END
		WHERE
			basic_player.name IS DISTINCT FROM EXCLUDED.name OR
			basic_player.league_id IS DISTINCT FROM COALESCE(EXCLUDED.league_id, basic_player.league_id) OR
			basic_player.clan_tag IS DISTINCT FROM EXCLUDED.clan_tag OR
			basic_player.townhall_level IS DISTINCT FROM EXCLUDED.townhall_level OR
			(EXCLUDED.trophies > 0 AND basic_player.trophies IS DISTINCT FROM EXCLUDED.trophies)
	`)
	return int(tag.RowsAffected()), err
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
				"time", "type", clan_tag, player_tag, townhall_level, player_name
			)
			VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''))
			`, row.EventTime, row.EventType, row.ClanTag, row.PlayerTag, row.TownHallLevel, row.PlayerName)
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

func (s *memoryGlobalClanStore) NextTargetPage(ctx context.Context, bucket string, cursor string, limit int) (globalClanTargetPage, error) {
	tags, err := s.ListTargetTags(ctx, bucket, limit)
	if err != nil {
		return globalClanTargetPage{}, err
	}
	start := 0
	if cursor != "" {
		start = sort.SearchStrings(tags, cursor)
		for start < len(tags) && tags[start] <= cursor {
			start++
		}
	}
	if start >= len(tags) {
		return globalClanTargetPage{}, nil
	}
	end := start + limit
	if limit <= 0 || end > len(tags) {
		end = len(tags)
	}
	page := append([]string(nil), tags[start:end]...)
	nextCursor := ""
	if end < len(tags) {
		nextCursor = page[len(page)-1]
	}
	return globalClanTargetPage{Tags: page, NextCursor: nextCursor}, nil
}

func (s *memoryGlobalClanStore) CountTargetTags(ctx context.Context, bucket string) (int, error) {
	tags, err := s.ListTargetTags(ctx, bucket, 0)
	if err != nil {
		return 0, err
	}
	return len(tags), nil
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
			row.RecordWarWinStreak = existing.RecordWarWinStreak
			row.RecordWarWinStreakAt = existing.RecordWarWinStreakAt
			row.RecordClanPoints = existing.RecordClanPoints
			row.RecordClanPointsAt = existing.RecordClanPointsAt
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
	for _, record := range ingest.ClanRecords {
		row := s.rows[record.Tag]
		if record.ClanPoints > row.RecordClanPoints {
			row.RecordClanPoints = record.ClanPoints
			row.RecordClanPointsAt = record.ClanPointsAt
		}
		if record.WarWinStreak > row.RecordWarWinStreak {
			row.RecordWarWinStreak = record.WarWinStreak
			row.RecordWarWinStreakAt = record.WarWinStreakAt
		}
		s.rows[record.Tag] = row
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
	result.WriteCount += len(ingest.ClanRecords)
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
		left.WarWinStreak == right.WarWinStreak &&
		left.ClanPoints == right.ClanPoints &&
		left.MemberCount == right.MemberCount &&
		left.BadgeURL == right.BadgeURL &&
		left.TroopsDonated == right.TroopsDonated &&
		left.TroopsReceived == right.TroopsReceived &&
		memberSlicesEqual(left.Members, right.Members)
}

func memberSlicesEqual(left, right []models.BasicClanMember) bool {
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
