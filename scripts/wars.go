package scripts

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	clashevents "github.com/clashkinginc/clashy.go/events"
	clashstores "github.com/clashkinginc/clashy.go/stores"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	warsDomainName   = "wars"
	warsRateLimit    = 950
	cwlSyncFrequency = 3 * time.Minute
)

type warsDomain struct {
	mu        sync.Mutex
	inWar     map[string]time.Time
	scheduled map[string]struct{}
}

func NewWarsDomain() platform.Domain {
	return &warsDomain{
		inWar:     make(map[string]time.Time),
		scheduled: make(map[string]struct{}),
	}
}

func (d *warsDomain) Name() string { return warsDomainName }

func (d *warsDomain) Run(ctx context.Context, app *platform.App) error {
	if app.Config.RunOnce {
		return d.runOnce(ctx, app)
	}

	go d.runCWLLoop(ctx, app)

	tracker := clashevents.NewTracker(
		"war",
		app.Clash,
		func(ctx context.Context, tag string) (*clashy.ClanWar, int, error) {
			start := time.Now()
			war, err := app.Clash.GetCurrentWar(ctx, tag)
			app.Stats.RecordRequest(warsDomainName, time.Since(start), err)
			retry := 0
			if war != nil {
				retry = war.RetryAfter()
			}
			return war, retry, err
		},
		clashstores.NewMemoryStore(),
	)

	tracker.
		Group(
			"wars",
			clashevents.Interval(0),
			clashevents.RateLimit(warsRateLimit),
			clashevents.Source(activeWarSource{app: app, domain: d}),
		).
		On(clashevents.EveryPoll("sync", func(ctx context.Context, change clashevents.Change[clashy.ClanWar]) error {
			return d.handleWarChange(ctx, app, change)
		})).
		OnError(func(ctx context.Context, ec clashevents.ErrorChange[clashy.ClanWar]) *clashevents.ErrorDecision {
			app.Stats.SetReady(warsDomainName, false, ec.Err.Error())
			return &clashevents.ErrorDecision{Action: clashevents.ErrorActionSkip}
		})

	if err := tracker.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func (d *warsDomain) runOnce(ctx context.Context, app *platform.App) error {
	tags, err := d.targets(ctx, app)
	if err != nil {
		return err
	}
	for _, tag := range tags {
		if d.skipTag(tag) {
			continue
		}
		start := time.Now()
		war, err := app.Clash.GetCurrentWar(ctx, tag)
		app.Stats.RecordRequest(warsDomainName, time.Since(start), err)
		if err != nil || war == nil {
			continue
		}
		if err := d.handleWarChange(ctx, app, clashevents.Change[clashy.ClanWar]{Tag: tag, Current: war}); err != nil {
			return err
		}
	}
	if utils.IsCWL(time.Now()) {
		return d.syncCWLGroups(ctx, app, tags)
	}
	return nil
}

type activeWarSource struct {
	app    *platform.App
	domain *warsDomain
}

func (s activeWarSource) ForEach(ctx context.Context, fn func(tag string) error) error {
	tags, err := s.domain.targets(ctx, s.app)
	if err != nil {
		return err
	}
	for _, tag := range tags {
		if s.domain.skipTag(tag) {
			continue
		}
		if err := fn(tag); err != nil {
			return err
		}
	}
	return nil
}

func (d *warsDomain) targets(ctx context.Context, app *platform.App) ([]string, error) {
	active, err := app.Store.Stats().Collection("all_clans").DistinctStrings(ctx, "tag", bson.M{"active": true})
	if err != nil {
		return nil, err
	}
	botClans, _ := app.Store.Static().Collection("clans_db").DistinctStrings(ctx, "tag", bson.M{})
	return mergeStrings(active, botClans), nil
}

func (d *warsDomain) handleWarChange(ctx context.Context, app *platform.App, change clashevents.Change[clashy.ClanWar]) error {
	if change.Current == nil {
		return nil
	}
	current := *change.Current
	if current.PreparationStartTime == nil || current.EndTime == nil || current.EndTime.Time.Before(time.Now().UTC()) {
		return nil
	}
	if current.Clan == nil || current.Opponent == nil {
		return nil
	}
	if change.Previous != nil && equalJSON(*change.Previous, current) {
		return nil
	}

	prepAt := current.PreparationStartTime.Time
	endAt := current.EndTime.Time
	warID := models.ComputeWarID(current.Clan.Tag, current.Opponent.Tag, prepAt)
	d.trackInWar(current.Clan.Tag, endAt)
	d.trackInWar(current.Opponent.Tag, endAt)

	if err := app.Store.Stats().Collection("clan_wars").UpdateOne(ctx, bson.M{"war_id": warID}, bson.M{
		"$set": bson.M{
			"war_id":  warID,
			"clans":   []string{current.Clan.Tag, current.Opponent.Tag},
			"endTime": endAt.Unix(),
		},
	}, true); err != nil {
		return err
	}
	if err := app.Store.Stats().Collection("war_timer").UpdateOne(ctx, bson.M{"_id": current.Clan.Tag}, bson.M{
		"$set": bson.M{"clans": []string{current.Clan.Tag, current.Opponent.Tag}, "time": endAt},
	}, true); err != nil {
		return err
	}

	d.scheduleStore(app, warID, change.Tag, endAt, current)
	app.Stats.RecordWrite(warsDomainName, 2)
	app.Stats.SetReady(warsDomainName, true, "")
	app.Stats.SetQueueDepth(warsDomainName, app.Scheduler.Pending())
	return nil
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
			tags, err := d.targets(ctx, app)
			if err == nil {
				_ = d.syncCWLGroups(ctx, app, tags)
			}
		}

		timer.Reset(cwlSyncFrequency)
	}
}

func (d *warsDomain) syncCWLGroups(ctx context.Context, app *platform.App, tags []string) error {
	season := utils.CurrentSeason(time.Now())
	seen := make(map[string]struct{})
	for _, tag := range tags {
		start := time.Now()
		group, err := app.Clash.GetLeagueGroup(ctx, tag)
		app.Stats.RecordRequest(warsDomainName, time.Since(start), err)
		if err != nil || group == nil || group.Season != season {
			continue
		}
		keys := make([]string, 0, len(group.Clans))
		for _, clan := range group.Clans {
			if clan.Tag != "" {
				keys = append(keys, strings.TrimPrefix(clan.Tag, "#"))
			}
		}
		if len(keys) == 0 {
			continue
		}
		sort.Strings(keys)
		cwlID := season + "-" + strings.Join(keys, "-")
		if _, ok := seen[cwlID]; ok {
			continue
		}
		seen[cwlID] = struct{}{}
		if err := app.Store.Stats().Collection("cwl_group").UpdateOne(ctx, bson.M{"cwl_id": cwlID}, bson.M{
			"$set": bson.M{"cwl_id": cwlID, "data": jsonDocument(group)},
		}, true); err != nil {
			return err
		}
		app.Stats.RecordWrite(warsDomainName, 1)
	}
	return nil
}

func (d *warsDomain) scheduleStore(app *platform.App, warID, clanTag string, endAt time.Time, fallback clashy.ClanWar) {
	d.mu.Lock()
	if _, exists := d.scheduled[warID]; exists {
		d.mu.Unlock()
		return
	}
	d.scheduled[warID] = struct{}{}
	d.mu.Unlock()

	app.Scheduler.Schedule(platform.Job{
		ID:   warID,
		When: endAt,
		Run: func(ctx context.Context) {
			war := fallback
			if fetched, err := app.Clash.GetCurrentWar(ctx, clanTag); err == nil && fetched != nil && fetched.EndTime != nil {
				war = *fetched
			}
			data := rawDocument(war.Raw)
			if len(data) == 0 {
				data = jsonDocument(war)
			}
			_ = app.Store.Stats().Collection("clan_wars").UpdateOne(ctx, bson.M{"war_id": warID}, bson.M{
				"$set": bson.M{"data": data, "type": war.Type()},
			}, true)
			app.Bus.Publish(platform.Event{Topic: "war", ClanTag: clanTag, Value: map[string]any{"type": "war_stored", "war_id": warID}})
		},
	})
}

func (d *warsDomain) skipTag(tag string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	until, ok := d.inWar[tag]
	return ok && until.After(time.Now().UTC())
}

func (d *warsDomain) trackInWar(tag string, until time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inWar[tag] = until
}
