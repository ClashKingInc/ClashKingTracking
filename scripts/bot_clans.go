package scripts

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	"clashking_tracking/internal/platform"

	clashy "github.com/clashkinginc/clashy.go"
	clashevents "github.com/clashkinginc/clashy.go/events"
	clashstores "github.com/clashkinginc/clashy.go/stores"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	botClansDomainName = "botclans"
	botClansRateLimit  = 950
)

type botClansDomain struct {
	mu        sync.Mutex
	scheduled map[string]struct{}
}

func NewBotClansDomain() platform.Domain {
	return &botClansDomain{
		scheduled: make(map[string]struct{}),
	}
}

func (d *botClansDomain) Name() string { return botClansDomainName }

func (d *botClansDomain) Run(ctx context.Context, app *platform.App) error {
	if app.Config.RunOnce {
		return d.runOnce(ctx, app)
	}

	source := staticClanSource{app: app}
	clanTracker := clashevents.NewTracker(
		"clan",
		app.Clash,
		func(ctx context.Context, tag string) (*clashy.Clan, int, error) {
			start := time.Now()
			clan, err := app.Clash.GetClan(ctx, tag)
			app.Stats.RecordRequest(botClansDomainName, time.Since(start), err)
			retry := 0
			if clan != nil {
				retry = clan.RetryAfter()
			}
			return clan, retry, err
		},
		clashstores.NewMemoryStore(),
	)
	clanTracker.
		Group("clans", clashevents.Interval(0), clashevents.RateLimit(botClansRateLimit), clashevents.Source(source)).
		On(clashevents.EveryPoll("sync", func(ctx context.Context, change clashevents.Change[clashy.Clan]) error {
			return d.handleClanChange(app, change)
		}))

	warTracker := clashevents.NewTracker(
		"war",
		app.Clash,
		func(ctx context.Context, tag string) (*clashy.ClanWar, int, error) {
			start := time.Now()
			war, err := app.Clash.GetCurrentWar(ctx, tag)
			app.Stats.RecordRequest(botClansDomainName, time.Since(start), err)
			retry := 0
			if war != nil {
				retry = war.RetryAfter()
			}
			return war, retry, err
		},
		clashstores.NewMemoryStore(),
	)
	warTracker.
		Group("wars", clashevents.Interval(0), clashevents.RateLimit(botClansRateLimit), clashevents.Source(source)).
		On(clashevents.EveryPoll("sync", func(ctx context.Context, change clashevents.Change[clashy.ClanWar]) error {
			return d.handleWarChange(ctx, app, change)
		}))

	raidTracker := clashevents.NewTracker(
		"raid",
		app.Clash,
		func(ctx context.Context, tag string) (*clashy.RaidLogEntry, int, error) {
			start := time.Now()
			raids, err := app.Clash.GetRaidLog(ctx, tag, 1, "", "")
			app.Stats.RecordRequest(botClansDomainName, time.Since(start), err)
			if err != nil {
				return nil, 0, err
			}
			if len(raids) == 0 {
				return &clashy.RaidLogEntry{}, 0, nil
			}
			raid := raids[0]
			return &raid, raid.RetryAfter(), nil
		},
		clashstores.NewMemoryStore(),
	)
	raidTracker.
		Group("raids", clashevents.Interval(0), clashevents.RateLimit(botClansRateLimit), clashevents.Source(source)).
		On(clashevents.EveryPoll("sync", func(ctx context.Context, change clashevents.Change[clashy.RaidLogEntry]) error {
			return d.handleRaidChange(ctx, app, change)
		}))

	for _, tracker := range []interface{ Start(context.Context) error }{clanTracker, warTracker, raidTracker} {
		if err := tracker.Start(ctx); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (d *botClansDomain) runOnce(ctx context.Context, app *platform.App) error {
	tags, err := collectSourceTags(ctx, staticClanSource{app: app})
	if err != nil {
		return err
	}
	for _, tag := range tags {
		if clan, err := app.Clash.GetClan(ctx, tag); err == nil && clan != nil {
			_ = d.handleClanChange(app, clashevents.Change[clashy.Clan]{Tag: tag, Current: clan})
		}
		if war, err := app.Clash.GetCurrentWar(ctx, tag); err == nil && war != nil {
			_ = d.handleWarChange(ctx, app, clashevents.Change[clashy.ClanWar]{Tag: tag, Current: war})
		}
		if raids, err := app.Clash.GetRaidLog(ctx, tag, 1, "", ""); err == nil && len(raids) > 0 {
			raid := raids[0]
			_ = d.handleRaidChange(ctx, app, clashevents.Change[clashy.RaidLogEntry]{Tag: tag, Current: &raid})
		}
	}
	return nil
}

type staticClanSource struct {
	app *platform.App
}

func (s staticClanSource) ForEach(ctx context.Context, fn func(tag string) error) error {
	tags, err := s.app.Store.Static().Collection("clans_db").DistinctStrings(ctx, "tag", bson.M{})
	if err != nil {
		return err
	}
	for _, tag := range tags {
		if err := fn(tag); err != nil {
			return err
		}
	}
	return nil
}

func (d *botClansDomain) handleClanChange(app *platform.App, change clashevents.Change[clashy.Clan]) error {
	if change.Current == nil {
		return nil
	}
	if change.Previous != nil && equalJSON(*change.Previous, *change.Current) {
		return nil
	}
	app.Stats.SetReady(botClansDomainName, true, "")
	app.Bus.Publish(platform.Event{
		Topic:   "clan",
		ClanTag: change.Tag,
		Value:   map[string]any{"type": "clan_update", "raw": rawPayload(change.Current.Raw, *change.Current)},
	})
	return nil
}

func (d *botClansDomain) handleWarChange(ctx context.Context, app *platform.App, change clashevents.Change[clashy.ClanWar]) error {
	if change.Current == nil {
		return nil
	}
	if change.Previous != nil && equalJSON(*change.Previous, *change.Current) {
		return nil
	}

	current := *change.Current
	app.Stats.SetReady(botClansDomainName, true, "")
	app.Bus.Publish(platform.Event{
		Topic:   "war",
		ClanTag: change.Tag,
		Value:   map[string]any{"type": "war_update", "raw": rawPayload(current.Raw, current)},
	})

	if current.EndTime == nil || current.EndTime.Time.IsZero() {
		return nil
	}
	reminderTimes, _ := app.Store.Static().Collection("reminders").DistinctStrings(ctx, "time", bson.M{"clan": change.Tag, "type": "War"})
	for _, reminder := range reminderTimes {
		hours, err := parseReminderHours(reminder)
		if err != nil {
			continue
		}
		jobID := change.Tag + ":" + reminder + ":" + current.EndTime.RawTime
		runAt := current.EndTime.Time.Add(-time.Duration(hours * float64(time.Hour)))
		d.mu.Lock()
		if _, exists := d.scheduled[jobID]; exists {
			d.mu.Unlock()
			continue
		}
		d.scheduled[jobID] = struct{}{}
		d.mu.Unlock()
		app.Scheduler.Schedule(platform.Job{
			ID:   jobID,
			When: runAt,
			Run: func(context.Context) {
				app.Bus.Publish(platform.Event{Topic: "reminder", ClanTag: change.Tag, Value: map[string]any{"type": "war", "time": reminder}})
			},
		})
	}
	return nil
}

func (d *botClansDomain) handleRaidChange(ctx context.Context, app *platform.App, change clashevents.Change[clashy.RaidLogEntry]) error {
	if change.Current == nil {
		return nil
	}
	if change.Previous != nil && equalJSON(*change.Previous, *change.Current) {
		return nil
	}
	payload := rawPayload(change.Current.Raw, *change.Current)
	if err := app.Store.Stats().Collection("capital_cache").UpdateOne(ctx, bson.M{"tag": change.Tag}, bson.M{"$set": bson.M{"data": payload}}, true); err != nil {
		return err
	}
	app.Stats.RecordWrite(botClansDomainName, 1)
	app.Stats.SetReady(botClansDomainName, true, "")
	app.Bus.Publish(platform.Event{
		Topic:   "capital",
		ClanTag: change.Tag,
		Value:   map[string]any{"type": "raid_update", "raw": payload},
	})
	return nil
}

func rawPayload(raw []byte, fallback any) string {
	if len(raw) > 0 {
		return string(raw)
	}
	return string(jsonBytes(fallback))
}

func parseReminderHours(raw string) (float64, error) {
	value := strings.TrimSpace(strings.TrimSuffix(raw, "hr"))
	return strconv.ParseFloat(value, 64)
}
