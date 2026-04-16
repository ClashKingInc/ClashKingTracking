package scripts

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"

	clashy "github.com/clashkinginc/clashy.go"
	clashevents "github.com/clashkinginc/clashy.go/events"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	globalClansDomainName    = "globalclans"
	globalClansActiveRate    = 950
	globalClansInactiveRate  = 50
	globalClansActiveCache   = globalClansActiveRate * 2
	globalClansInactiveCache = globalClansInactiveRate * 2
)

type globalClansDomain struct {
	mu              sync.RWMutex
	priorityClans   map[string]struct{}
	priorityPlayers map[string]struct{}
	season          string

	activeSnapshots   *allClansSnapshotStore
	inactiveSnapshots *allClansSnapshotStore
}

func NewGlobalClansDomain() platform.Domain { return &globalClansDomain{} }

func (d *globalClansDomain) Name() string { return globalClansDomainName }

func (d *globalClansDomain) Run(ctx context.Context, app *platform.App) error {
	d.activeSnapshots = newAllClansSnapshotStore(app.Store, globalClansActiveCache)
	d.inactiveSnapshots = newAllClansSnapshotStore(app.Store, globalClansInactiveCache)

	if app.Config.RunOnce {
		return d.runOnce(ctx, app)
	}

	tracker := clashevents.NewTracker(
		"clan",
		app.Clash,
		func(ctx context.Context, tag string) (*clashy.Clan, int, error) {
			start := time.Now()
			clan, err := app.Clash.GetClan(ctx, tag)
			app.Stats.RecordRequest(globalClansDomainName, time.Since(start), err)
			retry := 0
			if clan != nil {
				retry = clan.RetryAfter()
			}
			return clan, retry, err
		},
		d.activeSnapshots,
	)

	tracker.
		Group(
			"active",
			clashevents.Interval(0),
			clashevents.RateLimit(globalClansActiveRate),
			clashevents.Source(activeClanSource{app: app, domain: d}),
			clashevents.Store(d.activeSnapshots),
		).
		Group(
			"inactive",
			clashevents.Interval(0),
			clashevents.RateLimit(globalClansInactiveRate),
			clashevents.Source(inactiveClanSource{app: app, domain: d}),
			clashevents.Store(d.inactiveSnapshots),
		).
		On(clashevents.EveryPoll("sync", func(ctx context.Context, change clashevents.Change[clashy.Clan]) error {
			return d.applyClanChange(ctx, app, change)
		})).
		OnError(func(ctx context.Context, ec clashevents.ErrorChange[clashy.Clan]) *clashevents.ErrorDecision {
			app.Stats.SetReady(globalClansDomainName, false, ec.Err.Error())
			return &clashevents.ErrorDecision{Action: clashevents.ErrorActionSkip}
		})

	if err := tracker.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func (d *globalClansDomain) runOnce(ctx context.Context, app *platform.App) error {
	if err := d.refreshMetadata(ctx, app); err != nil {
		return err
	}
	activeTags, err := d.collectTags(ctx, activeClanSource{app: app, domain: d})
	if err != nil {
		return err
	}
	inactiveTags, err := d.collectTags(ctx, inactiveClanSource{app: app, domain: d})
	if err != nil {
		return err
	}
	for _, run := range []struct {
		group string
		tags  []string
	}{
		{group: "active", tags: activeTags},
		{group: "inactive", tags: inactiveTags},
	} {
		previous, records, err := loadExistingClans(ctx, app, run.tags)
		if err != nil {
			return err
		}
		store := d.storeForGroup(run.group)
		for tag, clan := range previous {
			store.cache.Put(tag, jsonBytes(*clan), records[tag])
		}
		for _, tag := range run.tags {
			start := time.Now()
			clan, err := app.Clash.GetClan(ctx, tag)
			app.Stats.RecordRequest(globalClansDomainName, time.Since(start), err)
			if err != nil || clan == nil {
				continue
			}
			if err := d.applyClanChange(ctx, app, clashevents.Change[clashy.Clan]{
				Group:    run.group,
				Kind:     "clan",
				Tag:      tag,
				Previous: previous[tag],
				Current:  clan,
			}); err != nil {
				return err
			}
			store.Save(ctx, clashevents.Snapshot{Kind: "clan", Key: tag, Data: jsonBytes(clan)})
		}
	}
	return nil
}

func (d *globalClansDomain) collectTags(ctx context.Context, source clashevents.TagSource) ([]string, error) {
	var tags []string
	err := source.ForEach(ctx, func(tag string) error {
		tags = append(tags, tag)
		return nil
	})
	return tags, err
}

func (d *globalClansDomain) refreshMetadata(ctx context.Context, app *platform.App) error {
	priorityClans, err := buildStringSet(app.Store.Static().Collection("clans_db").DistinctStrings(ctx, "tag", bson.M{}))
	if err != nil {
		return err
	}
	priorityPlayers, err := buildStringSet(app.Store.Static().Collection("user_settings").DistinctStrings(ctx, "search.player.bookmarked", bson.M{}))
	if err != nil {
		return err
	}

	d.mu.Lock()
	d.priorityClans = priorityClans
	d.priorityPlayers = priorityPlayers
	d.season = utils.CurrentSeason(time.Now())
	d.mu.Unlock()
	return nil
}

func (d *globalClansDomain) metadata() (map[string]struct{}, map[string]struct{}, string) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.priorityClans, d.priorityPlayers, d.season
}

func (d *globalClansDomain) storeForGroup(group string) *allClansSnapshotStore {
	if group == "inactive" {
		return d.inactiveSnapshots
	}
	return d.activeSnapshots
}

type activeClanSource struct {
	app    *platform.App
	domain *globalClansDomain
}

func (s activeClanSource) ForEach(ctx context.Context, fn func(tag string) error) error {
	if err := s.domain.refreshMetadata(ctx, s.app); err != nil {
		return err
	}
	active, err := s.app.Store.Stats().Collection("all_clans").DistinctStrings(ctx, "tag", bson.M{"active": true})
	if err != nil {
		return err
	}
	botClans, err := s.app.Store.Static().Collection("clans_db").DistinctStrings(ctx, "tag", bson.M{})
	if err != nil {
		return err
	}
	for _, tag := range mergeStrings(active, botClans) {
		if err := fn(tag); err != nil {
			return err
		}
	}
	return nil
}

type inactiveClanSource struct {
	app    *platform.App
	domain *globalClansDomain
}

func (s inactiveClanSource) ForEach(ctx context.Context, fn func(tag string) error) error {
	if err := s.domain.refreshMetadata(ctx, s.app); err != nil {
		return err
	}
	inactive, err := s.app.Store.Stats().Collection("all_clans").DistinctStrings(ctx, "tag", bson.M{"active": false})
	if err != nil {
		return err
	}
	for _, tag := range inactive {
		if err := fn(tag); err != nil {
			return err
		}
	}
	return nil
}

func (d *globalClansDomain) applyClanChange(ctx context.Context, app *platform.App, change clashevents.Change[clashy.Clan]) error {
	if change.Current == nil {
		return nil
	}
	current := *change.Current
	if current.Tag == "" {
		current.Tag = change.Tag
	}
	store := d.storeForGroup(change.Group)
	currentRecords, _ := store.Records(current.Tag)
	priorityClans, priorityPlayers, season := d.metadata()

	var allClanWrites []mongo.WriteModel
	var historyWrites []mongo.WriteModel
	var joinLeaveWrites []mongo.WriteModel
	var seasonStatWrites []mongo.WriteModel

	if current.MemberCount == 0 {
		allClanWrites = append(allClanWrites, mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": current.Tag}))
	} else {
		if change.Previous != nil {
			previous := *change.Previous
			joinLeaveWrites = append(joinLeaveWrites, joinLeaveModels(previous, current)...)
			historyWrites = append(historyWrites, clanHistoryModels(previous, current)...)
			recordWrites, updatedRecords := recordModels(currentRecords, current)
			currentRecords = updatedRecords
			allClanWrites = append(allClanWrites, recordWrites...)
			seasonStatWrites = append(seasonStatWrites, donationModels(previous, current, priorityClans, priorityPlayers, season)...)
		} else {
			_, updatedRecords := recordModels(currentRecords, current)
			currentRecords = updatedRecords
		}
		if update := clanUpsertModel(change.Previous, current); update != nil {
			allClanWrites = append(allClanWrites, update)
		}
	}

	if err := app.Store.Stats().Collection("all_clans").BulkWrite(ctx, allClanWrites, false); err != nil {
		return err
	}
	if err := app.Store.Stats().Collection("clan_change_history").BulkWrite(ctx, historyWrites, false); err != nil {
		return err
	}
	if err := app.Store.Stats().Collection("join_leave_history").BulkWrite(ctx, joinLeaveWrites, false); err != nil {
		return err
	}
	if err := app.Store.Stats().Collection("new_player_stats").BulkWrite(ctx, seasonStatWrites, false); err != nil {
		return err
	}

	store.SetRecords(current.Tag, currentRecords)
	app.Stats.RecordWrite(globalClansDomainName, len(allClanWrites)+len(historyWrites)+len(joinLeaveWrites)+len(seasonStatWrites))
	app.Stats.SetReady(globalClansDomainName, true, "")
	app.Bus.Publish(platform.Event{
		Topic:   "clan",
		ClanTag: current.Tag,
		Value:   map[string]any{"tag": current.Tag, "name": current.Name},
	})
	return nil
}

func loadExistingClans(ctx context.Context, app *platform.App, tags []string) (map[string]*clashy.Clan, map[string]clanRecords, error) {
	docs, err := app.Store.Stats().Collection("all_clans").FindAll(
		ctx,
		bson.M{"tag": bson.M{"$in": tags}},
		platform.FindOptions{Projection: bson.M{"tag": 1, "data": 1, "records": 1}},
	)
	if err != nil {
		return nil, nil, err
	}
	clans := make(map[string]*clashy.Clan, len(docs))
	records := make(map[string]clanRecords, len(docs))
	for _, doc := range docs {
		tag, _ := doc["tag"].(string)
		if tag == "" {
			continue
		}
		rawData, ok := doc["data"]
		if !ok {
			continue
		}
		payload, err := json.Marshal(rawData)
		if err != nil {
			continue
		}
		var clan clashy.Clan
		if err := json.Unmarshal(payload, &clan); err != nil {
			continue
		}
		clans[tag] = &clan
		if rawRecords, ok := doc["records"].(bson.M); ok {
			var decoded clanRecords
			if decodeBSON(rawRecords, &decoded) == nil {
				records[tag] = decoded
			}
		}
	}
	return clans, records, nil
}

func clanUpsertModel(previous *clashy.Clan, current clashy.Clan) mongo.WriteModel {
	if previous != nil && equalJSON(*previous, current) {
		return nil
	}
	data := clanDocument(current)
	if previous == nil {
		return mongo.NewInsertOneModel().SetDocument(bson.M{
			"_id":     current.Tag,
			"tag":     current.Tag,
			"active":  true,
			"data":    data,
			"records": bson.M{},
		})
	}
	return mongo.NewUpdateOneModel().SetFilter(bson.M{"tag": current.Tag}).SetUpdate(bson.M{
		"$set": bson.M{
			"tag":    current.Tag,
			"active": true,
			"data":   data,
		},
	}).SetUpsert(true)
}

func clanDocument(clan clashy.Clan) bson.M {
	if len(clan.Raw) > 0 {
		return rawDocument(clan.Raw)
	}
	return jsonDocument(clan)
}

func recordModels(previous clanRecords, current clashy.Clan) ([]mongo.WriteModel, clanRecords) {
	now := int(time.Now().UTC().Unix())
	next := previous
	var out []mongo.WriteModel
	if current.WarWinStreak > previous.WarWinStreak.Value {
		next.WarWinStreak = clanRecordValue{Value: current.WarWinStreak, Time: now}
		out = append(out, mongo.NewUpdateOneModel().SetFilter(bson.M{"tag": current.Tag}).SetUpdate(bson.M{
			"$set": bson.M{"records.warWinStreak": next.WarWinStreak},
		}))
	}
	if current.Points > previous.ClanPoints.Value {
		next.ClanPoints = clanRecordValue{Value: current.Points, Time: now}
		out = append(out, mongo.NewUpdateOneModel().SetFilter(bson.M{"tag": current.Tag}).SetUpdate(bson.M{
			"$set": bson.M{"records.clanPoints": next.ClanPoints},
		}))
	}
	return out, next
}

func clanHistoryModels(previous, current clashy.Clan) []mongo.WriteModel {
	now := int(time.Now().UTC().Unix())
	var out []mongo.WriteModel
	if previous.Description != current.Description {
		out = append(out, mongo.NewInsertOneModel().SetDocument(bson.M{
			"type": "description", "clan": current.Tag, "previous": previous.Description, "current": current.Description, "time": now,
		}))
	}
	if previous.Level != current.Level {
		out = append(out, mongo.NewInsertOneModel().SetDocument(bson.M{
			"type": "clan_level", "clan": current.Tag, "previous": previous.Level, "current": current.Level, "time": now,
		}))
	}
	return out
}

func joinLeaveModels(previous, current clashy.Clan) []mongo.WriteModel {
	now := time.Now().UTC()
	currentMembers := clanMembersByTag(current.Members)
	previousMembers := clanMembersByTag(previous.Members)
	var out []mongo.WriteModel
	for tag, member := range currentMembers {
		if _, ok := previousMembers[tag]; ok {
			continue
		}
		out = append(out, mongo.NewInsertOneModel().SetDocument(bson.M{
			"type": "join", "clan": current.Tag, "time": now, "tag": tag, "name": member.Name, "th": 0,
		}))
	}
	for tag, member := range previousMembers {
		if _, ok := currentMembers[tag]; ok {
			continue
		}
		out = append(out, mongo.NewInsertOneModel().SetDocument(bson.M{
			"type": "leave", "clan": current.Tag, "time": now, "tag": tag, "name": member.Name, "th": 0,
		}))
	}
	return out
}

func donationModels(previous, current clashy.Clan, priorityClans, priorityPlayers map[string]struct{}, season string) []mongo.WriteModel {
	if _, ok := priorityClans[current.Tag]; ok {
		return nil
	}
	currentMembers := clanMembersByTag(current.Members)
	previousMembers := clanMembersByTag(previous.Members)
	var out []mongo.WriteModel
	for tag, member := range currentMembers {
		if _, ok := priorityPlayers[tag]; ok {
			continue
		}
		prev, ok := previousMembers[tag]
		if !ok {
			continue
		}
		donated := member.Donations - prev.Donations
		received := member.Received - prev.Received
		if donated <= 0 && received <= 0 {
			continue
		}
		out = append(out, mongo.NewUpdateOneModel().SetFilter(bson.M{
			"tag": tag, "season": season, "clan_tag": current.Tag,
		}).SetUpdate(bson.M{"$inc": bson.M{"donations": donated, "donationsReceived": received}}).SetUpsert(true))
	}
	return out
}

func clanMembersByTag(members []clashy.ClanMember) map[string]clashy.ClanMember {
	out := make(map[string]clashy.ClanMember, len(members))
	for _, member := range members {
		out[member.Tag] = member
	}
	return out
}
