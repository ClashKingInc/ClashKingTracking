package scripts

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"

	clashy "github.com/clashkinginc/clashy.go"
	clashevents "github.com/clashkinginc/clashy.go/events"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	botPlayersDomainName = "botplayers"
	botPlayersRateLimit  = 950
)

type botPlayersDomain struct {
	snapshots *redisSnapshotStore
}

func NewBotPlayersDomain() platform.Domain { return &botPlayersDomain{} }

func (d *botPlayersDomain) Name() string { return botPlayersDomainName }

func (d *botPlayersDomain) Run(ctx context.Context, app *platform.App) error {
	d.snapshots = newRedisSnapshotStore(app.Redis, "player-cache:", botPlayersRateLimit*2)

	if app.Config.RunOnce {
		return d.runOnce(ctx, app)
	}

	tracker := clashevents.NewTracker(
		"player",
		app.Clash,
		func(ctx context.Context, tag string) (*clashy.Player, int, error) {
			start := time.Now()
			player, err := app.Clash.GetPlayer(ctx, tag)
			app.Stats.RecordRequest(botPlayersDomainName, time.Since(start), err)
			retry := 0
			if player != nil {
				retry = player.RetryAfter()
			}
			return player, retry, err
		},
		d.snapshots,
	)

	tracker.
		Group(
			"players",
			clashevents.Interval(0),
			clashevents.RateLimit(botPlayersRateLimit),
			clashevents.Source(trackedPlayersSource{app: app}),
			clashevents.Store(d.snapshots),
		).
		On(clashevents.EveryPoll("sync", func(ctx context.Context, change clashevents.Change[clashy.Player]) error {
			return d.applyPlayerChange(ctx, app, change)
		})).
		OnError(func(ctx context.Context, ec clashevents.ErrorChange[clashy.Player]) *clashevents.ErrorDecision {
			app.Stats.SetReady(botPlayersDomainName, false, ec.Err.Error())
			return &clashevents.ErrorDecision{Action: clashevents.ErrorActionSkip}
		})

	if err := tracker.Start(ctx); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func (d *botPlayersDomain) runOnce(ctx context.Context, app *platform.App) error {
	tags, err := collectSourceTags(ctx, trackedPlayersSource{app: app})
	if err != nil {
		return err
	}
	for _, tag := range tags {
		var previous *clashy.Player
		snapshot, err := d.snapshots.Load(ctx, "player", tag)
		if err == nil {
			var decoded clashy.Player
			if json.Unmarshal(snapshot.Data, &decoded) == nil {
				previous = &decoded
			}
		}
		start := time.Now()
		current, err := app.Clash.GetPlayer(ctx, tag)
		app.Stats.RecordRequest(botPlayersDomainName, time.Since(start), err)
		if err != nil || current == nil {
			continue
		}
		if err := d.applyPlayerChange(ctx, app, clashevents.Change[clashy.Player]{
			Group:    "players",
			Kind:     "player",
			Tag:      tag,
			Previous: previous,
			Current:  current,
		}); err != nil {
			return err
		}
		_ = d.snapshots.Save(ctx, clashevents.Snapshot{Kind: "player", Key: tag, Data: jsonBytes(current)})
	}
	return nil
}

type trackedPlayersSource struct {
	app *platform.App
}

func (s trackedPlayersSource) ForEach(ctx context.Context, fn func(tag string) error) error {
	tags, err := trackedPlayers(ctx, s.app)
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

func trackedPlayers(ctx context.Context, app *platform.App) ([]string, error) {
	clanTags, err := app.Store.Static().Collection("clans_db").DistinctStrings(ctx, "tag", bson.M{})
	if err != nil {
		return nil, err
	}
	bookmarked, _ := app.Store.Static().Collection("user_settings").DistinctStrings(ctx, "search.player.bookmarked", bson.M{})
	set := make(map[string]struct{}, len(bookmarked))
	for _, tag := range bookmarked {
		set[tag] = struct{}{}
	}
	for _, clanTag := range clanTags {
		start := time.Now()
		clan, err := app.Clash.GetClan(ctx, clanTag)
		app.Stats.RecordRequest(botPlayersDomainName, time.Since(start), err)
		if err != nil || clan == nil {
			continue
		}
		for _, member := range clan.Members {
			set[member.Tag] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for tag := range set {
		out = append(out, tag)
	}
	sort.Strings(out)
	return out, nil
}

func (d *botPlayersDomain) applyPlayerChange(ctx context.Context, app *platform.App, change clashevents.Change[clashy.Player]) error {
	if change.Current == nil {
		return nil
	}

	current := playerMap(*change.Current)
	if len(current) == 0 {
		return nil
	}
	if change.Previous == nil {
		return nil
	}
	previous := playerMap(*change.Previous)
	if equalJSON(previous, current) {
		return nil
	}

	historyWrites, activityScore, statWrites := playerChanges(change.Tag, previous, current)
	var baseWrites []mongo.WriteModel
	if activityScore > 0 {
		now := time.Now().UTC().Unix()
		baseWrites = append(baseWrites, mongo.NewUpdateOneModel().SetFilter(bson.M{"tag": change.Tag}).SetUpdate(bson.M{
			"$push": bson.M{"last_online_list": now},
			"$set":  bson.M{"last_online": now},
		}).SetUpsert(true))
	}

	if err := app.Store.Stats().Collection("base_player").BulkWrite(ctx, baseWrites, false); err != nil {
		return err
	}
	if err := app.Store.Stats().Collection("player_history").BulkWrite(ctx, historyWrites, false); err != nil {
		return err
	}
	if err := app.Store.Stats().Collection("new_player_stats").BulkWrite(ctx, statWrites, false); err != nil {
		return err
	}

	app.Stats.RecordWrite(botPlayersDomainName, len(baseWrites)+len(historyWrites)+len(statWrites))
	app.Stats.SetReady(botPlayersDomainName, true, "")
	app.Bus.Publish(platform.Event{
		Topic:   "player",
		ClanTag: clanTag(current),
		Value: map[string]any{
			"tag":           change.Tag,
			"changed_types": changedTypes(historyWrites),
			"new_player":    current,
			"old_player":    previous,
		},
	})
	return nil
}

func playerMap(player clashy.Player) map[string]any {
	value, _ := jsonValue(player).(map[string]any)
	return value
}

func playerChanges(tag string, previous, current map[string]any) ([]mongo.WriteModel, int, []mongo.WriteModel) {
	var historyWrites []mongo.WriteModel
	var statWrites []mongo.WriteModel
	activityScore := 0
	season := utils.CurrentSeason(time.Now())
	clan := clanTag(current)

	for key, currentValue := range current {
		previousValue, exists := previous[key]
		if exists && equalJSON(previousValue, currentValue) {
			continue
		}
		if isHistoricalField(key) {
			historyWrites = append(historyWrites, mongo.NewInsertOneModel().SetDocument(bson.M{
				"tag": tag, "type": key, "p_value": previousValue, "value": currentValue, "time": time.Now().UTC().Unix(), "clan": clan, "th": current["townHallLevel"],
			}))
		}
		if isOnlineField(key) {
			activityScore++
		}
		if incKey, ok := seasonalIncField(key); ok {
			change := numericChange(previousValue, currentValue)
			if change > 0 && clan != "" {
				statWrites = append(statWrites, mongo.NewUpdateOneModel().SetFilter(bson.M{
					"tag": tag, "season": season, "clan_tag": clan,
				}).SetUpdate(bson.M{"$inc": bson.M{incKey: change}}).SetUpsert(true))
			}
		}
	}
	if activityScore > 0 && clan != "" {
		statWrites = append(statWrites, mongo.NewUpdateOneModel().SetFilter(bson.M{
			"tag": tag, "season": season, "clan_tag": clan,
		}).SetUpdate(bson.M{"$inc": bson.M{"activity_score": 1}}).SetUpsert(true))
	}
	return historyWrites, activityScore, statWrites
}

func isHistoricalField(key string) bool {
	switch key {
	case "name", "troops", "heroes", "spells", "heroEquipment", "townHallLevel", "warStars", "warPreference", "bestBuilderBaseTrophies", "bestTrophies", "expLevel":
		return true
	default:
		return false
	}
}

func isOnlineField(key string) bool {
	switch key {
	case "donations", "attackWins", "warStars", "builderBaseTrophies", "warPreference", "name", "heroEquipment":
		return true
	default:
		return false
	}
}

func seasonalIncField(key string) (string, bool) {
	switch key {
	case "donations":
		return "donated", true
	case "donationsReceived":
		return "received", true
	case "clanCapitalContributions":
		return "capital_gold_dono", true
	default:
		return "", false
	}
}

func numericChange(previous, current any) int {
	prev, ok1 := asInt(previous)
	curr, ok2 := asInt(current)
	if !ok1 || !ok2 {
		return 0
	}
	return curr - prev
}

func asInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func equalJSON(a, b any) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return bytes.Equal(left, right)
}

func changedTypes(changes []mongo.WriteModel) []string {
	out := make([]string, 0, len(changes))
	for _, change := range changes {
		switch change.(type) {
		case *mongo.InsertOneModel:
			out = append(out, "insert")
		case *mongo.UpdateOneModel:
			out = append(out, "update")
		case *mongo.DeleteOneModel:
			out = append(out, "delete")
		default:
			out = append(out, "change")
		}
	}
	return out
}

func clanTag(player map[string]any) string {
	clan, _ := player["clan"].(map[string]any)
	value, _ := clan["tag"].(string)
	return value
}

func collectSourceTags(ctx context.Context, source clashevents.TagSource) ([]string, error) {
	var tags []string
	err := source.ForEach(ctx, func(tag string) error {
		tags = append(tags, tag)
		return nil
	})
	return tags, err
}
