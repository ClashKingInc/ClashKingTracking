package scripts

import (
	"context"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"

	clashy "github.com/clashkinginc/clashy.go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const scheduledDomainName = "scheduled"

type leaderboardLoader func(context.Context, *clashy.Client, int) (any, error)

var leaderboardPaths = []struct {
	Collection string
	Load       leaderboardLoader
}{
	{Collection: "clan_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID int) (any, error) {
		return client.GetLocationClans(ctx, locationID, 0, "", "")
	}},
	{Collection: "clan_versus_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID int) (any, error) {
		return client.GetLocationClansBuilderBase(ctx, locationID, 0, "", "")
	}},
	{Collection: "capital", Load: func(ctx context.Context, client *clashy.Client, locationID int) (any, error) {
		return client.GetLocationClansCapital(ctx, locationID, 0, "", "")
	}},
	{Collection: "player_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID int) (any, error) {
		return client.GetLocationPlayers(ctx, locationID, 0, "", "")
	}},
	{Collection: "player_versus_trophies", Load: func(ctx context.Context, client *clashy.Client, locationID int) (any, error) {
		return client.GetLocationPlayersBuilderBase(ctx, locationID, 0, "", "")
	}},
}

type scheduledDomain struct{}

func NewScheduledDomain() platform.Domain { return &scheduledDomain{} }

func (d *scheduledDomain) Name() string { return scheduledDomainName }

func (d *scheduledDomain) Run(ctx context.Context, app *platform.App) error {
	for {
		start := time.Now()
		err := d.runCycle(ctx, app)
		app.Stats.RecordProcess(scheduledDomainName, time.Since(start))
		if err != nil && app.Config.RunOnce {
			return err
		}
		if app.Config.RunOnce {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(15 * time.Minute):
		}
	}
}

func (d *scheduledDomain) runCycle(ctx context.Context, app *platform.App) error {
	if utils.IsCWL(time.Now()) {
		_ = d.storeCWLWars(ctx, app)
	}
	global, err := app.Clash.GetLocationNamed(ctx, "global")
	if err != nil || global == nil {
		return err
	}
	for _, item := range leaderboardPaths {
		start := time.Now()
		payload, err := item.Load(ctx, app.Clash, global.ID)
		app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
		if err != nil {
			continue
		}
		doc := bson.M{
			"location": "global",
			"date":     time.Now().UTC().Format("2006-01-02"),
			"data":     bson.M{"items": jsonValue(payload)},
		}
		if err := app.Store.Stats().Collection(item.Collection).InsertOne(ctx, doc); err == nil {
			app.Stats.RecordWrite(scheduledDomainName, 1)
		}
	}
	return nil
}

func (d *scheduledDomain) storeCWLWars(ctx context.Context, app *platform.App) error {
	season := utils.CurrentGamesSeason(time.Now())
	docs, err := app.Store.Stats().Collection("cwl_group").FindAll(ctx, bson.M{"data.season": season}, platform.FindOptions{})
	if err != nil {
		return err
	}
	for _, doc := range docs {
		data, _ := doc["data"].(bson.M)
		rounds, _ := data["rounds"].([]any)
		for _, round := range rounds {
			roundMap, _ := round.(bson.M)
			warTags, _ := roundMap["warTags"].([]any)
			for _, tagValue := range warTags {
				tag, _ := tagValue.(string)
				if tag == "" {
					continue
				}
				start := time.Now()
				wars, err := app.Clash.GetLeagueWars(ctx, []string{tag})
				app.Stats.RecordRequest(scheduledDomainName, time.Since(start), err)
				if err != nil || len(wars) == 0 {
					continue
				}
				war := wars[0]
				data := rawDocument(war.Raw)
				if len(data) == 0 {
					data = jsonDocument(war)
				}
				data["tag"] = tag
				data["season"] = season
				if err := app.Store.Stats().Collection("clan_wars").UpdateOne(ctx, bson.M{"data.tag": tag}, bson.M{"$set": bson.M{"data": data, "type": "cwl"}}, true); err == nil {
					app.Stats.RecordWrite(scheduledDomainName, 1)
				}
			}
		}
	}
	return nil
}
