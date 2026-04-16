package scripts

import (
	"context"
	"time"

	"clashking_tracking/internal/platform"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const giveawaysDomainName = "giveaways"

type giveawaysDomain struct{}

func NewGiveawaysDomain() platform.Domain { return &giveawaysDomain{} }

func (d *giveawaysDomain) Name() string { return giveawaysDomainName }

func (d *giveawaysDomain) Run(ctx context.Context, app *platform.App) error {
	for {
		start := time.Now()
		err := d.scan(ctx, app)
		app.Stats.RecordProcess(giveawaysDomainName, time.Since(start))
		if err != nil && app.Config.RunOnce {
			return err
		}
		if app.Config.RunOnce {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Minute):
		}
	}
}

func (d *giveawaysDomain) scan(ctx context.Context, app *platform.App) error {
	now := time.Now().UTC()
	// Run the same scan over each lifecycle transition so event emission stays tied to the DB mutation that follows.
	for _, query := range []struct {
		kind   string
		filter bson.M
		update bson.M
	}{
		{kind: "giveaway_start", filter: bson.M{"start_time": bson.M{"$lte": now}, "status": "scheduled"}, update: bson.M{"$set": bson.M{"status": "ongoing"}}},
		{kind: "giveaway_end", filter: bson.M{"end_time": bson.M{"$lte": now}, "status": "ongoing"}, update: bson.M{"$set": bson.M{"status": "ended"}}},
		{kind: "giveaway_update", filter: bson.M{"status": "ongoing", "updated": "yes"}, update: bson.M{"$set": bson.M{"updated": "no"}}},
	} {
		docs, err := app.Store.Stats().Collection("giveaways").FindAll(ctx, query.filter, platform.FindOptions{})
		if err != nil {
			return err
		}
		for _, doc := range docs {
			app.Bus.Publish(platform.Event{
				Topic: "giveaway",
				Value: map[string]any{"type": query.kind, "giveaway": doc},
			})
		}
		if len(docs) > 0 {
			if err := app.Store.Stats().Collection("giveaways").UpdateMany(ctx, query.filter, query.update); err != nil {
				return err
			}
			app.Stats.RecordWrite(giveawaysDomainName, len(docs))
		}
	}
	return nil
}
