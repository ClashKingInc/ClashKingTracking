package platform

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoStore struct {
	statsClient  *mongo.Client
	staticClient *mongo.Client
}

func NewMongoStore(ctx context.Context, statsURI, staticURI string) (*MongoStore, error) {
	// Stats and static data live in separate Mongo clusters, so the store keeps
	// one client per URI instead of multiplexing through a shared connection.
	statsClient, err := mongo.Connect(options.Client().ApplyURI(statsURI).SetCompressors([]string{"snappy"}))
	if err != nil {
		return nil, err
	}
	staticClient, err := mongo.Connect(options.Client().ApplyURI(staticURI))
	if err != nil {
		_ = statsClient.Disconnect(ctx)
		return nil, err
	}
	return &MongoStore{statsClient: statsClient, staticClient: staticClient}, nil
}

func (s *MongoStore) Close(ctx context.Context) error {
	if err := s.statsClient.Disconnect(ctx); err != nil {
		return err
	}
	return s.staticClient.Disconnect(ctx)
}

func (s *MongoStore) IsMock() bool { return false }

func (s *MongoStore) Stats() Database {
	return mongoDatabase{client: s.statsClient}
}

func (s *MongoStore) Static() Database {
	return mongoDatabase{client: s.staticClient}
}

type mongoDatabase struct {
	client *mongo.Client
}

func (d mongoDatabase) Collection(name string) Collection {
	dbName, collName := collectionPath(name)
	return mongoCollection{collection: d.client.Database(dbName).Collection(collName)}
}

type mongoCollection struct {
	collection *mongo.Collection
}

func (c mongoCollection) AggregateAll(ctx context.Context, pipeline any) ([]bson.M, error) {
	cursor, err := c.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func (c mongoCollection) BulkWrite(ctx context.Context, models []mongo.WriteModel, ordered bool) error {
	if len(models) == 0 {
		return nil
	}
	_, err := c.collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(ordered))
	return err
}

func (c mongoCollection) DeleteMany(ctx context.Context, filter any) error {
	_, err := c.collection.DeleteMany(ctx, filter)
	return err
}

func (c mongoCollection) DeleteOne(ctx context.Context, filter any) error {
	_, err := c.collection.DeleteOne(ctx, filter)
	return err
}

func (c mongoCollection) DistinctStrings(ctx context.Context, field string, filter any) ([]string, error) {
	distinctResult := c.collection.Distinct(ctx, field, filter)
	if err := distinctResult.Err(); err != nil {
		return nil, err
	}
	var values []string
	if err := distinctResult.Decode(&values); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	return out, nil
}

func (c mongoCollection) FindAll(ctx context.Context, filter any, opts FindOptions) ([]bson.M, error) {
	findOptions := options.Find()
	if opts.Projection != nil {
		findOptions.SetProjection(opts.Projection)
	}
	if opts.Sort != nil {
		findOptions.SetSort(opts.Sort)
	}
	if opts.Limit > 0 {
		findOptions.SetLimit(opts.Limit)
	}
	cursor, err := c.collection.Find(ctx, filter, findOptions)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func (c mongoCollection) FindOne(ctx context.Context, filter any) (bson.M, error) {
	var doc bson.M
	err := c.collection.FindOne(ctx, filter).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrNotFound
	}
	return doc, err
}

func (c mongoCollection) InsertMany(ctx context.Context, docs []any, ordered bool) error {
	if len(docs) == 0 {
		return nil
	}
	_, err := c.collection.InsertMany(ctx, docs, options.InsertMany().SetOrdered(ordered))
	return err
}

func (c mongoCollection) InsertOne(ctx context.Context, doc any) error {
	_, err := c.collection.InsertOne(ctx, doc)
	return err
}

func (c mongoCollection) UpdateMany(ctx context.Context, filter any, update any) error {
	_, err := c.collection.UpdateMany(ctx, filter, update)
	return err
}

func (c mongoCollection) UpdateOne(ctx context.Context, filter any, update any, upsert bool) error {
	_, err := c.collection.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(upsert))
	return err
}

func collectionPath(name string) (string, string) {
	// Preserve the Python-era database/collection split so the Go rewrite writes
	// to the same logical storage locations without changing document shape.
	known := map[string][2]string{
		"tracking_stats":         {"clashking", "tracking_stats"},
		"global_clans":           {"looper", "clan_tags"},
		"all_clans":              {"looper", "all_clans"},
		"clan_change_history":    {"looper", "all_clans_changes"},
		"join_leave_history":     {"looper", "join_leave_history"},
		"new_player_stats":       {"looper", "player_stats"},
		"base_player":            {"looper", "base_player"},
		"player_history":         {"new_looper", "player_history"},
		"clan_wars":              {"looper", "clan_war"},
		"cwl_group":              {"looper", "cwl_group"},
		"basic_clan":             {"looper", "clan_tags"},
		"war_timer":              {"looper", "war_timer"},
		"legend_history":         {"looper", "legend_history"},
		"raid_weekends":          {"looper", "raid_weekends"},
		"capital_cache":          {"cache", "capital_raids"},
		"region_leaderboard":     {"new_looper", "leaderboard_db"},
		"player_trophies":        {"ranking_history", "player_trophies"},
		"player_versus_trophies": {"ranking_history", "player_versus_trophies"},
		"clan_trophies":          {"ranking_history", "clan_trophies"},
		"clan_versus_trophies":   {"ranking_history", "clan_versus_trophies"},
		"capital":                {"ranking_history", "capital"},
		"giveaways":              {"clashking", "giveaways"},
		"deleted_clans":          {"new_looper", "deleted_clans"},
		"clans_db":               {"usafam", "clans"},
		"user_settings":          {"usafam", "user_settings"},
		"reminders":              {"usafam", "reminders"},
	}
	if target, ok := known[name]; ok {
		return target[0], target[1]
	}
	return "looper", name
}
