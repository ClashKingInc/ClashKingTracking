package platform

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.opentelemetry.io/otel/attribute"
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
	ctx, span := StartSpan(ctx, "mongo.aggregate", mongoAttrs(c.collection, "aggregate")...)
	defer span.End()
	cursor, err := c.collection.Aggregate(ctx, pipeline)
	if err != nil {
		RecordSpanError(span, err)
		span.SetAttributes(SpanErrorStatus(err))
		return nil, err
	}
	defer cursor.Close(ctx)
	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		RecordSpanError(span, err)
		span.SetAttributes(SpanErrorStatus(err))
		return nil, err
	}
	span.SetAttributes(attribute.Int("rows.count", len(docs)), SpanErrorStatus(nil))
	return docs, nil
}

func (c mongoCollection) BulkWrite(ctx context.Context, models []mongo.WriteModel, ordered bool) error {
	if len(models) == 0 {
		return nil
	}
	ctx, span := StartSpan(ctx, "mongo.bulk_write", append(mongoAttrs(c.collection, "bulk_write"), attribute.Int("write.count", len(models)))...)
	defer span.End()
	_, err := c.collection.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(ordered))
	RecordSpanError(span, err)
	span.SetAttributes(SpanErrorStatus(err))
	return err
}

func (c mongoCollection) DeleteMany(ctx context.Context, filter any) error {
	ctx, span := StartSpan(ctx, "mongo.delete_many", mongoAttrs(c.collection, "delete_many")...)
	defer span.End()
	_, err := c.collection.DeleteMany(ctx, filter)
	RecordSpanError(span, err)
	span.SetAttributes(SpanErrorStatus(err))
	return err
}

func (c mongoCollection) DeleteOne(ctx context.Context, filter any) error {
	ctx, span := StartSpan(ctx, "mongo.delete_one", append(mongoAttrs(c.collection, "delete_one"), attribute.Int("write.count", 1))...)
	defer span.End()
	_, err := c.collection.DeleteOne(ctx, filter)
	RecordSpanError(span, err)
	span.SetAttributes(SpanErrorStatus(err))
	return err
}

func (c mongoCollection) DistinctStrings(ctx context.Context, field string, filter any) ([]string, error) {
	ctx, span := StartSpan(ctx, "mongo.distinct", mongoAttrs(c.collection, "distinct")...)
	defer span.End()
	distinctResult := c.collection.Distinct(ctx, field, filter)
	if err := distinctResult.Err(); err != nil {
		RecordSpanError(span, err)
		span.SetAttributes(SpanErrorStatus(err))
		return nil, err
	}
	var values []string
	if err := distinctResult.Decode(&values); err != nil {
		RecordSpanError(span, err)
		span.SetAttributes(SpanErrorStatus(err))
		return nil, err
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	span.SetAttributes(attribute.Int("rows.count", len(out)), SpanErrorStatus(nil))
	return out, nil
}

func (c mongoCollection) FindAll(ctx context.Context, filter any, opts FindOptions) ([]bson.M, error) {
	ctx, span := StartSpan(ctx, "mongo.find", mongoAttrs(c.collection, "find")...)
	defer span.End()
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
		RecordSpanError(span, err)
		span.SetAttributes(SpanErrorStatus(err))
		return nil, err
	}
	defer cursor.Close(ctx)
	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		RecordSpanError(span, err)
		span.SetAttributes(SpanErrorStatus(err))
		return nil, err
	}
	span.SetAttributes(attribute.Int("rows.count", len(docs)), SpanErrorStatus(nil))
	return docs, nil
}

func (c mongoCollection) FindOne(ctx context.Context, filter any) (bson.M, error) {
	ctx, span := StartSpan(ctx, "mongo.find_one", mongoAttrs(c.collection, "find_one")...)
	defer span.End()
	var doc bson.M
	err := c.collection.FindOne(ctx, filter).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		span.SetAttributes(attribute.Int("rows.count", 0), SpanErrorStatus(nil))
		return nil, ErrNotFound
	}
	RecordSpanError(span, err)
	if err == nil {
		span.SetAttributes(attribute.Int("rows.count", 1))
	}
	span.SetAttributes(SpanErrorStatus(err))
	return doc, err
}

func (c mongoCollection) InsertMany(ctx context.Context, docs []any, ordered bool) error {
	if len(docs) == 0 {
		return nil
	}
	ctx, span := StartSpan(ctx, "mongo.insert_many", append(mongoAttrs(c.collection, "insert_many"), attribute.Int("write.count", len(docs)))...)
	defer span.End()
	_, err := c.collection.InsertMany(ctx, docs, options.InsertMany().SetOrdered(ordered))
	RecordSpanError(span, err)
	span.SetAttributes(SpanErrorStatus(err))
	return err
}

func (c mongoCollection) InsertOne(ctx context.Context, doc any) error {
	ctx, span := StartSpan(ctx, "mongo.insert_one", append(mongoAttrs(c.collection, "insert_one"), attribute.Int("write.count", 1))...)
	defer span.End()
	_, err := c.collection.InsertOne(ctx, doc)
	RecordSpanError(span, err)
	span.SetAttributes(SpanErrorStatus(err))
	return err
}

func (c mongoCollection) UpdateMany(ctx context.Context, filter any, update any) error {
	ctx, span := StartSpan(ctx, "mongo.update_many", mongoAttrs(c.collection, "update_many")...)
	defer span.End()
	_, err := c.collection.UpdateMany(ctx, filter, update)
	RecordSpanError(span, err)
	span.SetAttributes(SpanErrorStatus(err))
	return err
}

func (c mongoCollection) UpdateOne(ctx context.Context, filter any, update any, upsert bool) error {
	ctx, span := StartSpan(ctx, "mongo.update_one", append(mongoAttrs(c.collection, "update_one"), attribute.Int("write.count", 1))...)
	defer span.End()
	_, err := c.collection.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(upsert))
	RecordSpanError(span, err)
	span.SetAttributes(SpanErrorStatus(err))
	return err
}

func mongoAttrs(collection *mongo.Collection, operation string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("operation", operation),
		attribute.String("db.system", "mongodb"),
		attribute.String("db.name", collection.Database().Name()),
		attribute.String("db.collection", collection.Name()),
	}
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
