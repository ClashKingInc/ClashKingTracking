package platform

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// ErrNotFound normalizes the "no documents" case across Mongo and mock stores.
var ErrNotFound = errors.New("repository: not found")

type FindOptions struct {
	// These mirror the subset of Mongo find options the rewrites actually need.
	Projection any
	Sort       any
	Limit      int64
}

type Collection interface {
	AggregateAll(ctx context.Context, pipeline any) ([]bson.M, error)
	BulkWrite(ctx context.Context, models []mongo.WriteModel, ordered bool) error
	DeleteMany(ctx context.Context, filter any) error
	DeleteOne(ctx context.Context, filter any) error
	DistinctStrings(ctx context.Context, field string, filter any) ([]string, error)
	FindAll(ctx context.Context, filter any, opts FindOptions) ([]bson.M, error)
	FindOne(ctx context.Context, filter any) (bson.M, error)
	InsertMany(ctx context.Context, docs []any, ordered bool) error
	InsertOne(ctx context.Context, doc any) error
	UpdateMany(ctx context.Context, filter any, update any) error
	UpdateOne(ctx context.Context, filter any, update any, upsert bool) error
}

type Database interface {
	Collection(name string) Collection
}

type Store interface {
	Close(ctx context.Context) error
	IsMock() bool
	Static() Database
	Stats() Database
}
