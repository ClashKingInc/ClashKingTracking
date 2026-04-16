package platform

import (
	"context"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Operation struct {
	Database   string
	Collection string
	Kind       string
	Filter     any
	Update     any
	Document   any
	Documents  []any
	Models     []mongo.WriteModel
}

type MockStore struct {
	stats  *mockDatabase
	static *mockDatabase
}

func NewMockStore() *MockStore {
	return &MockStore{
		stats:  newMockDatabase("stats"),
		static: newMockDatabase("static"),
	}
}

func (s *MockStore) Close(context.Context) error { return nil }
func (s *MockStore) IsMock() bool                { return true }
func (s *MockStore) Static() Database            { return s.static }
func (s *MockStore) Stats() Database             { return s.stats }

type mockDatabase struct {
	name        string
	mu          sync.RWMutex
	collections map[string]*mockCollection
}

func newMockDatabase(name string) *mockDatabase {
	return &mockDatabase{name: name, collections: make(map[string]*mockCollection)}
}

func (d *mockDatabase) Collection(name string) Collection {
	d.mu.Lock()
	defer d.mu.Unlock()
	coll, ok := d.collections[name]
	if !ok {
		// Reuse the same in-memory collection so tests can seed docs and inspect recorded operations.
		coll = &mockCollection{}
		d.collections[name] = coll
	}
	return coll
}

type mockCollection struct {
	mu         sync.RWMutex
	docs       []bson.M
	operations []Operation
}

func (c *mockCollection) AggregateAll(context.Context, any) ([]bson.M, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneDocs(c.docs), nil
}

func (c *mockCollection) BulkWrite(_ context.Context, models []mongo.WriteModel, ordered bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Dry-run tests care that a bulk write was requested, not that Mongo semantics are replayed locally.
	c.operations = append(c.operations, Operation{Kind: "bulk_write", Models: models, Update: ordered})
	return nil
}

func (c *mockCollection) DeleteMany(_ context.Context, filter any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations = append(c.operations, Operation{Kind: "delete_many", Filter: filter})
	return nil
}

func (c *mockCollection) DeleteOne(_ context.Context, filter any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations = append(c.operations, Operation{Kind: "delete_one", Filter: filter})
	return nil
}

func (c *mockCollection) DistinctStrings(_ context.Context, field string, _ any) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	seen := make(map[string]struct{})
	var out []string
	for _, doc := range c.docs {
		value, ok := dottedLookup(doc, field).(string)
		if !ok || value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}

func (c *mockCollection) FindAll(_ context.Context, _ any, _ FindOptions) ([]bson.M, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneDocs(c.docs), nil
}

func (c *mockCollection) FindOne(_ context.Context, _ any) (bson.M, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.docs) == 0 {
		return nil, ErrNotFound
	}
	return cloneDoc(c.docs[0]), nil
}

func (c *mockCollection) InsertMany(_ context.Context, docs []any, ordered bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations = append(c.operations, Operation{Kind: "insert_many", Documents: docs, Update: ordered})
	return nil
}

func (c *mockCollection) InsertOne(_ context.Context, doc any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations = append(c.operations, Operation{Kind: "insert_one", Document: doc})
	return nil
}

func (c *mockCollection) UpdateMany(_ context.Context, filter any, update any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations = append(c.operations, Operation{Kind: "update_many", Filter: filter, Update: update})
	return nil
}

func (c *mockCollection) UpdateOne(_ context.Context, filter any, update any, upsert bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.operations = append(c.operations, Operation{Kind: "update_one", Filter: filter, Update: bson.M{"update": update, "upsert": upsert}})
	return nil
}

func cloneDocs(in []bson.M) []bson.M {
	out := make([]bson.M, 0, len(in))
	for _, doc := range in {
		out = append(out, cloneDoc(doc))
	}
	return out
}

func cloneDoc(in bson.M) bson.M {
	out := make(bson.M, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func dottedLookup(doc bson.M, path string) any {
	current := any(doc)
	for _, part := range strings.Split(path, ".") {
		// Distinct/find helpers use dotted keys against nested bson.M test fixtures.
		next, ok := current.(bson.M)
		if !ok {
			return nil
		}
		current = next[part]
	}
	return current
}
