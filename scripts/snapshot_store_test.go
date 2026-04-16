package scripts

import (
	"context"
	"sync"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	clashevents "github.com/clashkinginc/clashy.go/events"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type stubStore struct {
	stats *stubDatabase
}

func (s *stubStore) Close(context.Context) error { return nil }
func (s *stubStore) IsMock() bool                { return true }
func (s *stubStore) Static() platform.Database   { return &stubDatabase{} }
func (s *stubStore) Stats() platform.Database    { return s.stats }

type stubDatabase struct {
	collection *stubCollection
}

func (d *stubDatabase) Collection(string) platform.Collection {
	if d.collection == nil {
		d.collection = &stubCollection{}
	}
	return d.collection
}

type stubCollection struct {
	mu       sync.Mutex
	docs     []bson.M
	findAlls int
}

func (c *stubCollection) AggregateAll(context.Context, any) ([]bson.M, error) { return nil, nil }
func (c *stubCollection) BulkWrite(context.Context, []mongo.WriteModel, bool) error {
	return nil
}
func (c *stubCollection) DeleteMany(context.Context, any) error { return nil }
func (c *stubCollection) DeleteOne(context.Context, any) error  { return nil }
func (c *stubCollection) DistinctStrings(context.Context, string, any) ([]string, error) {
	return nil, nil
}
func (c *stubCollection) FindAll(_ context.Context, filter any, _ platform.FindOptions) ([]bson.M, error) {
	c.mu.Lock()
	c.findAlls++
	c.mu.Unlock()

	match := map[string]struct{}{}
	if query, ok := filter.(bson.M); ok {
		if tagFilter, ok := query["tag"].(bson.M); ok {
			if values, ok := tagFilter["$in"].([]string); ok {
				for _, tag := range values {
					match[tag] = struct{}{}
				}
			}
		}
	}

	var out []bson.M
	for _, doc := range c.docs {
		tag, _ := doc["tag"].(string)
		if len(match) > 0 {
			if _, ok := match[tag]; !ok {
				continue
			}
		}
		out = append(out, doc)
	}
	return out, nil
}
func (c *stubCollection) FindOne(context.Context, any) (bson.M, error) {
	return nil, platform.ErrNotFound
}
func (c *stubCollection) InsertMany(context.Context, []any, bool) error { return nil }
func (c *stubCollection) InsertOne(context.Context, any) error          { return nil }
func (c *stubCollection) UpdateMany(context.Context, any, any) error    { return nil }
func (c *stubCollection) UpdateOne(context.Context, any, any, bool) error {
	return nil
}

func TestAllClansSnapshotStoreBatchesAndCaches(t *testing.T) {
	collection := &stubCollection{
		docs: []bson.M{
			{
				"tag":  "#AAA",
				"data": bson.M{"tag": "#AAA", "name": "Alpha"},
				"records": bson.M{
					"clanPoints":   bson.M{"value": 10, "time": 1},
					"warWinStreak": bson.M{"value": 2, "time": 1},
				},
			},
			{
				"tag":  "#BBB",
				"data": bson.M{"tag": "#BBB", "name": "Beta"},
				"records": bson.M{
					"clanPoints":   bson.M{"value": 20, "time": 2},
					"warWinStreak": bson.M{"value": 3, "time": 2},
				},
			},
		},
	}
	store := newAllClansSnapshotStore(&stubStore{stats: &stubDatabase{collection: collection}}, 4)
	store.batchWindow = 5 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, tag := range []string{"#AAA", "#BBB"} {
		wg.Add(1)
		go func(tag string) {
			defer wg.Done()
			snapshot, err := store.Load(ctx, "clan", tag)
			if err != nil {
				t.Errorf("Load(%s) returned error: %v", tag, err)
				return
			}
			if snapshot.Key != tag {
				t.Errorf("unexpected key: %s", snapshot.Key)
			}
		}(tag)
	}
	wg.Wait()

	if collection.findAlls != 1 {
		t.Fatalf("expected one batched FindAll call, got %d", collection.findAlls)
	}

	if _, err := store.Load(ctx, "clan", "#AAA"); err != nil {
		t.Fatalf("cached Load returned error: %v", err)
	}
	if collection.findAlls != 1 {
		t.Fatalf("expected cache hit to avoid more FindAll calls, got %d", collection.findAlls)
	}

	records, ok := store.Records("#AAA")
	if !ok || records.ClanPoints.Value != 10 || records.WarWinStreak.Value != 2 {
		t.Fatalf("unexpected cached records: %+v", records)
	}
}

func TestAllClansSnapshotStoreSaveAndDelete(t *testing.T) {
	store := newAllClansSnapshotStore(&stubStore{stats: &stubDatabase{collection: &stubCollection{}}}, 2)
	if err := store.Save(context.Background(), clashevents.Snapshot{Kind: "clan", Key: "#ABC", Data: []byte(`{"tag":"#ABC"}`)}); err != nil {
		t.Fatalf("Save returned error: %v", err)
	}
	if _, err := store.Load(context.Background(), "clan", "#ABC"); err != nil {
		t.Fatalf("expected saved snapshot to load: %v", err)
	}
	if err := store.Delete(context.Background(), "clan", "#ABC"); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	if _, err := store.Load(context.Background(), "clan", "#ABC"); err != clashevents.ErrSnapshotNotFound {
		t.Fatalf("expected not found after delete, got %v", err)
	}
}
