package scripts

import (
	"container/list"
	"context"
	"encoding/json"
	"sync"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"

	clashevents "github.com/clashkinginc/clashy.go/events"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type cacheEntry struct {
	key string
	raw []byte
}

type boundedSnapshotCache struct {
	size    int
	mu      sync.Mutex
	items   map[string]*list.Element
	evictor *list.List
}

func newBoundedSnapshotCache(size int) *boundedSnapshotCache {
	return &boundedSnapshotCache{
		size:    size,
		items:   make(map[string]*list.Element),
		evictor: list.New(),
	}
}

func (c *boundedSnapshotCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.evictor.MoveToBack(element)
	entry := element.Value.(*cacheEntry)
	return append([]byte(nil), entry.raw...), true
}

func (c *boundedSnapshotCache) Put(key string, raw []byte) {
	if c == nil || c.size <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[key]; ok {
		c.evictor.MoveToBack(element)
		element.Value.(*cacheEntry).raw = append([]byte(nil), raw...)
		return
	}
	element := c.evictor.PushBack(&cacheEntry{key: key, raw: append([]byte(nil), raw...)})
	c.items[key] = element
	for len(c.items) > c.size {
		front := c.evictor.Front()
		if front == nil {
			break
		}
		entry := front.Value.(*cacheEntry)
		delete(c.items, entry.key)
		c.evictor.Remove(front)
	}
}

func (c *boundedSnapshotCache) Delete(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return
	}
	delete(c.items, key)
	c.evictor.Remove(element)
}

type snapshotLoadResult struct {
	raw []byte
	err error
}

type snapshotLoadRequest struct {
	key  string
	resp chan snapshotLoadResult
}

type clanRecordValue struct {
	Value int `bson:"value" json:"value"`
	Time  int `bson:"time" json:"time"`
}

type clanRecords struct {
	ClanPoints   clanRecordValue `bson:"clanPoints" json:"clanPoints"`
	WarWinStreak clanRecordValue `bson:"warWinStreak" json:"warWinStreak"`
}

type allClansCacheEntry struct {
	key     string
	raw     []byte
	records clanRecords
}

type allClansSnapshotCache struct {
	size    int
	mu      sync.Mutex
	items   map[string]*list.Element
	evictor *list.List
}

func newAllClansSnapshotCache(size int) *allClansSnapshotCache {
	return &allClansSnapshotCache{
		size:    size,
		items:   make(map[string]*list.Element),
		evictor: list.New(),
	}
}

func (c *allClansSnapshotCache) Get(key string) ([]byte, clanRecords, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return nil, clanRecords{}, false
	}
	c.evictor.MoveToBack(element)
	entry := element.Value.(*allClansCacheEntry)
	return append([]byte(nil), entry.raw...), entry.records, true
}

func (c *allClansSnapshotCache) Put(key string, raw []byte, records clanRecords) {
	if c == nil || c.size <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[key]; ok {
		c.evictor.MoveToBack(element)
		entry := element.Value.(*allClansCacheEntry)
		entry.raw = append([]byte(nil), raw...)
		entry.records = records
		return
	}
	element := c.evictor.PushBack(&allClansCacheEntry{
		key:     key,
		raw:     append([]byte(nil), raw...),
		records: records,
	})
	c.items[key] = element
	for len(c.items) > c.size {
		front := c.evictor.Front()
		if front == nil {
			break
		}
		entry := front.Value.(*allClansCacheEntry)
		delete(c.items, entry.key)
		c.evictor.Remove(front)
	}
}

func (c *allClansSnapshotCache) PutRaw(key string, raw []byte) {
	if c == nil || c.size <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[key]; ok {
		c.evictor.MoveToBack(element)
		element.Value.(*allClansCacheEntry).raw = append([]byte(nil), raw...)
		return
	}
	element := c.evictor.PushBack(&allClansCacheEntry{
		key: key,
		raw: append([]byte(nil), raw...),
	})
	c.items[key] = element
	for len(c.items) > c.size {
		front := c.evictor.Front()
		if front == nil {
			break
		}
		entry := front.Value.(*allClansCacheEntry)
		delete(c.items, entry.key)
		c.evictor.Remove(front)
	}
}

func (c *allClansSnapshotCache) Records(key string) (clanRecords, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return clanRecords{}, false
	}
	c.evictor.MoveToBack(element)
	return element.Value.(*allClansCacheEntry).records, true
}

func (c *allClansSnapshotCache) SetRecords(key string, records clanRecords) {
	if c == nil || c.size <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[key]; ok {
		c.evictor.MoveToBack(element)
		element.Value.(*allClansCacheEntry).records = records
		return
	}
	element := c.evictor.PushBack(&allClansCacheEntry{
		key:     key,
		records: records,
	})
	c.items[key] = element
	for len(c.items) > c.size {
		front := c.evictor.Front()
		if front == nil {
			break
		}
		entry := front.Value.(*allClansCacheEntry)
		delete(c.items, entry.key)
		c.evictor.Remove(front)
	}
}

func (c *allClansSnapshotCache) Delete(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return
	}
	delete(c.items, key)
	c.evictor.Remove(element)
}

type allClansSnapshotStore struct {
	store       platform.Store
	cache       *allClansSnapshotCache
	requests    chan snapshotLoadRequest
	batchWindow time.Duration
}

func newAllClansSnapshotStore(store platform.Store, cacheSize int) *allClansSnapshotStore {
	s := &allClansSnapshotStore{
		store:       store,
		cache:       newAllClansSnapshotCache(cacheSize),
		requests:    make(chan snapshotLoadRequest, cacheSize),
		batchWindow: 2 * time.Millisecond,
	}
	go s.runLoader()
	return s
}

func (s *allClansSnapshotStore) Load(ctx context.Context, kind, key string) (clashevents.Snapshot, error) {
	if raw, _, ok := s.cache.Get(key); ok {
		return clashevents.Snapshot{Kind: kind, Key: key, Data: raw}, nil
	}
	response := make(chan snapshotLoadResult, 1)
	request := snapshotLoadRequest{key: key, resp: response}
	select {
	case s.requests <- request:
	case <-ctx.Done():
		return clashevents.Snapshot{}, ctx.Err()
	}
	select {
	case result := <-response:
		if result.err != nil {
			return clashevents.Snapshot{}, result.err
		}
		return clashevents.Snapshot{Kind: kind, Key: key, Data: result.raw}, nil
	case <-ctx.Done():
		return clashevents.Snapshot{}, ctx.Err()
	}
}

func (s *allClansSnapshotStore) Save(_ context.Context, snapshot clashevents.Snapshot) error {
	s.cache.PutRaw(snapshot.Key, snapshot.Data)
	return nil
}

func (s *allClansSnapshotStore) Delete(_ context.Context, _, key string) error {
	s.cache.Delete(key)
	return nil
}

func (s *allClansSnapshotStore) Records(key string) (clanRecords, bool) {
	return s.cache.Records(key)
}

func (s *allClansSnapshotStore) SetRecords(key string, records clanRecords) {
	s.cache.SetRecords(key, records)
}

func (s *allClansSnapshotStore) runLoader() {
	pending := make(map[string][]chan snapshotLoadResult)
	var timer *time.Timer
	var timerCh <-chan time.Time
	flush := func() {
		if len(pending) == 0 {
			return
		}
		waiters := pending
		pending = make(map[string][]chan snapshotLoadResult)
		timer = nil
		timerCh = nil

		keys := make([]string, 0, len(waiters))
		for key := range waiters {
			if raw, _, ok := s.cache.Get(key); ok {
				for _, waiter := range waiters[key] {
					waiter <- snapshotLoadResult{raw: raw}
				}
				delete(waiters, key)
				continue
			}
			keys = append(keys, key)
		}

		found := make(map[string][]byte, len(keys))
		if len(keys) > 0 {
			docs, err := s.store.Stats().Collection("all_clans").FindAll(
				context.Background(),
				bson.M{"tag": bson.M{"$in": keys}},
				platform.FindOptions{Projection: bson.M{"tag": 1, "data": 1, "records": 1}},
			)
			if err != nil {
				for _, chans := range waiters {
					for _, waiter := range chans {
						waiter <- snapshotLoadResult{err: err}
					}
				}
				return
			}
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
				var records clanRecords
				if rawRecords, ok := doc["records"].(bson.M); ok {
					_ = decodeBSON(rawRecords, &records)
				}
				found[tag] = payload
				s.cache.Put(tag, payload, records)
			}
		}

		for key, chans := range waiters {
			raw, ok := found[key]
			result := snapshotLoadResult{raw: raw, err: clashevents.ErrSnapshotNotFound}
			if ok {
				result.err = nil
			}
			for _, waiter := range chans {
				waiter <- result
			}
		}
	}

	for {
		select {
		case request := <-s.requests:
			pending[request.key] = append(pending[request.key], request.resp)
			if timer == nil {
				timer = time.NewTimer(s.batchWindow)
				timerCh = timer.C
			}
		case <-timerCh:
			flush()
		}
	}
}

type redisSnapshotStore struct {
	client *redis.Client
	cache  *boundedSnapshotCache
	prefix string
}

func newRedisSnapshotStore(client *redis.Client, prefix string, cacheSize int) *redisSnapshotStore {
	return &redisSnapshotStore{
		client: client,
		cache:  newBoundedSnapshotCache(cacheSize),
		prefix: prefix,
	}
}

func (s *redisSnapshotStore) Load(ctx context.Context, kind, key string) (clashevents.Snapshot, error) {
	if raw, ok := s.cache.Get(key); ok {
		return clashevents.Snapshot{Kind: kind, Key: key, Data: raw}, nil
	}
	if s.client == nil {
		return clashevents.Snapshot{}, clashevents.ErrSnapshotNotFound
	}
	value, err := s.client.Get(ctx, s.prefix+key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return clashevents.Snapshot{}, clashevents.ErrSnapshotNotFound
		}
		return clashevents.Snapshot{}, err
	}
	raw, err := utils.Decompress(value)
	if err != nil {
		return clashevents.Snapshot{}, err
	}
	s.cache.Put(key, raw)
	return clashevents.Snapshot{Kind: kind, Key: key, Data: raw}, nil
}

func (s *redisSnapshotStore) Save(ctx context.Context, snapshot clashevents.Snapshot) error {
	s.cache.Put(snapshot.Key, snapshot.Data)
	if s.client == nil {
		return nil
	}
	return s.client.Set(ctx, s.prefix+snapshot.Key, utils.Compress(snapshot.Data), 0).Err()
}

func (s *redisSnapshotStore) Delete(ctx context.Context, _, key string) error {
	s.cache.Delete(key)
	if s.client == nil {
		return nil
	}
	return s.client.Del(ctx, s.prefix+key).Err()
}
