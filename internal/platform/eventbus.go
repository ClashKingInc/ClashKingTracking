package platform

import (
	"fmt"
	"sync"
	"time"
)

type Event struct {
	Topic     string                 `json:"topic"`
	ClanTag   string                 `json:"clan_tag,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Value     map[string]interface{} `json:"value"`
}

type Filter struct {
	Topics map[string]struct{}
	Clans  map[string]struct{}
}

type Snapshot struct {
	Subscribers int              `json:"subscribers"`
	ByTopic     map[string]int64 `json:"by_topic"`
	Recent      []Event          `json:"recent"`
	QueueDepths map[string]int   `json:"queue_depths"`
}

type subscriber struct {
	id     uint64
	filter Filter
	ch     chan Event
}

type Bus struct {
	mu          sync.RWMutex
	nextID      uint64
	bufferSize  int
	recentLimit int
	subs        map[uint64]subscriber
	byTopic     map[string]int64
	recent      []Event
}

func NewBus(bufferSize, recentLimit int) *Bus {
	return &Bus{
		bufferSize:  bufferSize,
		recentLimit: recentLimit,
		subs:        make(map[uint64]subscriber),
		byTopic:     make(map[string]int64),
	}
}

func (b *Bus) Publish(event Event) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	b.mu.Lock()
	b.byTopic[event.Topic]++
	b.recent = append(b.recent, event)
	if len(b.recent) > b.recentLimit {
		b.recent = append([]Event(nil), b.recent[len(b.recent)-b.recentLimit:]...)
	}
	subs := make([]subscriber, 0, len(b.subs))
	for _, sub := range b.subs {
		subs = append(subs, sub)
	}
	b.mu.Unlock()

	// Copy the subscriber list under lock, then fan out without holding the mutex.
	for _, sub := range subs {
		if !matches(sub.filter, event) {
			continue
		}
		select {
		case sub.ch <- event:
		default:
		}
	}
}

func (b *Bus) Subscribe(filter Filter) (<-chan Event, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	sub := subscriber{
		id:     b.nextID,
		filter: filter,
		ch:     make(chan Event, b.bufferSize),
	}
	b.subs[sub.id] = sub
	return sub.ch, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if existing, ok := b.subs[sub.id]; ok {
			delete(b.subs, sub.id)
			close(existing.ch)
		}
	}
}

func (b *Bus) Snapshot() Snapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()
	depths := make(map[string]int, len(b.subs))
	for id, sub := range b.subs {
		depths[fmt.Sprintf("subscriber_%d", id)] = len(sub.ch)
	}
	out := Snapshot{
		Subscribers: len(b.subs),
		ByTopic:     make(map[string]int64, len(b.byTopic)),
		Recent:      append([]Event(nil), b.recent...),
		QueueDepths: depths,
	}
	for topic, count := range b.byTopic {
		out.ByTopic[topic] = count
	}
	return out
}

func matches(filter Filter, event Event) bool {
	if len(filter.Topics) > 0 {
		if _, ok := filter.Topics[event.Topic]; !ok {
			return false
		}
	}
	if len(filter.Clans) > 0 {
		if _, ok := filter.Clans[event.ClanTag]; !ok {
			return false
		}
	}
	return true
}
