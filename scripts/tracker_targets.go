package scripts

import (
	"context"
	"sync"
	"time"

	"clashking_tracking/internal/platform"

	clashtracker "github.com/clashkinginc/clashy.go/tracker"
)

type scriptTargetLoader func(context.Context) ([]clashtracker.Target, error)

type scriptTargetRunner interface {
	AddTargets(context.Context, ...clashtracker.Target) error
	RemoveTargets(context.Context, ...string) error
}

type cursorTargetBatchProgress struct {
	mu             sync.Mutex
	total          int
	completed      int
	expectedStores int
	stored         int
	committed      bool
}

func (p *cursorTargetBatchProgress) reset(total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.total = total
	p.completed = 0
	p.expectedStores = 0
	p.stored = 0
	p.committed = false
}

func (p *cursorTargetBatchProgress) completedWithStore() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completed++
	p.expectedStores++
	return p.readyLocked()
}

func (p *cursorTargetBatchProgress) completedWithoutStore() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completed++
	return p.readyLocked()
}

func (p *cursorTargetBatchProgress) storedOne() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stored++
	return p.readyLocked()
}

func (p *cursorTargetBatchProgress) readyLocked() bool {
	if p.committed || p.total <= 0 {
		return false
	}
	if p.completed < p.total || p.stored < p.expectedStores {
		return false
	}
	p.committed = true
	return true
}

type scriptMemoryTargetSet struct {
	app      *platform.App
	domain   string
	group    string
	interval time.Duration
	loader   scriptTargetLoader
	store    *clashtracker.MemoryTagStore

	mu            sync.RWMutex
	runners       []scriptTargetRunner
	progressNames map[string]struct{}
}

func newScriptMemoryTargetSet(
	ctx context.Context,
	app *platform.App,
	domain string,
	group string,
	interval time.Duration,
	loader scriptTargetLoader,
) (*scriptMemoryTargetSet, error) {
	targets, err := loader(ctx)
	if err != nil {
		return nil, err
	}
	set := &scriptMemoryTargetSet{
		app:      app,
		domain:   domain,
		group:    group,
		interval: interval,
		loader:   loader,
		store:    clashtracker.NewMemoryTagStore(targets...),
	}
	app.Logger.Info("loaded tracker targets", "domain", domain, "group", group, "count", len(targets))
	return set, nil
}

func trackingProgressName(domain, group string) string {
	if group == "" {
		return domain
	}
	return domain + "." + group
}

func (s *scriptMemoryTargetSet) ProgressName() string {
	if s == nil {
		return ""
	}
	return trackingProgressName(s.domain, s.group)
}

func (s *scriptMemoryTargetSet) RegisterProgress(ctx context.Context, name string) error {
	if s == nil || name == "" {
		return nil
	}
	count, err := s.Len(ctx)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.progressNames == nil {
		s.progressNames = make(map[string]struct{})
	}
	s.progressNames[name] = struct{}{}
	s.mu.Unlock()
	s.app.Stats.SetTrackingTargets(name, count)
	return nil
}

func (s *scriptMemoryTargetSet) RecordTracked(names ...string) {
	if s == nil {
		return
	}
	if len(names) == 0 {
		names = []string{s.ProgressName()}
	}
	for _, name := range names {
		if name == "" {
			continue
		}
		s.app.Stats.RecordTrackedTarget(name)
	}
}

func (s *scriptMemoryTargetSet) Len(ctx context.Context) (int, error) {
	if s == nil || s.store == nil {
		return 0, nil
	}
	if counter, ok := any(s.store).(clashtracker.TargetCounter); ok {
		return counter.Count(ctx)
	}
	targets, err := s.store.List(ctx)
	if err != nil {
		return 0, err
	}
	return len(targets), nil
}

func (s *scriptMemoryTargetSet) Tags() clashtracker.TagStore {
	if s == nil || s.store == nil {
		return clashtracker.NewMemoryTagStore()
	}
	return s.store
}

func (s *scriptMemoryTargetSet) TargetPager() clashtracker.TargetPager {
	if s == nil || s.store == nil {
		return clashtracker.NewMemoryTagStore()
	}
	return s.store
}

func (s *scriptMemoryTargetSet) SetRunner(runner scriptTargetRunner) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if runner != nil {
		s.runners = append(s.runners, runner)
	}
}

func (s *scriptMemoryTargetSet) Run(ctx context.Context) {
	if s == nil || s.interval <= 0 || s.loader == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Refresh(ctx); err != nil {
				s.app.Logger.Error("tracker target refresh failed", "domain", s.domain, "group", s.group, "err", err)
				s.app.Stats.SetReady(s.domain, false, err.Error())
			}
		}
	}
}

func (s *scriptMemoryTargetSet) Refresh(ctx context.Context) error {
	loaded, err := s.loader(ctx)
	if err != nil {
		return err
	}
	want := targetMap(loaded)
	haveList, err := s.store.List(ctx)
	if err != nil {
		return err
	}
	have := targetMap(haveList)
	add := make([]clashtracker.Target, 0)
	update := make([]clashtracker.Target, 0)
	remove := make([]string, 0)
	for key, target := range want {
		current, ok := have[key]
		if !ok {
			add = append(add, target)
			continue
		}
		if current.Value != target.Value {
			update = append(update, target)
		}
	}
	for key := range have {
		if _, ok := want[key]; !ok {
			remove = append(remove, key)
		}
	}
	if len(remove) > 0 {
		if err := s.Remove(ctx, remove...); err != nil {
			return err
		}
	}
	if len(add) > 0 || len(update) > 0 {
		targets := make([]clashtracker.Target, 0, len(add)+len(update))
		targets = append(targets, add...)
		targets = append(targets, update...)
		if err := s.Add(ctx, targets...); err != nil {
			return err
		}
	}
	s.app.Logger.Info("refreshed tracker targets",
		"domain", s.domain,
		"group", s.group,
		"count", len(want),
		"have_count", len(have),
		"added", len(add),
		"updated", len(update),
		"removed", len(remove),
		"added_sample", sampleTargetKeys(add, 5),
		"updated_sample", sampleTargetKeys(update, 5),
		"removed_sample", sampleStrings(remove, 5),
	)
	s.updateProgressTargets(len(want))
	return nil
}

func (s *scriptMemoryTargetSet) Add(ctx context.Context, targets ...clashtracker.Target) error {
	if err := s.store.Add(ctx, targets...); err != nil {
		return err
	}
	runners := s.runnerSnapshot()
	for _, runner := range runners {
		if err := runner.AddTargets(ctx, targets...); err != nil {
			return err
		}
	}
	return nil
}

func (s *scriptMemoryTargetSet) Remove(ctx context.Context, keys ...string) error {
	if err := s.store.Remove(ctx, keys...); err != nil {
		return err
	}
	runners := s.runnerSnapshot()
	for _, runner := range runners {
		if err := runner.RemoveTargets(ctx, keys...); err != nil {
			return err
		}
	}
	return nil
}

func (s *scriptMemoryTargetSet) runnerSnapshot() []scriptTargetRunner {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]scriptTargetRunner(nil), s.runners...)
}

func (s *scriptMemoryTargetSet) updateProgressTargets(count int) {
	if s == nil {
		return
	}
	s.mu.RLock()
	names := make([]string, 0, len(s.progressNames))
	for name := range s.progressNames {
		names = append(names, name)
	}
	s.mu.RUnlock()
	for _, name := range names {
		s.app.Stats.SetTrackingTargets(name, count)
	}
}

func targetMap(targets []clashtracker.Target) map[string]clashtracker.Target {
	out := make(map[string]clashtracker.Target, len(targets))
	for _, target := range targets {
		key := target.Key
		if key == "" {
			key = target.Value
		}
		if key == "" {
			continue
		}
		target.Key = key
		out[key] = target
	}
	return out
}

func sampleTargetKeys(targets []clashtracker.Target, limit int) []string {
	if limit <= 0 || len(targets) == 0 {
		return nil
	}
	if len(targets) < limit {
		limit = len(targets)
	}
	out := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, targets[i].Key)
	}
	return out
}

func sampleStrings(values []string, limit int) []string {
	if limit <= 0 || len(values) == 0 {
		return nil
	}
	if len(values) < limit {
		limit = len(values)
	}
	return append([]string(nil), values[:limit]...)
}
