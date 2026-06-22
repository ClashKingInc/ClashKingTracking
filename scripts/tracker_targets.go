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

type scriptMemoryTargetSet struct {
	app      *platform.App
	domain   string
	group    string
	interval time.Duration
	loader   scriptTargetLoader
	store    *clashtracker.MemoryTargetStore

	mu      sync.RWMutex
	runners []scriptTargetRunner
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
		store:    clashtracker.NewMemoryTargetStore(targets...),
	}
	app.Logger.Info("loaded tracker targets", "domain", domain, "group", group, "count", len(targets))
	return set, nil
}

func (s *scriptMemoryTargetSet) Store() clashtracker.TargetStore {
	if s == nil || s.store == nil {
		return clashtracker.NewMemoryTargetStore()
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
	remove := make([]string, 0)
	for key, target := range want {
		current, ok := have[key]
		if !ok || current.Value != target.Value {
			add = append(add, target)
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
	if len(add) > 0 {
		if err := s.Add(ctx, add...); err != nil {
			return err
		}
	}
	s.app.Logger.Info("refreshed tracker targets",
		"domain", s.domain,
		"group", s.group,
		"count", len(want),
		"added", len(add),
		"removed", len(remove),
	)
	return nil
}

func (s *scriptMemoryTargetSet) Add(ctx context.Context, targets ...clashtracker.Target) error {
	runners := s.runnerSnapshot()
	if len(runners) > 0 {
		for _, runner := range runners {
			if err := runner.AddTargets(ctx, targets...); err != nil {
				return err
			}
		}
		return nil
	}
	return s.store.Add(ctx, targets...)
}

func (s *scriptMemoryTargetSet) Remove(ctx context.Context, keys ...string) error {
	runners := s.runnerSnapshot()
	if len(runners) > 0 {
		for _, runner := range runners {
			if err := runner.RemoveTargets(ctx, keys...); err != nil {
				return err
			}
		}
		return nil
	}
	return s.store.Remove(ctx, keys...)
}

func (s *scriptMemoryTargetSet) runnerSnapshot() []scriptTargetRunner {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]scriptTargetRunner(nil), s.runners...)
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
