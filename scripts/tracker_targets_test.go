//go:build script_internal_tests

package scripts

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	clashtracker "github.com/clashkinginc/clashy.go/tracker"
)

type fakeScriptTargetRunner struct {
	added   []clashtracker.Target
	removed []string
}

func (r *fakeScriptTargetRunner) AddTargets(_ context.Context, targets ...clashtracker.Target) error {
	r.added = append(r.added, targets...)
	return nil
}

func (r *fakeScriptTargetRunner) RemoveTargets(_ context.Context, keys ...string) error {
	r.removed = append(r.removed, keys...)
	return nil
}

func (r *fakeScriptTargetRunner) reset() {
	r.added = nil
	r.removed = nil
}

func TestScriptMemoryTargetSetRefreshUpdatesBackingStoreWithRunner(t *testing.T) {
	ctx := context.Background()
	loaded := []clashtracker.Target{
		{Key: "#A", Value: "old"},
		{Key: "#B", Value: "same"},
	}
	app := &platform.App{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Stats:  platform.NewTracker(),
	}
	set, err := newScriptMemoryTargetSet(ctx, app, "test", "targets", time.Hour, func(context.Context) ([]clashtracker.Target, error) {
		return append([]clashtracker.Target(nil), loaded...), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := &fakeScriptTargetRunner{}
	set.SetRunner(runner)

	loaded = []clashtracker.Target{
		{Key: "#A", Value: "new"},
		{Key: "#B", Value: "same"},
		{Key: "#C", Value: "added"},
	}
	if err := set.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if len(runner.added) != 2 || len(runner.removed) != 0 {
		t.Fatalf("first refresh runner added=%#v removed=%#v, want two adds and no removes", runner.added, runner.removed)
	}
	if got := mapTargetsByKey(runner.added); got["#A"].Value != "new" || got["#C"].Value != "added" {
		t.Fatalf("first refresh should replace #A and add #C, got %#v", runner.added)
	}

	runner.reset()
	if err := set.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if len(runner.added) != 0 || len(runner.removed) != 0 {
		t.Fatalf("second refresh should not repeat unchanged deltas, added=%#v removed=%#v", runner.added, runner.removed)
	}

	loaded = []clashtracker.Target{{Key: "#B", Value: "same"}}
	if err := set.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if len(runner.added) != 0 || len(runner.removed) != 2 {
		t.Fatalf("remove refresh runner added=%#v removed=%#v, want no adds and two removes", runner.added, runner.removed)
	}

	runner.reset()
	if err := set.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if len(runner.added) != 0 || len(runner.removed) != 0 {
		t.Fatalf("second remove refresh should not repeat unchanged deltas, added=%#v removed=%#v", runner.added, runner.removed)
	}
}

func mapTargetsByKey(targets []clashtracker.Target) map[string]clashtracker.Target {
	out := make(map[string]clashtracker.Target, len(targets))
	for _, target := range targets {
		out[target.Key] = target
	}
	return out
}
