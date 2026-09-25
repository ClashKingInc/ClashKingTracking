package scripts

import (
	"context"
	"errors"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
)

type closeoutIsolationStore struct {
	*memoryScheduledStore
	legendError   error
	rankedError   error
	rankedCalls   int
	onLegendError func()
}

func (s *closeoutIsolationStore) FinalizeLegendCloseout(ctx context.Context, day time.Time) (int, error) {
	s.legendCloseoutCalls++
	if s.legendError != nil && s.onLegendError != nil {
		s.onLegendError()
	}
	return 0, s.legendError
}

func (s *closeoutIsolationStore) ListRankedGroupTargets(context.Context, int64) ([]string, error) {
	s.rankedCalls++
	return nil, s.rankedError
}

func TestLeagueCloseoutRunsRankedAfterLegendFailure(t *testing.T) {
	now := time.Date(2026, 9, 21, 5, 12, 0, 0, time.UTC)
	legendError := errors.New("Legend closeout failed")
	store := &closeoutIsolationStore{memoryScheduledStore: newMemoryScheduledStore(), legendError: legendError}
	domain := &scheduledDomain{store: store, now: func() time.Time { return now }, waitUntil: func(context.Context, time.Time) error { return nil }}
	app := &platform.App{Stats: platform.NewTracker()}
	if err := domain.runLeagueCloseoutLoop(t.Context(), app); !errors.Is(err, legendError) {
		t.Fatalf("loop error = %v, want Legend failure", err)
	}
	if store.rankedCalls != 1 {
		t.Fatalf("ranked discovery calls = %d, want 1", store.rankedCalls)
	}
	completed, err := store.ScheduledJobCompleted(t.Context(), "ranked_closeout", now)
	if err != nil || !completed {
		t.Fatalf("ranked completion = %v, %v", completed, err)
	}
	if completed, _ := store.ScheduledJobCompleted(t.Context(), "legend_closeout", latestEligibleLegendDay(now)); completed {
		t.Fatal("failed Legend closeout was marked complete")
	}
	if err := domain.runRankedCloseout(t.Context(), app, now); err != nil || store.rankedCalls != 1 {
		t.Fatalf("completed Ranked phase reran: calls=%d err=%v", store.rankedCalls, err)
	}
}

func TestLeagueCloseoutKeepsLegendCompletionWhenRankedFails(t *testing.T) {
	now := time.Date(2026, 9, 21, 5, 12, 0, 0, time.UTC)
	rankedError := errors.New("Ranked discovery failed")
	store := &closeoutIsolationStore{memoryScheduledStore: newMemoryScheduledStore(), rankedError: rankedError}
	domain := &scheduledDomain{store: store, now: func() time.Time { return now }, waitUntil: func(context.Context, time.Time) error { return nil }}
	app := &platform.App{Stats: platform.NewTracker()}
	if err := domain.runLeagueCloseoutLoop(t.Context(), app); !errors.Is(err, rankedError) {
		t.Fatalf("loop error = %v, want Ranked failure", err)
	}
	day := latestEligibleLegendDay(now)
	completed, err := store.ScheduledJobCompleted(t.Context(), "legend_closeout", day)
	if err != nil || !completed {
		t.Fatalf("Legend completion = %v, %v", completed, err)
	}
	if completed, _ := store.ScheduledJobCompleted(t.Context(), "ranked_closeout", now); completed {
		t.Fatal("failed Ranked closeout was marked complete")
	}
	if err := domain.runLegendCloseout(t.Context(), app, day); err != nil || store.legendCloseoutCalls != 1 {
		t.Fatalf("completed Legend phase reran: calls=%d err=%v", store.legendCloseoutCalls, err)
	}
}

func TestLeagueCloseoutStopsBeforeRankedOnCancellation(t *testing.T) {
	now := time.Date(2026, 9, 21, 5, 12, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	store := &closeoutIsolationStore{memoryScheduledStore: newMemoryScheduledStore(), legendError: context.Canceled, onLegendError: cancel}
	domain := &scheduledDomain{store: store, now: func() time.Time { return now }, waitUntil: func(context.Context, time.Time) error { return nil }}
	if err := domain.runLeagueCloseoutLoop(ctx, &platform.App{Stats: platform.NewTracker()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("loop error = %v, want cancellation", err)
	}
	if store.rankedCalls != 0 {
		t.Fatalf("ranked discovery ran after cancellation: %d calls", store.rankedCalls)
	}
}
