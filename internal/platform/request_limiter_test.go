//go:build platform_internal_tests

package platform

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRequestLimiterRejectsInvalidLimit(t *testing.T) {
	_, err := NewRequestLimiter(0).Acquire(context.Background())
	if !errors.Is(err, ErrInvalidRequestLimit) {
		t.Fatalf("expected ErrInvalidRequestLimit, got %v", err)
	}
}

func TestRequestLimiterCapsInflight(t *testing.T) {
	limiter := NewRequestLimiter(1)
	release, err := limiter.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	limiter.mu.Lock()
	limiter.starts = nil
	limiter.mu.Unlock()

	acquired := make(chan func(), 1)
	go func() {
		release, err := limiter.Acquire(context.Background())
		if err != nil {
			t.Errorf("Acquire returned error: %v", err)
			return
		}
		acquired <- release
	}()

	select {
	case release := <-acquired:
		release()
		t.Fatal("second request acquired while first was still in flight")
	case <-time.After(25 * time.Millisecond):
	}

	release()
	select {
	case release := <-acquired:
		release()
	case <-time.After(200 * time.Millisecond):
		t.Fatal("second request did not acquire after first release")
	}
}

func TestRequestLimiterCapsRollingSecondStarts(t *testing.T) {
	limiter := NewRequestLimiter(2)
	for i := 0; i < 2; i++ {
		release, err := limiter.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		release()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if release, err := limiter.Acquire(ctx); err == nil {
		release()
		t.Fatal("third request acquired inside the same rolling second")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline while rolling window is full, got %v", err)
	}
}
