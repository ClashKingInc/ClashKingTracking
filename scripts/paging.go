package scripts

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"clashking_tracking/internal/platform"

	clashy "github.com/clashkinginc/clashy.go"
)

func trackingProgressName(domain, group string) string {
	if group == "" {
		return domain
	}
	return domain + "." + group
}

func sleepOrDone(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func newTrackingLimiter(requestsPerSecond int) (*clashy.Limiter, error) {
	return clashy.NewLimiter(requestsPerSecond, platform.RequestConcurrency(requestsPerSecond))
}

func bulkFetchWorkerCount(requestsPerSecond, maxInFlight int) int {
	workers := requestsPerSecond
	if workers <= 0 {
		workers = 1
	}
	if maxInFlight > 0 && workers > maxInFlight {
		workers = maxInFlight
	}
	return workers
}

// A broad SQL-backed scan can safely leave these targets for its next full
// pass. Other failures still stop the process so configuration and data-shape
// problems remain visible.
func isDeferredBulkFetch(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var gateway *clashy.GatewayError
	if errors.As(err, &gateway) && gateway.HTTPException != nil && gateway.Status == 504 {
		return true
	}
	var httpErr *clashy.HTTPException
	return errors.As(err, &httpErr) && httpErr.Status == 429
}

// runBounded processes one application-owned page with a fixed worker count.
// Paging, retry policy, comparison, and persistence stay with the calling script.
func runBounded[T any](ctx context.Context, workers int, items []T, handle func(context.Context, T) error) error {
	if workers <= 0 {
		return errors.New("bounded workers must be greater than zero")
	}
	if len(items) == 0 {
		return nil
	}
	if workers > len(items) {
		workers = len(items)
	}

	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan T)
	var wg sync.WaitGroup
	var once sync.Once
	var firstErr error
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				if err := handle(workCtx, item); err != nil {
					once.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
			}
		}()
	}

sendLoop:
	for _, item := range items {
		select {
		case jobs <- item:
		case <-workCtx.Done():
			break sendLoop
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	return ctx.Err()
}

func retryLimitedClashFetch[T any](
	ctx context.Context,
	app *platform.App,
	limiter *clashy.Limiter,
	fetch func(context.Context) (T, error),
) (T, error) {
	return platform.RetryClashFetch(ctx, app.Availability, func(fetchCtx context.Context) (T, error) {
		release, err := limiter.Acquire(fetchCtx)
		if err != nil {
			var zero T
			return zero, err
		}
		defer release()
		return fetch(fetchCtx)
	})
}
