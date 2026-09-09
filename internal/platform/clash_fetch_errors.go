package platform

import (
	"context"
	"errors"
	"io"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
)

const (
	ClashGatewayTimeoutRetryDelay = 500 * time.Millisecond
	ClashGatewayTimeoutMaxRetries = 3
	ClashThrottledRetryDelay      = time.Second
	ClashThrottledMaxRetries      = 3
	ClashUnavailableRetryDelay    = 60 * time.Second
)

type ClashFetchRetry struct {
	RetryAfter time.Duration
	MaxRetries int
}

func ClashFetchRetryPolicy(err error) (ClashFetchRetry, bool) {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ClashFetchRetry{
			RetryAfter: ClashGatewayTimeoutRetryDelay,
			MaxRetries: ClashGatewayTimeoutMaxRetries,
		}, true
	}

	var gateway *clashy.GatewayError
	if errors.As(err, &gateway) {
		switch gateway.Status {
		case 0:
			return ClashFetchRetry{
				RetryAfter: ClashUnavailableRetryDelay,
			}, true
		case 504:
			return ClashFetchRetry{
				RetryAfter: ClashGatewayTimeoutRetryDelay,
				MaxRetries: ClashGatewayTimeoutMaxRetries,
			}, true
		}
	}

	var maintenance *clashy.Maintenance
	if errors.As(err, &maintenance) {
		return ClashFetchRetry{
			RetryAfter: ClashUnavailableRetryDelay,
		}, true
	}

	var httpErr *clashy.HTTPException
	if errors.As(err, &httpErr) && httpErr.Status == 429 {
		return ClashFetchRetry{
			RetryAfter: ClashThrottledRetryDelay,
			MaxRetries: ClashThrottledMaxRetries,
		}, true
	}

	return ClashFetchRetry{}, false
}

func RetryClashFetch[T any](ctx context.Context, gate *AvailabilityGate, fetch func(context.Context) (T, error)) (T, error) {
	var zero T
	retries := 0
	for {
		if err := gate.Wait(ctx); err != nil {
			return zero, err
		}
		value, err := fetch(ctx)
		if err == nil {
			return value, nil
		}
		if gate.Observe(err) {
			continue
		}
		decision, ok := ClashFetchRetryPolicy(err)
		if !ok {
			return zero, err
		}
		if decision.MaxRetries > 0 {
			if retries >= decision.MaxRetries {
				return zero, err
			}
			retries++
		}
		if err := sleepContext(ctx, decision.RetryAfter); err != nil {
			return zero, err
		}
	}
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
