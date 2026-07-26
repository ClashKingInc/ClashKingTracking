package platform

import (
	"context"
	"errors"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
)

const (
	ClashGatewayTimeoutRetryDelay = 5 * time.Second
	ClashGatewayTimeoutMaxRetries = 3
	ClashUnavailableRetryDelay    = 60 * time.Second
)

type ClashFetchRetry struct {
	RetryAfter time.Duration
	MaxRetries int
}

func ClashFetchRetryPolicy(err error) (ClashFetchRetry, bool) {
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

	return ClashFetchRetry{}, false
}

func RetryClashFetch[T any](ctx context.Context, fetch func(context.Context) (T, error)) (T, error) {
	var zero T
	retries := 0
	for {
		value, err := fetch(ctx)
		if err == nil {
			return value, nil
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
