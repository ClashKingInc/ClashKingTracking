package platform

import (
	"context"
	"errors"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
	clashtracker "github.com/clashkinginc/clashy.go/tracker"
)

const (
	ClashGatewayTimeoutRetryDelay = 5 * time.Second
	ClashGatewayTimeoutMaxRetries = 3
	ClashUnavailableRetryDelay    = 60 * time.Second
)

func ClashFetchErrorDecision(err error) (clashtracker.FetchErrorDecision, bool) {
	var gateway *clashy.GatewayError
	if errors.As(err, &gateway) {
		switch gateway.Status {
		case 0:
			return clashtracker.FetchErrorDecision{
				Action:     clashtracker.FetchErrorRetry,
				RetryAfter: ClashUnavailableRetryDelay,
			}, true
		case 504:
			return clashtracker.FetchErrorDecision{
				Action:     clashtracker.FetchErrorRetry,
				RetryAfter: ClashGatewayTimeoutRetryDelay,
				MaxRetries: ClashGatewayTimeoutMaxRetries,
			}, true
		}
	}

	var maintenance *clashy.Maintenance
	if errors.As(err, &maintenance) {
		return clashtracker.FetchErrorDecision{
			Action:     clashtracker.FetchErrorRetry,
			RetryAfter: ClashUnavailableRetryDelay,
		}, true
	}

	return clashtracker.FetchErrorDecision{}, false
}

func RetryClashFetch[T any](ctx context.Context, fetch func(context.Context) (T, error)) (T, error) {
	var zero T
	retries := 0
	for {
		value, err := fetch(ctx)
		if err == nil {
			return value, nil
		}
		decision, ok := ClashFetchErrorDecision(err)
		if !ok || decision.Action != clashtracker.FetchErrorRetry {
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
