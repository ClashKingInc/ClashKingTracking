package platform

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestClashFetchJSONRetryBoundary(t *testing.T) {
	for _, body := range []string{"", `{"state":`, `not-json`} {
		t.Run(body, func(t *testing.T) {
			attempts := 0
			_, err := RetryClashFetch(context.Background(), NewAvailabilityGate(nil), func(context.Context) (any, error) {
				attempts++
				var value any
				return value, json.Unmarshal([]byte(body), &value)
			})
			var exhausted *ClashFetchExhausted
			if attempts != 4 || !errors.As(err, &exhausted) || exhausted.Attempts != 4 {
				t.Fatalf("attempts=%d error=%v", attempts, err)
			}
		})
	}
	attempts := 0
	value, err := RetryClashFetch(context.Background(), NewAvailabilityGate(nil), func(context.Context) (int, error) {
		attempts++
		if attempts == 1 {
			return 0, io.ErrUnexpectedEOF
		}
		return 42, nil
	})
	if err != nil || value != 42 || attempts != 2 {
		t.Fatalf("value=%d attempts=%d err=%v", value, attempts, err)
	}
	if _, retry := ClashFetchRetryPolicy(&json.UnmarshalTypeError{Value: "string"}); retry {
		t.Fatal("a contract type mismatch must remain visible, not become a transient syntax failure")
	}
	fatal := errors.New("database schema mismatch")
	_, err = RetryClashFetch(context.Background(), NewAvailabilityGate(nil), func(context.Context) (int, error) { return 0, fatal })
	var exhausted *ClashFetchExhausted
	if !errors.Is(err, fatal) || errors.As(err, &exhausted) {
		t.Fatalf("non-transient errors must not be marked safe to defer: %v", err)
	}
}

func TestClashFetchRetryPolicy(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		wantOK    bool
		wantDelay string
		wantMax   int
	}{
		{
			name:      "proxy unavailable",
			err:       &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 0}},
			wantOK:    true,
			wantDelay: "1m0s",
		},
		{
			name:      "gateway timeout",
			err:       &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 504}},
			wantOK:    true,
			wantDelay: "500ms",
			wantMax:   3,
		},
		{
			name:      "request throttled",
			err:       &clashy.HTTPException{Status: 429},
			wantOK:    true,
			wantDelay: "1s",
			wantMax:   3,
		},
		{
			name:      "truncated response",
			err:       io.ErrUnexpectedEOF,
			wantOK:    true,
			wantDelay: "500ms",
			wantMax:   3,
		},
		{
			name:      "maintenance",
			err:       &clashy.Maintenance{HTTPException: &clashy.HTTPException{Status: 503}},
			wantOK:    true,
			wantDelay: "1m0s",
		},
		{
			name:   "ordinary error",
			err:    errors.New("store write failed"),
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ClashFetchRetryPolicy(tt.err)
			if ok != tt.wantOK {
				t.Fatalf("ok = %t, want %t", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if got.RetryAfter.String() != tt.wantDelay {
				t.Fatalf("RetryAfter = %s, want %s", got.RetryAfter, tt.wantDelay)
			}
			if got.MaxRetries != tt.wantMax {
				t.Fatalf("MaxRetries = %d, want %d", got.MaxRetries, tt.wantMax)
			}
		})
	}
}
