package platform

import (
	"errors"
	"testing"

	clashy "github.com/clashkinginc/clashy.go"
	clashtracker "github.com/clashkinginc/clashy.go/tracker"
)

func TestClashFetchErrorDecision(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantOK     bool
		wantAction clashtracker.FetchErrorAction
		wantDelay  string
		wantMax    int
	}{
		{
			name:       "proxy unavailable",
			err:        &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 0}},
			wantOK:     true,
			wantAction: clashtracker.FetchErrorRetry,
			wantDelay:  "1m0s",
		},
		{
			name:       "gateway timeout",
			err:        &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 504}},
			wantOK:     true,
			wantAction: clashtracker.FetchErrorRetry,
			wantDelay:  "5s",
			wantMax:    3,
		},
		{
			name:       "maintenance",
			err:        &clashy.Maintenance{HTTPException: &clashy.HTTPException{Status: 503}},
			wantOK:     true,
			wantAction: clashtracker.FetchErrorRetry,
			wantDelay:  "1m0s",
		},
		{
			name:   "ordinary error",
			err:    errors.New("store write failed"),
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ClashFetchErrorDecision(tt.err)
			if ok != tt.wantOK {
				t.Fatalf("ok = %t, want %t", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if got.Action != tt.wantAction {
				t.Fatalf("Action = %v, want %v", got.Action, tt.wantAction)
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
