package platform

import (
	"errors"
	"io"
	"testing"

	clashy "github.com/clashkinginc/clashy.go"
)

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
