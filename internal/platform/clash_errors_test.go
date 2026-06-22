package platform

import (
	"context"
	"errors"
	"testing"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestIsNonFatalClashError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "gateway status zero",
			err:  &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 0}},
			want: true,
		},
		{
			name: "gateway 502",
			err:  &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 502}},
			want: true,
		},
		{
			name: "gateway 504",
			err:  &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 504}},
			want: true,
		},
		{
			name: "maintenance",
			err:  &clashy.Maintenance{HTTPException: &clashy.HTTPException{Status: 503}},
			want: true,
		},
		{
			name: "deadline exceeded",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "canceled",
			err:  context.Canceled,
			want: false,
		},
		{
			name: "bad request",
			err:  &clashy.InvalidArgument{HTTPException: &clashy.HTTPException{Status: 400}},
			want: true,
		},
		{
			name: "forbidden",
			err:  &clashy.Forbidden{HTTPException: &clashy.HTTPException{Status: 403}},
			want: true,
		},
		{
			name: "not found",
			err:  &clashy.NotFound{HTTPException: &clashy.HTTPException{Status: 404}},
			want: true,
		},
		{
			name: "ordinary error",
			err:  errors.New("store write failed"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsNonFatalClashError(tt.err); got != tt.want {
				t.Fatalf("IsNonFatalClashError() = %t, want %t", got, tt.want)
			}
		})
	}
}
