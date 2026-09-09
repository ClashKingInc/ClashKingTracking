package utils

import (
	"testing"
	"time"
)

func TestIsCWLUsesFirstThroughFifteenthUTC(t *testing.T) {
	for _, test := range []struct {
		day  int
		want bool
	}{
		{day: 1, want: true},
		{day: 15, want: true},
		{day: 16, want: false},
		{day: 28, want: false},
	} {
		now := time.Date(2026, time.August, test.day, 12, 0, 0, 0, time.UTC)
		if got := IsCWL(now); got != test.want {
			t.Fatalf("IsCWL(day %d) = %v, want %v", test.day, got, test.want)
		}
	}
}
