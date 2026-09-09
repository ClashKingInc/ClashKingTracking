package cwlstats

import (
	"reflect"
	"testing"
	"time"
)

func TestCurrentAndPreviousUTCCrossesYearBoundary(t *testing.T) {
	now := time.Date(2026, time.January, 31, 23, 59, 0, 0, time.FixedZone("west", -8*60*60))
	want := []string{"2026-02", "2026-01"}
	if got := CurrentAndPreviousUTC(now); !reflect.DeepEqual(got, want) {
		t.Fatalf("seasons = %#v, want %#v", got, want)
	}
}
