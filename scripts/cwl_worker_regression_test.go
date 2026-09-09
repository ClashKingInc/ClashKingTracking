//go:build script_internal_tests

package scripts

import (
	"clashking_tracking/internal/platform"
	clashy "github.com/clashkinginc/clashy.go"
	"testing"
	"time"
)

func TestRegularDiscoveryRejectsPastAndInvalidResponses(t *testing.T) {
	now := time.Now().UTC()
	for _, tc := range []struct {
		name  string
		state clashy.WarState
		end   time.Time
		want  bool
	}{
		{"active", clashy.WarStateInWar, now.Add(time.Hour), true},
		{"preparation", clashy.WarStatePreparation, now.Add(time.Hour), true},
		{"past", clashy.WarStateInWar, now.Add(-time.Second), false},
		{"boundary", clashy.WarStateInWar, now, false},
		{"ended", clashy.WarStateEnded, now.Add(-time.Hour), false},
		{"invalid", clashy.WarStateNotInWar, now.Add(time.Hour), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			war := sampleWar(now.Add(-time.Hour), now, tc.end)
			war.State = tc.state
			if got := discoverableRegularWar(war, now); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	war := sampleWar(now.Add(-time.Hour), now, now.Add(time.Hour))
	war.Clan.Tag = ""
	if discoverableRegularWar(war, now) {
		t.Fatal("missing identity accepted")
	}
}

func TestCWLBatchLookupUsesOnlyTheRequestedWarsSize(t *testing.T) {
	domain := &warsDomain{name: cwlDomainName, store: newMemoryWarStore()}
	size, err := domain.scheduleCWLWars(t.Context(), &platform.App{}, nil, cwlGroupFromRounds([][]string{{"#A"}}), true, map[string]int{"#A": 15, "#B": 30})
	if err != nil || size != 15 {
		t.Fatalf("size=%d err=%v", size, err)
	}
}

func TestCWLSweepCalendar(t *testing.T) {
	for _, tc := range []struct {
		at                 string
		discovery, refresh bool
	}{
		{"2026-09-01T07:59:59Z", false, true},
		{"2026-09-01T08:00:00Z", true, true},
		{"2026-09-03T08:00:00Z", true, true},
		{"2026-09-03T11:59:59Z", true, true},
		{"2026-09-03T12:00:00Z", false, true},
		{"2026-09-14T23:59:59Z", false, true},
		{"2026-09-15T00:00:00Z", false, false},
		{"2026-09-20T00:00:00Z", false, false},
		{"2026-10-01T08:00:00Z", true, true},
		{"2026-09-03T07:00:00-05:00", false, true},
	} {
		t.Run(tc.at, func(t *testing.T) {
			now, err := time.Parse(time.RFC3339, tc.at)
			if err != nil {
				t.Fatal(err)
			}
			if got := cwlSweepDue(now, false, false); got != tc.discovery {
				t.Fatalf("discovery=%v want %v", got, tc.discovery)
			}
			if got := cwlSweepDue(now, true, false); got != tc.refresh {
				t.Fatalf("refresh=%v want %v", got, tc.refresh)
			}
			if !cwlSweepDue(now, false, true) {
				t.Fatal("startup recovery must run outside the recurring window too")
			}
		})
	}
}
