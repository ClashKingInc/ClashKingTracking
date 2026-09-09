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
