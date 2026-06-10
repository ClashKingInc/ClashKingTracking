//go:build script_internal_tests

package scripts

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"

	clashy "github.com/clashkinginc/clashy.go"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestBotClanSnapshotChangedBaselinesThenDetectsChange(t *testing.T) {
	store := &memoryBotClanSnapshotStore{values: make(map[string][]byte)}
	clan := clashy.Clan{Tag: "#CLAN", Name: "Before"}
	prefix := "botclans:test:"

	_, _, hasPrevious, changed, err := botClanSnapshotChanged(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hasPrevious || !changed {
		t.Fatalf("first snapshot = hasPrevious %v changed %v, want false/true", hasPrevious, changed)
	}
	_, _, hasPrevious, changed, err = botClanSnapshotChanged(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || changed {
		t.Fatalf("same snapshot = hasPrevious %v changed %v, want true/false", hasPrevious, changed)
	}
	clan.Name = "After"
	previous, _, hasPrevious, changed, err := botClanSnapshotChanged(t.Context(), store, prefix, "clan", clan.Tag, clan, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !hasPrevious || !changed || previous == nil || previous.Name != "Before" {
		t.Fatalf("changed snapshot = previous %#v hasPrevious %v changed %v", previous, hasPrevious, changed)
	}
}

func TestBotCWLWindowSkipsEndedAndNoSpinState(t *testing.T) {
	now := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	active := botCWLState{Season: utils.CurrentSeason(now), GroupState: "inWar"}
	if !shouldPollCWL(now, active) {
		t.Fatal("active current-season cwl should keep polling after discovery window")
	}
	ended := active
	ended.Ended = true
	if shouldPollCWL(now, ended) {
		t.Fatal("ended current-season cwl should not keep polling")
	}
	noSpin := active
	noSpin.NoSpin = true
	if shouldPollCWL(now, noSpin) {
		t.Fatal("current-season no-spin cwl should not keep polling")
	}
}

func TestLatestCWLWarTagsUsesNewestNonEmptyRound(t *testing.T) {
	group := &clashy.ClanWarLeagueGroup{Rounds: []struct {
		WarTags []string `json:"warTags,omitempty"`
	}{
		{WarTags: []string{"#OLD"}},
		{WarTags: []string{"#0", ""}},
		{WarTags: []string{"#NEW1", "#NEW2"}},
	}}
	tags, hash := latestCWLWarTags(group)
	if want := []string{"#NEW1", "#NEW2"}; !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags = %#v, want %#v", tags, want)
	}
	if hash != "#NEW1,#NEW2" {
		t.Fatalf("hash = %q", hash)
	}
}

func TestCWLLineupChanges(t *testing.T) {
	previous := clashy.ClanWar{
		Clan:     &clashy.WarClan{Tag: "#A", Members: []clashy.ClanWarMember{{Tag: "#P1"}, {Tag: "#P2"}}},
		Opponent: &clashy.WarClan{Tag: "#B", Members: []clashy.ClanWarMember{{Tag: "#O1"}}},
	}
	current := clashy.ClanWar{
		Clan:     &clashy.WarClan{Tag: "#A", Members: []clashy.ClanWarMember{{Tag: "#P2"}, {Tag: "#P3"}}},
		Opponent: &clashy.WarClan{Tag: "#B", Members: []clashy.ClanWarMember{{Tag: "#O1"}, {Tag: "#O2"}}},
	}
	changes := cwlLineupChanges(previous, current)
	if !cwlLineupChanged(changes) {
		t.Fatal("lineup change was not detected")
	}
	if added := changes["added"].([]clashy.ClanWarMember); len(added) != 1 || added[0].Tag != "#P3" {
		t.Fatalf("added = %#v", added)
	}
	if removed := changes["removed"].([]clashy.ClanWarMember); len(removed) != 1 || removed[0].Tag != "#P1" {
		t.Fatalf("removed = %#v", removed)
	}
}

func TestRaidMissingMembersUsesClanSnapshot(t *testing.T) {
	clan := clashy.Clan{Members: []clashy.ClanMember{
		{Tag: "#A", Name: "A", TownHall: 16, Role: clashy.RoleMember},
		{Tag: "#B", Name: "B", TownHall: 12, Role: clashy.RoleMember},
		{Tag: "#C", Name: "C", TownHall: 16, Role: clashy.RoleLeader},
	}}
	raid := clashy.RaidLogEntry{Members: []clashy.RaidMember{
		{Tag: "#A", AttackCount: 3, AttackLimit: 5},
		{Tag: "#B", AttackCount: 0, AttackLimit: 5},
	}}
	missing := raidMissingMembers(clan, raid, bson.M{
		"attack_threshold": 1,
		"roles":            bson.A{"member"},
		"townhalls":        bson.A{16},
	})
	if len(missing) != 1 || missing[0]["tag"] != "#A" {
		t.Fatalf("missing = %#v, want only #A", missing)
	}
}

type trackerTestValue struct {
	Tag   string `json:"tag"`
	Score int    `json:"score"`
}

type TargetSourceFunc func(context.Context) ([]string, error)

func (f TargetSourceFunc) Targets(ctx context.Context) ([]string, error) {
	return f(ctx)
}

func TestChunkStrings(t *testing.T) {
	chunks := chunkStrings([]string{"a", "b", "c", "d", "e"}, 2)
	if len(chunks) != 3 || len(chunks[0]) != 2 || len(chunks[2]) != 1 {
		t.Fatalf("unexpected chunks: %#v", chunks)
	}
}

func TestCycleRunnerRunOnceProcessesTargets(t *testing.T) {
	var processed atomic.Int32
	runner := CycleRunner[trackerTestValue]{
		App:    botClansTestApp(),
		Domain: "test",
		Groups: []CycleGroup[trackerTestValue]{{
			Name: "values", Kind: "value",
			Source: TargetSourceFunc(func(context.Context) ([]string, error) {
				return []string{"#AAA", "#BBB"}, nil
			}),
			Fetch: func(_ context.Context, tag string) (*trackerTestValue, int, error) {
				return &trackerTestValue{Tag: tag, Score: 1}, 0, nil
			},
			Processor: BatchProcessorFunc[trackerTestValue](func(_ context.Context, _ *platform.App, item TrackedItem[trackerTestValue]) error {
				if item.Current == nil || item.Tag == "" {
					t.Fatalf("missing current item: %#v", item)
				}
				processed.Add(1)
				return nil
			}),
			BatchSize: 10,
			RateLimit: 2,
		}},
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if processed.Load() != 2 {
		t.Fatalf("expected two processed items, got %d", processed.Load())
	}
}

func TestCycleRunnerRunOnceCanBeCalledRepeatedly(t *testing.T) {
	var processed atomic.Int32
	app := botClansTestApp()
	runner := CycleRunner[trackerTestValue]{
		App:    app,
		Domain: "test",
		Groups: []CycleGroup[trackerTestValue]{{
			Name: "values", Kind: "value",
			Source: TargetSourceFunc(func(context.Context) ([]string, error) {
				return []string{"#AAA"}, nil
			}),
			Fetch: func(_ context.Context, tag string) (*trackerTestValue, int, error) {
				return &trackerTestValue{Tag: tag, Score: 1}, 0, nil
			},
			Processor: BatchProcessorFunc[trackerTestValue](func(context.Context, *platform.App, TrackedItem[trackerTestValue]) error {
				processed.Add(1)
				return nil
			}),
			BatchSize: 1,
			RateLimit: 1,
		}},
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if processed.Load() != 2 {
		t.Fatalf("expected each run-once invocation to process, got %d calls", processed.Load())
	}
}

func TestCycleRunnerHonorsRateLimit(t *testing.T) {
	var current atomic.Int32
	var maxSeen atomic.Int32
	runner := CycleRunner[trackerTestValue]{
		App:    botClansTestApp(),
		Domain: "test",
		Groups: []CycleGroup[trackerTestValue]{{
			Name: "values", Kind: "value",
			Source: TargetSourceFunc(func(context.Context) ([]string, error) {
				return []string{"#A", "#B", "#C", "#D"}, nil
			}),
			Fetch: func(_ context.Context, tag string) (*trackerTestValue, int, error) {
				now := current.Add(1)
				for {
					old := maxSeen.Load()
					if now <= old || maxSeen.CompareAndSwap(old, now) {
						break
					}
				}
				time.Sleep(10 * time.Millisecond)
				current.Add(-1)
				return &trackerTestValue{Tag: tag}, 0, nil
			},
			Processor: BatchProcessorFunc[trackerTestValue](func(context.Context, *platform.App, TrackedItem[trackerTestValue]) error {
				return nil
			}),
			BatchSize: 4,
			RateLimit: 2,
		}},
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if maxSeen.Load() > 2 {
		t.Fatalf("expected max concurrency <= 2, got %d", maxSeen.Load())
	}
}

func TestCycleRunnerFetchErrorMarksDomainUnhealthy(t *testing.T) {
	app := botClansTestApp()
	runner := CycleRunner[trackerTestValue]{
		App:    app,
		Domain: "test",
		Groups: []CycleGroup[trackerTestValue]{{
			Name: "values", Kind: "value",
			Source: TargetSourceFunc(func(context.Context) ([]string, error) {
				return []string{"#AAA"}, nil
			}),
			Fetch: func(context.Context, string) (*trackerTestValue, int, error) {
				return nil, 0, errors.New("maintenance")
			},
			Processor: BatchProcessorFunc[trackerTestValue](func(context.Context, *platform.App, TrackedItem[trackerTestValue]) error {
				t.Fatal("processor should not run after fetch error")
				return nil
			}),
			BatchSize: 1,
			RateLimit: 1,
		}},
	}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	stats := app.Stats.Domain("test")
	if stats.Healthy {
		t.Fatalf("expected domain to be unhealthy after fetch error")
	}
	if stats.LastError != "maintenance" {
		t.Fatalf("unexpected last error: %q", stats.LastError)
	}
}

func TestParseReminderHours(t *testing.T) {
	got, err := parseReminderHours("1.5hr")
	if err != nil {
		t.Fatal(err)
	}
	if got != 1.5 {
		t.Fatalf("parseReminderHours = %v, want 1.5", got)
	}
	if _, err := parseReminderHours("soon"); err == nil {
		t.Fatal("expected invalid reminder time error")
	}
}

func botClansTestApp() *platform.App {
	return &platform.App{
		Config: platform.Config{
			RunOnce:                  true,
			BotClanRequestsPerSecond: 950,
			BotClanSnapshotPrefix:    "botclans:test:",
			BotClanCWLStateSnapshot:  "cwlstate",
		},
		Stats: platform.NewTracker(),
	}
}
