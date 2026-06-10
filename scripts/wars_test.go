//go:build script_internal_tests

package scripts

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestWarQueueRejectsIncompleteStoreWork(t *testing.T) {
	queue := &warQueue{}
	if err := queue.Enqueue(warFetchRequest{}); err == nil {
		t.Fatal("expected missing clan tag error")
	}
	if err := queue.Enqueue(warFetchRequest{ClanTag: "#A", StoreOnly: true}); err == nil {
		t.Fatal("expected incomplete store request error")
	}
	err := queue.Enqueue(warFetchRequest{
		ClanTag:     "#A",
		OpponentTag: "#B",
		WarID:       "war",
		PrepTime:    time.Now(),
		EndTime:     time.Now().Add(time.Hour),
		StoreOnly:   true,
	})
	if err != nil {
		t.Fatalf("complete store request rejected: %v", err)
	}
}

func TestWarR2Key(t *testing.T) {
	if got, want := warR2Key("#A-#B-123"), "wars/#A-#B-123.json.snappy"; got != want {
		t.Fatalf("warR2Key = %q, want %q", got, want)
	}
}

func TestWarTargetsSQLOnlyUsesPublicWarLogs(t *testing.T) {
	if !strings.Contains(warTargetsSQL, "public_war_log = true") {
		t.Fatalf("war target query should require public war logs: %s", warTargetsSQL)
	}
	if strings.Contains(warTargetsSQL, "last_active") {
		t.Fatalf("war target query should not include activity fallback: %s", warTargetsSQL)
	}
	if !strings.Contains(warTargetsSQL, "NOT EXISTS") || !strings.Contains(warTargetsSQL, "war_schedule") {
		t.Fatalf("war target query should skip scheduled in-war clans: %s", warTargetsSQL)
	}
}

func TestBuildWarIngestFlattensWar(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	prep := now.Add(-time.Hour)
	start := now
	end := start.Add(24 * time.Hour)
	war := sampleWar(prep, start, end)

	ingest, err := buildWarIngest(war, "#AAA", false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.IndexRows) != 2 {
		t.Fatalf("IndexRows len = %d, want 2", len(ingest.IndexRows))
	}
	if len(ingest.AttackRows) != 1 {
		t.Fatalf("AttackRows len = %d, want 1", len(ingest.AttackRows))
	}
	attack := ingest.AttackRows[0]
	if attack.AttackingClanTag != "#AAA" || attack.DefendingClanTag != "#BBB" || attack.AttackerTownHall != 16 || attack.DefenderTownHall != 15 {
		t.Fatalf("unexpected attack row: %#v", attack)
	}
	if !attack.WarEndTime.Equal(end) {
		t.Fatalf("attack war end time = %s, want %s", attack.WarEndTime, end)
	}
	if len(ingest.Schedules) != 1 || ingest.Schedules[0].WarID == "" || !ingest.Schedules[0].NextRunAt.Equal(end) {
		t.Fatalf("unexpected schedule: %#v", ingest.Schedules)
	}
}

func TestBuildWarIngestFinishedAddsR2Payload(t *testing.T) {
	prep := time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)
	war := sampleWar(prep, prep.Add(time.Hour), prep.Add(2*time.Hour))

	ingest, err := buildWarIngest(war, "#AAA", true, "#WAR")
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.Schedules) != 0 {
		t.Fatalf("finished ingest should not reschedule: %#v", ingest.Schedules)
	}
	if ingest.FinishedWarID == "" || ingest.R2Key != warR2Key(ingest.FinishedWarID) || len(ingest.RawWarJSON) == 0 {
		t.Fatalf("missing finished object fields: %#v", ingest)
	}
}

func TestCWLGroupIDAndRounds(t *testing.T) {
	group := &clashy.ClanWarLeagueGroup{
		Season: "2026-05",
		Clans:  []clashy.ClanWarLeagueClan{{Tag: "#BBB"}, {Tag: "#AAA"}},
		Rounds: []struct {
			WarTags []string `json:"warTags,omitempty"`
		}{
			{WarTags: []string{"#WAR1", "#0", ""}},
			{WarTags: []string{"#WAR2"}},
		},
	}
	id, tags := cwlGroupID(group)
	if id != "2026-05-AAA-BBB" {
		t.Fatalf("cwl id = %q", id)
	}
	if want := []string{"#AAA", "#BBB"}; !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags = %#v, want %#v", tags, want)
	}
	if want := [][]string{{"#WAR1"}, {"#WAR2"}}; !reflect.DeepEqual(cwlRounds(group), want) {
		t.Fatalf("rounds = %#v, want %#v", cwlRounds(group), want)
	}
}

func TestMemoryWarStoreShiftMaintenance(t *testing.T) {
	store := newMemoryWarStore()
	now := time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)
	err := store.Store(context.Background(), models.WarIngest{
		IndexRows: []models.WarLogIndexRow{{WarID: "war", ClanTag: "#A", OpponentTag: "#B", PrepTime: now, EndTime: now.Add(time.Hour)}},
		Schedules: []models.WarScheduleRow{{
			WarID: "#A-#B-1", SourceClanTag: "#A", OpponentTag: "#B",
			PrepTime: now, EndTime: now.Add(time.Hour), NextRunAt: now.Add(time.Hour), Status: warSchedulePending,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ShiftMaintenance(context.Background(), 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	schedule := store.schedules["#A-#B-1"]
	if !schedule.NextRunAt.Equal(now.Add(time.Hour + 2*time.Minute)) {
		t.Fatalf("shifted next run = %s", schedule.NextRunAt)
	}
}

func sampleWar(prep, start, end time.Time) clashy.ClanWar {
	return clashy.ClanWar{
		State:                clashy.WarStateInWar,
		TeamSize:             15,
		PreparationStartTime: &clashy.Timestamp{Time: prep},
		StartTime:            &clashy.Timestamp{Time: start},
		EndTime:              &clashy.Timestamp{Time: end},
		Clan: &clashy.WarClan{
			Tag:   "#AAA",
			Name:  "A",
			Badge: clashy.Badge{Large: "large-a"},
			Members: []clashy.ClanWarMember{{
				Tag: "#P1", Name: "Player", Townhall: 16, MapPosition: 1,
				Attacks: []clashy.WarAttack{{Order: 1, AttackerTag: "#P1", DefenderTag: "#P2", Stars: 3, Destruction: 100, Duration: 120}},
			}},
		},
		Opponent: &clashy.WarClan{
			Tag:   "#BBB",
			Name:  "B",
			Badge: clashy.Badge{Large: "large-b"},
			Members: []clashy.ClanWarMember{{
				Tag: "#P2", Name: "Defender", Townhall: 15, MapPosition: 2,
			}},
		},
	}
}
