//go:build script_internal_tests

package scripts

import (
	"context"
	"errors"
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
		ScheduleKey: "#A-#B-1",
		WarID:       "018f4ad0-26c7-7b0d-9a4c-5b6c7d8e9f01",
		PrepTime:    time.Now(),
		EndTime:     time.Now().Add(time.Hour),
		StoreOnly:   true,
	})
	if err != nil {
		t.Fatalf("complete store request rejected: %v", err)
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

func TestBuildWarIngestSchedulesActiveWar(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	prep := now.Add(-time.Hour)
	start := now
	end := start.Add(24 * time.Hour)
	war := sampleWar(prep, start, end)

	ingest, err := buildWarIngest(war, "#AAA", false, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.IndexRows) != 0 || len(ingest.AttackRows) != 0 {
		t.Fatalf("active war should only schedule final store: %#v", ingest)
	}
	if len(ingest.Schedules) != 1 || ingest.Schedules[0].ScheduleKey == "" || ingest.Schedules[0].WarID == "" || !ingest.Schedules[0].NextRunAt.Equal(end) {
		t.Fatalf("unexpected schedule: %#v", ingest.Schedules)
	}
	if got := ingest.CurrentWarTimers; len(got) != 2 || got[0].PlayerTag != "#P1" || got[0].ClanTag != "#AAA" || got[0].OpponentTag != "#BBB" || got[1].PlayerTag != "#P2" || got[1].ClanTag != "#BBB" || got[1].OpponentTag != "#AAA" || got[0].WarID != ingest.Schedules[0].WarID || !got[0].EndTime.Equal(end) {
		t.Fatalf("unexpected current war timers: %#v", got)
	}
}

func TestMemoryWarStoreCurrentWarTimerConflictReplacesPriorWar(t *testing.T) {
	store := newMemoryWarStore()
	now := time.Now().UTC()
	for _, row := range []models.CurrentWarTimerRow{
		{PlayerTag: "#P1", WarID: "war-one", ClanTag: "#AAA", OpponentTag: "#BBB", EndTime: now.Add(time.Hour)},
		{PlayerTag: "#P1", WarID: "war-two", ClanTag: "#CCC", OpponentTag: "#DDD", EndTime: now.Add(2 * time.Hour)},
	} {
		if err := store.Store(context.Background(), models.WarIngest{CurrentWarTimers: []models.CurrentWarTimerRow{row}}); err != nil {
			t.Fatal(err)
		}
	}
	got, ok, err := store.LoadActiveCurrentWarTimer(context.Background(), "#P1")
	if err != nil || !ok || got.WarID != "war-two" || got.ClanTag != "#CCC" || got.OpponentTag != "#DDD" {
		t.Fatalf("replacement = %#v, %v, %v", got, ok, err)
	}
}

func TestCurrentWarTimerCleanupAndActiveReads(t *testing.T) {
	if currentWarTimerCleanupInterval != 5*time.Minute {
		t.Fatalf("cleanup interval = %s", currentWarTimerCleanupInterval)
	}
	if !strings.Contains(deleteExpiredCurrentWarTimersSQL, "end_time <= now()") || !strings.Contains(loadActiveCurrentWarTimerSQL, "end_time > now()") {
		t.Fatalf("timer SQL must clean expired and read active only: cleanup=%s read=%s", deleteExpiredCurrentWarTimersSQL, loadActiveCurrentWarTimerSQL)
	}
	store := newMemoryWarStore()
	now := time.Now().UTC()
	err := store.Store(context.Background(), models.WarIngest{CurrentWarTimers: []models.CurrentWarTimerRow{
		{PlayerTag: "#EXPIRED", WarID: "war-old", ClanTag: "#AAA", OpponentTag: "#BBB", EndTime: now.Add(-time.Second)},
		{PlayerTag: "#ACTIVE", WarID: "war-live", ClanTag: "#AAA", OpponentTag: "#BBB", EndTime: now.Add(time.Hour)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.LoadActiveCurrentWarTimer(context.Background(), "#EXPIRED"); err != nil || ok {
		t.Fatalf("expired read = ok:%v err:%v", ok, err)
	}
	if got, ok, err := store.LoadActiveCurrentWarTimer(context.Background(), "#ACTIVE"); err != nil || !ok || got.WarID != "war-live" {
		t.Fatalf("active read = %#v ok:%v err:%v", got, ok, err)
	}
	deleted, err := store.DeleteExpiredCurrentWarTimers(context.Background())
	if err != nil || deleted != 1 {
		t.Fatalf("cleanup = %d, %v", deleted, err)
	}
}

func TestCurrentWarTimerBulkUpsertSQL(t *testing.T) {
	if !strings.Contains(upsertCurrentWarTimersSQL, "unnest(") || strings.Contains(upsertCurrentWarTimersSQL, "pgx.Batch") || !strings.Contains(upsertCurrentWarTimersSQL, "ON CONFLICT (player_tag)") {
		t.Fatalf("current war timers must use one bulk upsert: %s", upsertCurrentWarTimersSQL)
	}
}

func TestBuildWarIngestFinishedAddsPermanentRows(t *testing.T) {
	prep := time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)
	war := sampleWar(prep, prep.Add(time.Hour), prep.Add(2*time.Hour))

	ingest, err := buildWarIngest(war, "#AAA", true, "#WAR", "#AAA-#BBB-1", "018f4ad0-26c7-7b0d-9a4c-5b6c7d8e9f01")
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.Schedules) != 0 {
		t.Fatalf("finished ingest should not reschedule: %#v", ingest.Schedules)
	}
	if ingest.FinishedScheduleKey != "#AAA-#BBB-1" || ingest.FinishedWarID != "018f4ad0-26c7-7b0d-9a4c-5b6c7d8e9f01" {
		t.Fatalf("missing finished-war fields: %#v", ingest)
	}
	if len(ingest.IndexRows) != 1 {
		t.Fatalf("IndexRows len = %d, want 1", len(ingest.IndexRows))
	}
	if ingest.IndexRows[0].AttacksPerMember != 1 {
		t.Fatalf("AttacksPerMember = %d, want 1 for a CWL war", ingest.IndexRows[0].AttacksPerMember)
	}
	if len(ingest.AttackRows) != 1 {
		t.Fatalf("AttackRows len = %d, want 1", len(ingest.AttackRows))
	}
	attack := ingest.AttackRows[0]
	if attack.AttackingClanTag != "#AAA" || attack.DefendingClanTag != "#BBB" || attack.AttackerTownHall != 16 || attack.DefenderTownHall != 15 {
		t.Fatalf("unexpected attack row: %#v", attack)
	}
}

func TestBuildWarIngestUsesTwoAttacksForRegularWar(t *testing.T) {
	prep := time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)
	war := sampleWar(prep, prep.Add(time.Hour), prep.Add(2*time.Hour))
	ingest, err := buildWarIngest(war, "#AAA", true, "", "#AAA-#BBB-1", "018f4ad0-26c7-7b0d-9a4c-5b6c7d8e9f01")
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.IndexRows) != 1 || ingest.IndexRows[0].AttacksPerMember != 2 {
		t.Fatalf("regular war index rows = %#v, want attacks_per_member 2", ingest.IndexRows)
	}
}

func TestBuildWarIngestDoesNotScheduleEndedActiveWar(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	war := sampleWar(now.Add(-3*time.Hour), now.Add(-2*time.Hour), now.Add(-time.Hour))

	ingest, err := buildWarIngest(war, "#AAA", false, "", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.Schedules) != 0 {
		t.Fatalf("ended active war should not schedule final store: %#v", ingest.Schedules)
	}
	if len(ingest.IndexRows) != 0 || len(ingest.AttackRows) != 0 {
		t.Fatalf("ended active war should be skipped until fetched as finished: %#v", ingest)
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
	if id != "I1flsSlmDR9d" {
		t.Fatalf("cwl id = %q", id)
	}
	if len(id) != 12 {
		t.Fatalf("cwl id length = %d, want 12", len(id))
	}
	if want := []string{"#AAA", "#BBB"}; !reflect.DeepEqual(tags, want) {
		t.Fatalf("tags = %#v, want %#v", tags, want)
	}
	if want := [][]string{{"#WAR1"}, {"#WAR2"}}; !reflect.DeepEqual(cwlRounds(group), want) {
		t.Fatalf("rounds = %#v, want %#v", cwlRounds(group), want)
	}
}

func TestCWLGroupRowUsesFinalTypedSnapshotShape(t *testing.T) {
	group := &clashy.ClanWarLeagueGroup{
		State:  "ended",
		Season: "2026-07",
		Clans: []clashy.ClanWarLeagueClan{
			{Tag: "#AAA", Name: "Alpha", Level: 17, Badge: clashy.Badge{Medium: "badge-alpha"}},
			{Tag: "#BBB", Name: "Beta", Level: 16, Badge: clashy.Badge{Medium: "badge-beta"}},
		},
	}
	row := cwlGroupRow("2026-07-AAA-BBB", group, 48000012)
	if row.CWLLeagueID == nil || *row.CWLLeagueID != 48000012 || row.State != "ended" {
		t.Fatalf("unexpected typed group row: %#v", row)
	}
	if len(row.Clans) != 2 || row.Clans[0].ClanTag != "#AAA" || row.Clans[0].Name != "Alpha" || row.Clans[0].ClanLevel != 17 || row.Clans[0].BadgeToken != "badge-alpha" {
		t.Fatalf("unexpected clan snapshots: %#v", row.Clans)
	}
	legacy := cwlGroupRow("legacy", &clashy.ClanWarLeagueGroup{State: "preparation", Season: "2026-07"}, 0)
	if legacy.CWLLeagueID != nil || legacy.WarSize != nil {
		t.Fatalf("nullable legacy dimensions were populated: %#v", legacy)
	}
}

func TestCWLGroupWritesUseFinalSchemaWithoutStandings(t *testing.T) {
	writeSQL := upsertCWLGroupsSQLShape + upsertCWLGroupClansSQL + upsertCWLGroupMembersSQL + deleteStaleCWLGroupMembersSQL
	for _, fragment := range []string{
		"cwl_id, season, cwl_league_id, state, war_size, rounds",
		"cwl_group_clans (cwl_id, clan_tag, name, clan_level, badge_token)",
		"cwl_group_members (cwl_id, clan_tag, name, tag, town_hall)",
		"ON CONFLICT (cwl_id, tag)",
		"WHERE cwl_id = $1",
		"AND clan_tag = $2",
		"NOT (tag = ANY($3::text[]))",
	} {
		if !strings.Contains(writeSQL, fragment) {
			t.Fatalf("final CWL write contract missing %q", fragment)
		}
	}
	if strings.Contains(writeSQL, "basic_clan") {
		t.Fatal("CWL master rosters must come from the official group response")
	}
	if strings.Contains(upsertCWLGroupClansSQL, "members)") {
		t.Fatal("cwl_group_clans must not retain a members JSONB column")
	}
	for _, removed := range []string{"ended_at", "created_at", "updated_at"} {
		if strings.Contains(upsertCWLGroupsSQLShape, removed) {
			t.Fatalf("final CWL group write still references %q", removed)
		}
	}
	standingsTable := "cwl_" + "standings"
	if strings.Contains(upsertCWLGroupsSQLShape, standingsTable) || strings.Contains(upsertCWLGroupClansSQL, standingsTable) {
		t.Fatal("group snapshot writes must not populate standings")
	}
}

func TestBasicClanMemberSnapshotIncludesTownHall(t *testing.T) {
	got := memberSnapshot([]clashy.ClanMember{{Tag: "#P1", Name: "One", TownHall: 17}})
	if len(got) != 1 || got[0].Tag != "#P1" || got[0].Name != "One" || got[0].TownHall != 17 {
		t.Fatalf("member snapshot = %#v", got)
	}
}

func TestCWLGroupClanBadgeIsTokenOnly(t *testing.T) {
	group := &clashy.ClanWarLeagueGroup{Clans: []clashy.ClanWarLeagueClan{{
		Tag: "#AAA", Badge: clashy.Badge{Medium: "https://api-assets.clashofclans.com/badges/200/example-token.png"},
		Members: []clashy.ClanWarLeagueClanMember{{Tag: "#P1", Name: "One", TownHallLevel: 17}},
	}}}
	rows := cwlGroupClanRows(group)
	if len(rows) != 1 || rows[0].BadgeToken != "example-token" {
		t.Fatalf("badge snapshot = %#v", rows)
	}
	if len(rows[0].Members) != 1 || rows[0].Members[0].Tag != "#P1" || rows[0].Members[0].TownHall != 17 {
		t.Fatalf("master roster snapshot = %#v", rows[0].Members)
	}
}

func TestCWLWarTagsDropsPlaceholdersAndDuplicates(t *testing.T) {
	group := &clashy.ClanWarLeagueGroup{Rounds: []struct {
		WarTags []string `json:"warTags,omitempty"`
	}{
		{WarTags: []string{"#WAR1", "#0", "#WAR2"}},
		{WarTags: []string{"#WAR2", "", "#WAR3"}},
	}}
	if want := []string{"#WAR1", "#WAR2", "#WAR3"}; !reflect.DeepEqual(warTags(group), want) {
		t.Fatalf("war tags = %#v, want %#v", warTags(group), want)
	}
}

func TestMemoryWarStoreShiftMaintenance(t *testing.T) {
	store := newMemoryWarStore()
	now := time.Now().UTC()
	err := store.Store(context.Background(), models.WarIngest{
		Schedules: []models.WarScheduleRow{
			{ScheduleKey: "#A-#B-1", WarID: "war-live", SourceClanTag: "#A", OpponentTag: "#B",
				PrepTime: now, EndTime: now.Add(time.Hour), NextRunAt: now.Add(time.Hour),
			},
			{ScheduleKey: "#C-#D-1", WarID: "war-expired", SourceClanTag: "#C", OpponentTag: "#D",
				PrepTime: now.Add(-2 * time.Hour), EndTime: now.Add(-time.Hour), NextRunAt: now.Add(-time.Hour),
			},
		},
		CurrentWarTimers: []models.CurrentWarTimerRow{
			{PlayerTag: "#P1", WarID: "war-live", ClanTag: "#A", OpponentTag: "#B", EndTime: now.Add(time.Hour)},
			{PlayerTag: "#P2", WarID: "war-live", ClanTag: "#B", OpponentTag: "#A", EndTime: now.Add(time.Hour)},
			{PlayerTag: "#P3", WarID: "war-expired", ClanTag: "#C", OpponentTag: "#D", EndTime: now.Add(-time.Hour)},
		},
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
	if got := store.schedules["#C-#D-1"].EndTime; !got.Equal(now.Add(-time.Hour)) {
		t.Fatalf("expired schedule shifted = %s", got)
	}
	if got := store.currentWarTimers["#P1"].EndTime; !got.Equal(now.Add(time.Hour + 2*time.Minute)) {
		t.Fatalf("live timer shifted = %s", got)
	}
	if got := store.currentWarTimers["#P2"].EndTime; !got.Equal(now.Add(time.Hour + 2*time.Minute)) {
		t.Fatalf("second participant shifted = %s", got)
	}
	if got := store.currentWarTimers["#P3"].EndTime; !got.Equal(now.Add(-time.Hour)) {
		t.Fatalf("expired timer shifted = %s", got)
	}
}

func TestOfficialMaintenance500Only(t *testing.T) {
	maintenance500 := &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 500}}
	if !isOfficialMaintenance500(maintenance500) {
		t.Fatal("official HTTP 500 must start maintenance")
	}
	for _, err := range []error{
		&clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 502}},
		&clashy.Maintenance{HTTPException: &clashy.HTTPException{Status: 503}},
		&clashy.Forbidden{HTTPException: &clashy.HTTPException{Status: 403}},
		errors.New("transport failure"),
	} {
		if isOfficialMaintenance500(err) {
			t.Fatalf("non-500 error started maintenance: %v", err)
		}
	}
}

func TestMaintenanceShiftDuration(t *testing.T) {
	start := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	if got, ok := maintenanceShiftDuration(start, start.Add(37*time.Second)); !ok || got != 37*time.Second {
		t.Fatalf("duration = %s, %v", got, ok)
	}
	if got, ok := maintenanceShiftDuration(start, start); ok || got != 0 {
		t.Fatalf("zero duration = %s, %v", got, ok)
	}
	if got, ok := maintenanceShiftDuration(time.Time{}, start); ok || got != 0 {
		t.Fatalf("missing start = %s, %v", got, ok)
	}
}

func TestShiftActiveWarMaintenanceSQLScopesSchedulesAndTimers(t *testing.T) {
	for _, fragment := range []string{
		"WITH shifted_wars", "WHERE end_time > now()", "RETURNING war_id", "timer.end_time > now()", "timer.war_id IN",
	} {
		if !strings.Contains(shiftActiveWarMaintenanceSQL, fragment) {
			t.Fatalf("maintenance SQL missing %q: %s", fragment, shiftActiveWarMaintenanceSQL)
		}
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
