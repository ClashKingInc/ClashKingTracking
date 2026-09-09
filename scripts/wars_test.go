//go:build script_internal_tests

package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/wararchive"
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
		WarID:       42,
		PrepTime:    time.Now(),
		EndTime:     time.Now().Add(time.Hour),
		StoreOnly:   true,
	})
	if err != nil {
		t.Fatalf("complete store request rejected: %v", err)
	}
}

func TestPrimeWarArchiveCacheUsesOneByteGet(t *testing.T) {
	var method, byteRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		byteRange = r.Header.Get("Range")
		w.Header().Set("CF-Cache-Status", "MISS")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte{0})
	}))
	defer server.Close()
	if err := primeWarArchiveCache(context.Background(), server.URL, "packs/000001.pack"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodGet || byteRange != "bytes=0-0" {
		t.Fatalf("cache prime request = %s %q, want GET bytes=0-0", method, byteRange)
	}
}

func TestWarTargetsSQLOnlyUsesPublicWarLogs(t *testing.T) {
	for name, query := range map[string]string{"active": activeWarTargetsSQL, "dormant": dormantWarTargetsSQL} {
		if !strings.Contains(query, "public_war_log = true") {
			t.Fatalf("%s war target query should require public war logs: %s", name, query)
		}
		if strings.Contains(query, "last_active") {
			t.Fatalf("%s war target query should not include activity fallback: %s", name, query)
		}
		if !strings.Contains(query, "30 days") || !strings.Contains(query, "NOT EXISTS") || !strings.Contains(query, "war_schedule") {
			t.Fatalf("%s war target query should tier by recent war and skip scheduled clans: %s", name, query)
		}
	}
}

func TestCWLTargetsSkipKnownGroupSiblingsDuringDiscovery(t *testing.T) {
	for _, required := range []string{
		"EXTRACT(DAY FROM now() AT TIME ZONE 'UTC') BETWEEN 1 AND 15",
		"WITH current_clans AS MATERIALIZED",
		"JOIN cwl_group_clans known_clan",
		"known_group.season >= to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM')",
	} {
		if !strings.Contains(cwlDiscoveryTargetsSQL, required) {
			t.Fatalf("CWL discovery query is missing rule %q: %s", required, cwlDiscoveryTargetsSQL)
		}
	}
	for _, excluded := range []string{"public_war_log", "cwl_league_id IS NOT NULL", "last_active", "last_war_at"} {
		if strings.Contains(cwlDiscoveryTargetsSQL, excluded) {
			t.Fatalf("CWL discovery query must not restrict candidates by %q: %s", excluded, cwlDiscoveryTargetsSQL)
		}
	}
	if !strings.Contains(cwlDiscoveryTargetsSQL, "COALESCE(cwl_league_id, 0)") {
		t.Fatalf("CWL discovery query must scan null cached leagues safely: %s", cwlDiscoveryTargetsSQL)
	}
	for _, required := range []string{"WITH current_groups AS MATERIALIZED", "min(group_clan.clan_tag)", "JOIN basic_clan candidate_clan", "state <> 'ended'"} {
		if !strings.Contains(cwlRefreshTargetsSQL, required) {
			t.Fatalf("CWL refresh query is missing rule %q: %s", required, cwlRefreshTargetsSQL)
		}
	}
	if strings.Contains(cwlRefreshTargetsSQL, "war_schedule") {
		t.Fatal("refresh eligibility must not depend on war schedules")
	}
}

func TestCWLGroupLeaguePrefersRankedSiblingConsensus(t *testing.T) {
	store := newMemoryWarStore()
	store.targets = []models.BasicClanRow{
		{Tag: "#FIRST", CWLLeagueID: unrankedWarLeagueID},
		{Tag: "#KNOWN1", CWLLeagueID: 48000012},
		{Tag: "#KNOWN2", CWLLeagueID: 48000012},
		{Tag: "#OTHER", CWLLeagueID: 48000013},
	}
	leagueID, err := store.ResolveCWLGroupLeague(t.Context(), []string{"#FIRST", "#KNOWN1", "#KNOWN2", "#OTHER"}, unrankedWarLeagueID)
	if err != nil {
		t.Fatal(err)
	}
	if leagueID != 48000012 {
		t.Fatalf("resolved league = %d, want ranked sibling consensus 48000012", leagueID)
	}
	leagueID, err = store.ResolveCWLGroupLeague(t.Context(), []string{"#FIRST"}, unrankedWarLeagueID)
	if err != nil || leagueID != unrankedWarLeagueID {
		t.Fatalf("unranked fallback = %d, %v", leagueID, err)
	}
}

func TestMemoryWarTargetSourceCountsFinitePools(t *testing.T) {
	source := newMemoryWarTargetSource([]models.BasicClanRow{{Tag: "#A"}, {Tag: "#B"}})
	count, err := source.CountTargets(t.Context(), activeWarTargets)
	if err != nil || count != 2 {
		t.Fatalf("CountTargets(active) = %d, %v; want 2", count, err)
	}
	count, err = source.CountTargets(t.Context(), dormantWarTargets)
	if err != nil || count != 0 {
		t.Fatalf("CountTargets(dormant) = %d, %v; want 0", count, err)
	}
	if _, err := source.CountTargets(t.Context(), cwlTargets); err == nil {
		t.Fatal("CWL source should not run an exact target count")
	}
}

func TestBuildWarIngestSchedulesActiveWar(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	prep := now.Add(-time.Hour)
	start := now
	end := start.Add(24 * time.Hour)
	war := sampleWar(prep, start, end)

	ingest, err := buildWarIngest(war, "#AAA", false, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.IndexRows) != 0 || len(ingest.ArchivePayload) != 0 {
		t.Fatalf("active war should only schedule final store: %#v", ingest)
	}
	if len(ingest.Schedules) != 1 || ingest.Schedules[0].ScheduleKey == "" || ingest.Schedules[0].WarID != 0 || !ingest.Schedules[0].NextRunAt.Equal(end) {
		t.Fatalf("unexpected schedule: %#v", ingest.Schedules)
	}
	if got := ingest.PlayerTimers; len(got) != 2 || got[0].PlayerTag != "#P1" || got[1].PlayerTag != "#P2" || got[0].EventType != "war" || got[0].EventKey != ingest.Schedules[0].ScheduleKey || !got[0].ExpiresAt.Equal(end) {
		t.Fatalf("unexpected player timers: %#v", got)
	}
}

func TestMemoryWarStoreRetainsMultipleWarsForPlayer(t *testing.T) {
	store := newMemoryWarStore()
	now := time.Now().UTC()
	for _, row := range []models.PlayerTimerRow{
		{PlayerTag: "#P1", EventType: "war", EventKey: "war-one", ExpiresAt: now.Add(time.Hour)},
		{PlayerTag: "#P1", EventType: "war", EventKey: "war-two", ExpiresAt: now.Add(2 * time.Hour)},
	} {
		if err := store.Store(context.Background(), models.WarIngest{PlayerTimers: []models.PlayerTimerRow{row}}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.LoadActivePlayerTimers(context.Background(), "#P1")
	if err != nil || len(got) != 2 {
		t.Fatalf("timers = %#v, %v", got, err)
	}
}

func TestPlayerTimerCleanupAndActiveReads(t *testing.T) {
	if playerTimerCleanupInterval != 5*time.Minute {
		t.Fatalf("cleanup interval = %s", playerTimerCleanupInterval)
	}
	if !strings.Contains(deleteExpiredPlayerTimersSQL, "expires_at <= now()") || !strings.Contains(loadActivePlayerTimersSQL, "expires_at > now()") {
		t.Fatalf("timer SQL must clean expired and read active only: cleanup=%s read=%s", deleteExpiredPlayerTimersSQL, loadActivePlayerTimersSQL)
	}
	store := newMemoryWarStore()
	now := time.Now().UTC()
	err := store.Store(context.Background(), models.WarIngest{PlayerTimers: []models.PlayerTimerRow{
		{PlayerTag: "#EXPIRED", EventType: "war", EventKey: "war-old", ExpiresAt: now.Add(-time.Second)},
		{PlayerTag: "#ACTIVE", EventType: "war", EventKey: "war-live", ExpiresAt: now.Add(time.Hour)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := store.LoadActivePlayerTimers(context.Background(), "#EXPIRED"); err != nil || len(got) != 0 {
		t.Fatalf("expired read = %#v err:%v", got, err)
	}
	if got, err := store.LoadActivePlayerTimers(context.Background(), "#ACTIVE"); err != nil || len(got) != 1 || got[0].EventKey != "war-live" {
		t.Fatalf("active read = %#v err:%v", got, err)
	}
	deleted, err := store.DeleteExpiredPlayerTimers(context.Background())
	if err != nil || deleted != 1 {
		t.Fatalf("cleanup = %d, %v", deleted, err)
	}
}

func TestAbandonedWarScheduleDeletesOnlyItsTimers(t *testing.T) {
	store := newMemoryWarStore()
	now := time.Now().UTC()
	if err := store.Store(context.Background(), models.WarIngest{
		Schedules: []models.WarScheduleRow{{ScheduleKey: "missing-war", WarID: 42, SourceClanTag: "#A", OpponentTag: "#B", EndTime: now}},
		PlayerTimers: []models.PlayerTimerRow{
			{PlayerTag: "#P1", EventType: "war", EventKey: "missing-war", ExpiresAt: now},
			{PlayerTag: "#P1", EventType: "raid", EventKey: "#A", ExpiresAt: now.Add(time.Hour)},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSchedule(context.Background(), "missing-war"); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.schedules["missing-war"]; ok {
		t.Fatal("abandoned war schedule was retained")
	}
	if _, ok := store.playerTimers["#P1|war|missing-war"]; ok {
		t.Fatal("abandoned war timer was retained")
	}
	if _, ok := store.playerTimers["#P1|raid|#A"]; !ok {
		t.Fatal("unrelated raid timer was removed")
	}
}

func TestPlayerTimerBulkUpsertSQL(t *testing.T) {
	if !strings.Contains(upsertPlayerTimersSQL, "unnest(") || strings.Contains(upsertPlayerTimersSQL, "pgx.Batch") || !strings.Contains(upsertPlayerTimersSQL, "ON CONFLICT (player_tag, event_type, event_key)") {
		t.Fatalf("player timers must use one bulk upsert: %s", upsertPlayerTimersSQL)
	}
}

func TestBuildWarIngestFinishedAddsPermanentRows(t *testing.T) {
	prep := time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)
	war := sampleWar(prep, prep.Add(time.Hour), prep.Add(2*time.Hour))

	ingest, err := buildWarIngest(war, "#AAA", true, "#WAR", "#AAA-#BBB-1", 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.Schedules) != 0 {
		t.Fatalf("finished ingest should not reschedule: %#v", ingest.Schedules)
	}
	if ingest.FinishedScheduleKey != "#AAA-#BBB-1" || ingest.FinishedWarID != 42 {
		t.Fatalf("missing finished-war fields: %#v", ingest)
	}
	if len(ingest.IndexRows) != 1 {
		t.Fatalf("IndexRows len = %d, want 1", len(ingest.IndexRows))
	}
	if ingest.IndexRows[0].AttacksPerMember != 1 {
		t.Fatalf("AttacksPerMember = %d, want 1 for a CWL war", ingest.IndexRows[0].AttacksPerMember)
	}
	if ingest.IndexRows[0].BattleModifier != wararchive.BattleModifierNone {
		t.Fatalf("BattleModifier = %q, want %q", ingest.IndexRows[0].BattleModifier, wararchive.BattleModifierNone)
	}
	archived, err := wararchive.Unmarshal(ingest.ArchivePayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(archived.Clan.Members) != 1 || len(archived.Clan.Members[0].Attacks) != 1 || archived.Clan.Members[0].Attacks[0].DefenderTag != "#P2" {
		t.Fatalf("unexpected archive payload: %#v", archived)
	}
	if archived.BattleModifier != wararchive.BattleModifierNone {
		t.Fatalf("archived BattleModifier = %q, want %q", archived.BattleModifier, wararchive.BattleModifierNone)
	}
	if !reflect.DeepEqual(ingest.ArchiveParticipants, []string{"#P1", "#P2"}) {
		t.Fatalf("archive participants = %#v", ingest.ArchiveParticipants)
	}
}

func TestBuildWarIngestPreservesCanonicalBattleModifier(t *testing.T) {
	prep := time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)
	war := sampleWar(prep, prep.Add(time.Hour), prep.Add(2*time.Hour))
	war.BattleModifier = clashy.BattleModifierHardMode

	ingest, err := buildWarIngest(war, "#AAA", true, "#WAR", "#AAA-#BBB-1", 42)
	if err != nil {
		t.Fatal(err)
	}
	archived, err := wararchive.Unmarshal(ingest.ArchivePayload)
	if err != nil {
		t.Fatal(err)
	}
	if ingest.IndexRows[0].BattleModifier != wararchive.BattleModifierHardMode || archived.BattleModifier != wararchive.BattleModifierHardMode {
		t.Fatalf("battle modifiers = index %q, archive %q; want %q", ingest.IndexRows[0].BattleModifier, archived.BattleModifier, wararchive.BattleModifierHardMode)
	}
}

func TestBuildWarIngestRejectsFinishedWarWithoutStartTime(t *testing.T) {
	prep := time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)
	war := sampleWar(prep, prep.Add(time.Hour), prep.Add(2*time.Hour))
	war.StartTime = nil
	if _, err := buildWarIngest(war, "#AAA", true, "", "#AAA-#BBB-1", 42); err == nil {
		t.Fatal("finished war without startTime was accepted")
	}
}

func TestBuildWarIngestRejectsFinishedWarWithoutSQLID(t *testing.T) {
	prep := time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)
	war := sampleWar(prep, prep.Add(time.Hour), prep.Add(2*time.Hour))
	if _, err := buildWarIngest(war, "#AAA", true, "", "#AAA-#BBB-1", 0); err == nil {
		t.Fatal("finished war without its SQL war ID was accepted")
	}
}

func TestBuildWarIngestUsesTwoAttacksForRegularWar(t *testing.T) {
	prep := time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)
	war := sampleWar(prep, prep.Add(time.Hour), prep.Add(2*time.Hour))
	ingest, err := buildWarIngest(war, "#AAA", true, "", "#AAA-#BBB-1", 42)
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.IndexRows) != 1 || ingest.IndexRows[0].AttacksPerMember != 2 {
		t.Fatalf("regular war index rows = %#v, want attacks_per_member 2", ingest.IndexRows)
	}
}

func TestScheduledWarIdentityAcceptsEitherPerspectiveAndRejectsNextWar(t *testing.T) {
	prep := time.Date(2026, 8, 15, 5, 51, 49, 0, time.UTC)
	war := sampleWar(prep, prep.Add(24*time.Hour), prep.Add(48*time.Hour))
	request := warFetchRequest{
		ClanTag: "#AAA", OpponentTag: "#BBB", StoreOnly: true,
		ScheduleKey: models.ComputeWarKey("#AAA", "#BBB", prep),
	}
	if !scheduledWarMatches(request, war) {
		t.Fatal("the scheduled war should match from its canonical tags and preparation time")
	}
	war.Clan, war.Opponent = war.Opponent, war.Clan
	if !scheduledWarMatches(request, war) {
		t.Fatal("the opponent perspective should resolve to the same schedule")
	}
	war.PreparationStartTime = &clashy.Timestamp{Time: prep.Add(48 * time.Hour)}
	if scheduledWarMatches(request, war) {
		t.Fatal("a later war between the same clans must not finalize the old schedule")
	}
	war.PreparationStartTime = &clashy.Timestamp{Time: prep}
	war.State = clashy.WarStateNotInWar
	if scheduledWarIsUsable(request, war) {
		t.Fatal("a cancelled partial war must not be treated as a pending scheduled war")
	}
}

func TestDueWarTriesBothClansThenAbandonsUnrecoverableSchedule(t *testing.T) {
	for _, test := range []struct {
		name    string
		respond func(http.ResponseWriter, models.WarScheduleRow)
	}{
		{name: "private", respond: func(response http.ResponseWriter, _ models.WarScheduleRow) {
			response.WriteHeader(http.StatusForbidden)
			_, _ = response.Write([]byte(`{"reason":"accessDenied","message":"Access denied"}`))
		}},
		{name: "newer war", respond: func(response http.ResponseWriter, schedule models.WarScheduleRow) {
			newer := schedule
			newer.PrepTime = newer.PrepTime.Add(48 * time.Hour)
			newer.EndTime = newer.EndTime.Add(48 * time.Hour)
			_ = json.NewEncoder(response).Encode(dueWarTestPayload(newer, "warEnded"))
		}},
		{name: "cancelled partial war", respond: func(response http.ResponseWriter, _ models.WarScheduleRow) {
			_ = json.NewEncoder(response).Encode(map[string]any{"state": "notInWar"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			schedule := dueWarTestSchedule(time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC))
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				response.Header().Set("Content-Type", "application/json")
				test.respond(response, schedule)
			}))
			defer server.Close()

			domain, app, store := newDueWarTestDomain(t, server.URL)
			if err := store.Store(t.Context(), models.WarIngest{Schedules: []models.WarScheduleRow{schedule}}); err != nil {
				t.Fatal(err)
			}
			schedule = store.schedules[schedule.ScheduleKey]
			if err := domain.processDueWarSchedule(t.Context(), app, "war-archiver.finalization", schedule); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatalf("war fetches = %d, want source clan plus opponent", calls.Load())
			}
			if _, exists := store.schedules[schedule.ScheduleKey]; exists {
				t.Fatal("unrecoverable schedule was retained")
			}
		})
	}
}

func TestDueWarFallsBackToOpponentForEveryUnusableSourceResponse(t *testing.T) {
	for _, test := range []struct {
		name   string
		source func(http.ResponseWriter, models.WarScheduleRow)
	}{
		{name: "private", source: func(response http.ResponseWriter, _ models.WarScheduleRow) {
			response.WriteHeader(http.StatusForbidden)
			_, _ = response.Write([]byte(`{"reason":"accessDenied","message":"Access denied"}`))
		}},
		{name: "newer war", source: func(response http.ResponseWriter, schedule models.WarScheduleRow) {
			newer := schedule
			newer.PrepTime = newer.PrepTime.Add(48 * time.Hour)
			newer.EndTime = newer.EndTime.Add(48 * time.Hour)
			_ = json.NewEncoder(response).Encode(dueWarTestPayload(newer, "warEnded"))
		}},
		{name: "cancelled partial war", source: func(response http.ResponseWriter, _ models.WarScheduleRow) {
			_ = json.NewEncoder(response).Encode(map[string]any{"state": "notInWar"})
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
			schedule := dueWarTestSchedule(now)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				response.Header().Set("Content-Type", "application/json")
				if strings.Contains(request.URL.Path, "#AAA") {
					test.source(response, schedule)
					return
				}
				_ = json.NewEncoder(response).Encode(dueWarTestPayload(schedule, "warEnded"))
			}))
			defer server.Close()

			domain, app, store := newDueWarTestDomain(t, server.URL)
			if err := store.Store(t.Context(), models.WarIngest{Schedules: []models.WarScheduleRow{schedule}}); err != nil {
				t.Fatal(err)
			}
			schedule = store.schedules[schedule.ScheduleKey]
			if err := domain.processDueWarSchedule(t.Context(), app, "war-archiver.finalization", schedule); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatalf("war fetches = %d, want unusable source plus public opponent", calls.Load())
			}
			if len(store.indexRows) != 1 {
				t.Fatalf("stored wars = %d, want opponent-visible finished war", len(store.indexRows))
			}
			if _, exists := store.schedules[schedule.ScheduleKey]; exists {
				t.Fatal("stored war schedule was retained")
			}
		})
	}
}

func TestDueWarUsesCacheExpiryWhileMatchingWarRemainsUnfinished(t *testing.T) {
	now := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
	schedule := dueWarTestSchedule(now)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "public, max-age=17")
		_ = json.NewEncoder(response).Encode(dueWarTestPayload(schedule, "inWar"))
	}))
	defer server.Close()

	domain, app, store := newDueWarTestDomain(t, server.URL)
	if err := store.Store(t.Context(), models.WarIngest{Schedules: []models.WarScheduleRow{schedule}}); err != nil {
		t.Fatal(err)
	}
	schedule = store.schedules[schedule.ScheduleKey]
	const checks = 4
	for attempt := 1; attempt <= checks; attempt++ {
		if err := domain.processDueWarSchedule(t.Context(), app, "war-archiver.finalization", schedule); err != nil {
			t.Fatal(err)
		}
		stored, exists := store.schedules[schedule.ScheduleKey]
		if !exists {
			t.Fatalf("matching unfinished schedule removed after check %d", attempt)
		}
		if want := now.Add(17 * time.Second); !stored.NextRunAt.Equal(want) {
			t.Fatalf("retry time = %s, want cache expiry %s", stored.NextRunAt, want)
		}
		schedule = stored
	}
	if calls.Load() != int32(checks+1) {
		t.Fatalf("unfinished war fetches = %d, want two initial views then one per retry (%d)", calls.Load(), checks+1)
	}
}

func TestDueWarAbandonsWhenBothLogsBecomePrivateAfterCacheRetry(t *testing.T) {
	now := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
	schedule := dueWarTestSchedule(now)
	var calls int
	var callsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		callsMu.Lock()
		calls++
		call := calls
		callsMu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		if call <= 2 {
			response.Header().Set("Cache-Control", "public, max-age=17")
			_ = json.NewEncoder(response).Encode(dueWarTestPayload(schedule, "inWar"))
			return
		}
		response.WriteHeader(http.StatusForbidden)
		_, _ = response.Write([]byte(`{"reason":"accessDenied","message":"Access denied"}`))
	}))
	defer server.Close()

	domain, app, store := newDueWarTestDomain(t, server.URL)
	if err := store.Store(t.Context(), models.WarIngest{Schedules: []models.WarScheduleRow{schedule}}); err != nil {
		t.Fatal(err)
	}
	schedule = store.schedules[schedule.ScheduleKey]
	if err := domain.processDueWarSchedule(t.Context(), app, "war-archiver.finalization", schedule); err != nil {
		t.Fatal(err)
	}
	if _, exists := store.schedules[schedule.ScheduleKey]; !exists {
		t.Fatal("matching unfinished war was not retained for its cache retry")
	}
	schedule = store.schedules[schedule.ScheduleKey]
	if err := domain.processDueWarSchedule(t.Context(), app, "war-archiver.finalization", schedule); err != nil {
		t.Fatal(err)
	}
	if calls != 4 {
		t.Fatalf("war fetches = %d, want both initial visible perspectives plus both private perspectives", calls)
	}
	if _, exists := store.schedules[schedule.ScheduleKey]; exists {
		t.Fatal("schedule was retained after both war logs became private")
	}
}

func TestInitialRegularWarChoosesShortestMatchingCacheAndPersistsPerspective(t *testing.T) {
	now := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
	schedule := dueWarTestSchedule(now)
	var calls int
	var callsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		if strings.Contains(request.URL.Path, "#BBB") {
			response.Header().Set("Cache-Control", "public, max-age=11")
		} else {
			response.Header().Set("Cache-Control", "public, max-age=47")
		}
		_ = json.NewEncoder(response).Encode(dueWarTestPayload(schedule, "inWar"))
	}))
	defer server.Close()

	domain, app, store := newDueWarTestDomain(t, server.URL)
	if err := store.Store(t.Context(), models.WarIngest{Schedules: []models.WarScheduleRow{schedule}}); err != nil {
		t.Fatal(err)
	}
	schedule = store.schedules[schedule.ScheduleKey]
	if err := domain.processDueWarSchedule(t.Context(), app, "war-archiver.finalization", schedule); err != nil {
		t.Fatal(err)
	}
	stored := store.schedules[schedule.ScheduleKey]
	if calls != 2 {
		t.Fatalf("initial regular-war requests = %d, want one for each clan", calls)
	}
	if stored.SourceClanTag != "#BBB" || stored.OpponentTag != "#AAA" {
		t.Fatalf("persisted perspective = %s then %s, want shorter-cache #BBB then #AAA", stored.SourceClanTag, stored.OpponentTag)
	}
	if want := now.Add(11 * time.Second); !stored.NextRunAt.Equal(want) {
		t.Fatalf("retry time = %s, want %s", stored.NextRunAt, want)
	}
}

func TestRegularWarRetryUsesPreferredPerspectiveThenFallsBack(t *testing.T) {
	now := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
	schedule := dueWarTestSchedule(now)
	schedule.SourceClanTag, schedule.OpponentTag = "#BBB", "#AAA"
	schedule.NextRunAt = now.Add(time.Minute)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		response.Header().Set("Content-Type", "application/json")
		if strings.Contains(request.URL.Path, "#BBB") {
			response.WriteHeader(http.StatusForbidden)
			_, _ = response.Write([]byte(`{"reason":"accessDenied","message":"Access denied"}`))
			return
		}
		_ = json.NewEncoder(response).Encode(dueWarTestPayload(schedule, "warEnded"))
	}))
	defer server.Close()

	domain, app, store := newDueWarTestDomain(t, server.URL)
	if err := store.Store(t.Context(), models.WarIngest{Schedules: []models.WarScheduleRow{schedule}}); err != nil {
		t.Fatal(err)
	}
	schedule = store.schedules[schedule.ScheduleKey]
	if err := domain.processDueWarSchedule(t.Context(), app, "war-archiver.finalization", schedule); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || !strings.Contains(paths[0], "#BBB") || !strings.Contains(paths[1], "#AAA") {
		t.Fatalf("fetch order = %#v, want preferred #BBB then fallback #AAA", paths)
	}
	if len(store.indexRows) != 1 {
		t.Fatalf("stored wars = %d, want fallback result stored", len(store.indexRows))
	}
}

func TestInitialRegularWarStoresEndedViewEvenWhenOtherViewIsUnfinished(t *testing.T) {
	now := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
	schedule := dueWarTestSchedule(now)
	var calls int
	var callsMu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		callsMu.Lock()
		calls++
		callsMu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		state := "inWar"
		if strings.Contains(request.URL.Path, "#BBB") {
			state = "warEnded"
		}
		_ = json.NewEncoder(response).Encode(dueWarTestPayload(schedule, state))
	}))
	defer server.Close()

	domain, app, store := newDueWarTestDomain(t, server.URL)
	if err := store.Store(t.Context(), models.WarIngest{Schedules: []models.WarScheduleRow{schedule}}); err != nil {
		t.Fatal(err)
	}
	schedule = store.schedules[schedule.ScheduleKey]
	if err := domain.processDueWarSchedule(t.Context(), app, "war-archiver.finalization", schedule); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(store.indexRows) != 1 {
		t.Fatalf("initial requests = %d, stored wars = %d; want two requests and the ended result", calls, len(store.indexRows))
	}
}

func TestCWLScheduleTimingAndFiveMinutePendingRetry(t *testing.T) {
	now := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
	if got, want := warScheduleNextRunAt(now.Add(time.Hour), "#WAR", now), now.Add(time.Hour+cwlFinalizationDelay); !got.Equal(want) {
		t.Fatalf("future CWL next run = %s, want %s", got, want)
	}
	if got, want := warScheduleNextRunAt(now.Add(-time.Hour), "#WAR", now), now.Add(-time.Hour); !got.Equal(want) {
		t.Fatalf("ended CWL next run = %s, want immediate due time %s", got, want)
	}

	schedule := dueWarTestSchedule(now)
	schedule.WarTag = "#WAR"
	schedule.WarType = "cwl"
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		response.Header().Set("Cache-Control", "public, max-age=600")
		_ = json.NewEncoder(response).Encode(dueWarTestPayload(schedule, "inWar"))
	}))
	defer server.Close()

	domain, app, store := newDueWarTestDomain(t, server.URL)
	if err := store.Store(t.Context(), models.WarIngest{Schedules: []models.WarScheduleRow{schedule}}); err != nil {
		t.Fatal(err)
	}
	schedule = store.schedules[schedule.ScheduleKey]
	if err := domain.processDueWarSchedule(t.Context(), app, "war-archiver.finalization", schedule); err != nil {
		t.Fatal(err)
	}
	stored := store.schedules[schedule.ScheduleKey]
	if want := now.Add(cwlFinalizationRetry); !stored.NextRunAt.Equal(want) {
		t.Fatalf("unfinished CWL retry = %s, want %s", stored.NextRunAt, want)
	}
}

func newDueWarTestDomain(t *testing.T, baseURL string) (*warsDomain, *platform.App, *memoryWarStore) {
	t.Helper()
	clientConfig := clashy.DefaultClientConfig()
	clientConfig.BaseURL = baseURL + "/v1"
	clientConfig.LookupCache = false
	clientConfig.UpdateCache = false
	client, err := clashy.NewClient(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := newTrackingLimiter(100)
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryWarStore()
	now := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC)
	domain := &warsDomain{
		name: warDiscoveryDomainName, mode: warDiscoveryMode, store: store, limiter: limiter,
		now: func() time.Time { return now }, scheduled: make(map[string]time.Time),
	}
	app := &platform.App{
		Clash: client, Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return domain, app, store
}

func dueWarTestSchedule(end time.Time) models.WarScheduleRow {
	prep := end.Add(-25 * time.Hour)
	return models.WarScheduleRow{
		ScheduleKey: models.ComputeWarKey("#AAA", "#BBB", prep), WarID: 42,
		SourceClanTag: "#AAA", OpponentTag: "#BBB", PrepTime: prep,
		EndTime: end, NextRunAt: end, WarType: "random",
	}
}

func dueWarTestPayload(schedule models.WarScheduleRow, state string) map[string]any {
	return map[string]any{
		"state": state, "teamSize": 15,
		"preparationStartTime": schedule.PrepTime.Format("20060102T150405.000Z"),
		"startTime":            schedule.PrepTime.Add(time.Hour).Format("20060102T150405.000Z"),
		"endTime":              schedule.EndTime.Format("20060102T150405.000Z"),
		"clan":                 map[string]any{"tag": "#AAA", "name": "Alpha", "members": []any{}},
		"opponent":             map[string]any{"tag": "#BBB", "name": "Beta", "members": []any{}},
	}
}

func TestBuildWarIngestSchedulesObservedEndedWarForImmediateFinalization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	war := sampleWar(now.Add(-3*time.Hour), now.Add(-2*time.Hour), now.Add(-time.Hour))

	ingest, err := buildWarIngest(war, "#AAA", false, "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ingest.Schedules) != 1 || ingest.Schedules[0].EndTime.After(now) || ingest.Schedules[0].WarTag != "" {
		t.Fatalf("ended observed war should create an immediately due finalization schedule: %#v", ingest.Schedules)
	}
	if len(ingest.IndexRows) != 0 || len(ingest.ArchivePayload) != 0 {
		t.Fatalf("ended observed war should still wait for the fenced finalizer fetch: %#v", ingest)
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
		"war_size = COALESCE(EXCLUDED.war_size, cwl_groups.war_size)",
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

func TestCWLSeasonMonthPreservesDatedProviderSeason(t *testing.T) {
	if got := cwlSeasonMonth("2026-09-03"); got != "2026-09" {
		t.Fatalf("season month = %q", got)
	}
	if got := cwlSeasonMonth("2026"); got != "" {
		t.Fatalf("short season month = %q", got)
	}
	for _, query := range []string{cwlDiscoveryTargetsSQL, cwlRefreshTargetsSQL} {
		if strings.Contains(query, "left(") || !strings.Contains(query, "season >= to_char") || !strings.Contains(query, "season < to_char") {
			t.Fatal("CWL target query does not use an indexable range that includes exact and dated stored seasons")
		}
	}
}

func TestGlobalCWLSyncSchedulesOverlappingBattleAndPreparationOnce(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	apiNow := time.Now().UTC().Truncate(time.Second)
	group := map[string]any{
		"state": "inWar", "season": "2026-08",
		"clans": []map[string]any{
			{"tag": "#AAA", "name": "Alpha", "clanLevel": 20, "members": []map[string]any{{"tag": "#P1", "name": "One", "townHallLevel": 18}}},
			{"tag": "#BBB", "name": "Beta", "clanLevel": 20, "members": []map[string]any{{"tag": "#P2", "name": "Two", "townHallLevel": 18}}},
		},
		"rounds": []map[string]any{{"warTags": []string{"#BATTLE"}}, {"warTags": []string{"#PREP"}}},
	}
	warPayload := func(state string, prep, start, end time.Time) map[string]any {
		return map[string]any{
			"state": state, "teamSize": 15,
			"preparationStartTime": prep.UTC().Format("20060102T150405.000Z"),
			"startTime":            start.UTC().Format("20060102T150405.000Z"),
			"endTime":              end.UTC().Format("20060102T150405.000Z"),
			"clan":                 map[string]any{"tag": "#AAA", "name": "Alpha", "members": []map[string]any{{"tag": "#P1", "name": "One", "townhallLevel": 18, "mapPosition": 1}}},
			"opponent":             map[string]any{"tag": "#BBB", "name": "Beta", "members": []map[string]any{{"tag": "#P2", "name": "Two", "townhallLevel": 18, "mapPosition": 1}}},
		}
	}
	wars := map[string]map[string]any{
		"#BATTLE": warPayload("inWar", apiNow.Add(-25*time.Hour), apiNow.Add(-24*time.Hour), apiNow.Add(time.Hour)),
		"#PREP":   warPayload("preparation", apiNow.Add(-time.Hour), apiNow.Add(23*time.Hour), apiNow.Add(47*time.Hour)),
	}
	var callsMu sync.Mutex
	calls := make(map[string]int)
	profileCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		key := path.Base(request.URL.Path)
		callsMu.Lock()
		calls[key]++
		callsMu.Unlock()
		response.Header().Set("Content-Type", "application/json")
		if strings.Contains(request.URL.Path, "leaguegroup") {
			_ = json.NewEncoder(response).Encode(group)
			return
		}
		if key == "#AAA" {
			callsMu.Lock()
			profileCalls++
			attempt := profileCalls
			callsMu.Unlock()
			if attempt == 1 {
				http.NotFound(response, request)
				return
			}
			_ = json.NewEncoder(response).Encode(map[string]any{"tag": "#AAA", "name": "Alpha", "warLeague": map[string]any{"id": 48000012, "name": "Master League I"}})
			return
		}
		war, ok := wars[key]
		if !ok {
			http.NotFound(response, request)
			return
		}
		_ = json.NewEncoder(response).Encode(war)
	}))
	defer server.Close()

	clientConfig := clashy.DefaultClientConfig()
	clientConfig.BaseURL = server.URL + "/v1"
	clientConfig.LookupCache = false
	clientConfig.UpdateCache = false
	client, err := clashy.NewClient(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryWarStore()
	store.targets = []models.BasicClanRow{{Tag: "#AAA", CWLLeagueID: 48000012}}
	domain := &warsDomain{
		name: cwlDomainName, mode: cwlMode, store: store,
		targets: newMemoryWarTargetSource(store.targets), now: func() time.Time { return now },
		scheduled: make(map[string]time.Time),
	}
	app := &platform.App{
		Config:       platform.Config{MockDB: true, CWLRequestsPerSecond: 100, TargetPageMultiplier: 5, CWLResolveLeagueFromClanProfile: true},
		Clash:        client,
		Stats:        platform.NewTracker(),
		Availability: platform.NewAvailabilityGate(nil),
	}
	limiter, err := newTrackingLimiter(100)
	if err != nil {
		t.Fatal(err)
	}
	domain.refreshWarTargetCounts(t.Context(), app)
	if err := domain.syncCWLGroups(t.Context(), app, limiter, limiter); err != nil {
		t.Fatal(err)
	}
	progress := app.Stats.Domain("cwl.groups")
	if progress.TargetCount != 0 || progress.TargetCycle != 1 || progress.TargetProcessed != 1 {
		t.Fatalf("CWL progress = %#v", progress)
	}
	if len(store.schedules) != 0 || calls["#BATTLE"] != 0 || calls["#PREP"] != 0 {
		t.Fatal("group discovery must not fetch or schedule war payloads")
	}
	for id, group := range store.cwlGroups {
		size, err := domain.scheduleCWLWars(t.Context(), app, limiter, cwlGroupFromRounds(group.Rounds), true)
		if err != nil {
			t.Fatal(err)
		}
		group.WarSize = intPtr(size)
		store.cwlGroups[id] = group
	}
	if len(store.schedules) != 2 {
		t.Fatalf("overlapping schedules = %d, want battle and preparation", len(store.schedules))
	}
	seenWarTags := map[string]bool{}
	for _, schedule := range store.schedules {
		seenWarTags[schedule.WarTag] = true
	}
	if !seenWarTags["#BATTLE"] || !seenWarTags["#PREP"] {
		t.Fatalf("scheduled war tags = %#v", seenWarTags)
	}
	if len(store.cwlGroups) != 1 {
		t.Fatalf("stored groups = %d, want 1", len(store.cwlGroups))
	}
	for _, stored := range store.cwlGroups {
		if stored.WarSize == nil || *stored.WarSize != 15 || stored.CWLLeagueID != nil {
			t.Fatalf("stored group dimensions = %#v", stored)
		}
	}
	restartedDomain := &warsDomain{
		name: cwlDomainName, mode: cwlMode, store: store,
		targets: newMemoryWarTargetSource(store.targets), now: func() time.Time { return now },
		scheduled: make(map[string]time.Time),
	}
	if err := restartedDomain.syncCWLGroups(t.Context(), app, limiter, limiter); err != nil {
		t.Fatal(err)
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if calls["#BATTLE"] != 1 || calls["#PREP"] != 1 {
		t.Fatalf("known tagged wars were refetched: calls=%#v", calls)
	}
	if profileCalls != 2 {
		t.Fatalf("profile lookups = %d, want failed discovery lookup plus one restart retry", profileCalls)
	}
	for _, stored := range store.cwlGroups {
		if stored.CWLLeagueID == nil || *stored.CWLLeagueID != 48000012 {
			t.Fatalf("retry did not persist resolved league: %#v", stored)
		}
	}
}

func TestCWLProfileLeagueLookupRunsOnceAndReusesPersistedGroup(t *testing.T) {
	now := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	group := map[string]any{
		"state": "preparation", "season": "2026-08",
		"clans":  []map[string]any{{"tag": "#AAA", "name": "Alpha"}, {"tag": "#BBB", "name": "Beta"}},
		"rounds": []map[string]any{},
	}
	var mu sync.Mutex
	profileCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		if strings.Contains(request.URL.Path, "leaguegroup") {
			_ = json.NewEncoder(response).Encode(group)
			return
		}
		if strings.Contains(request.URL.Path, "/clans/") {
			mu.Lock()
			profileCalls++
			mu.Unlock()
			_ = json.NewEncoder(response).Encode(map[string]any{"tag": path.Base(request.URL.Path), "name": "Representative", "warLeague": map[string]any{"id": 48000012, "name": "Master League I"}})
			return
		}
		http.NotFound(response, request)
	}))
	defer server.Close()

	clientConfig := clashy.DefaultClientConfig()
	clientConfig.BaseURL = server.URL + "/v1"
	clientConfig.LookupCache = false
	clientConfig.UpdateCache = false
	client, err := clashy.NewClient(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryWarStore()
	targets := []models.BasicClanRow{{Tag: "#AAA"}, {Tag: "#BBB"}}
	store.targets = targets
	app := &platform.App{
		Config: platform.Config{MockDB: true, CWLRequestsPerSecond: 100, TargetPageMultiplier: 5, CWLResolveLeagueFromClanProfile: true},
		Clash:  client, Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil),
	}
	limiter, err := newTrackingLimiter(100)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		domain := &warsDomain{
			name: cwlDomainName, mode: cwlMode, store: store,
			targets: newMemoryWarTargetSource(targets), now: func() time.Time { return now },
			scheduled: make(map[string]time.Time),
		}
		if err := domain.syncCWLGroups(t.Context(), app, limiter, limiter); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if profileCalls != 1 {
		t.Fatalf("profile lookups = %d, want one across refresh/restart", profileCalls)
	}
}

func TestGlobalCWLSyncUsesBoundedConcurrentGroupRequests(t *testing.T) {
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	targets := []models.BasicClanRow{
		{Tag: "#AAA", CWLLeagueID: 1}, {Tag: "#BBB", CWLLeagueID: 1},
		{Tag: "#CCC", CWLLeagueID: 1}, {Tag: "#DDD", CWLLeagueID: 1},
		{Tag: "#EEE", CWLLeagueID: 1}, {Tag: "#FFF", CWLLeagueID: 1},
	}
	var callsMu sync.Mutex
	active, maxActive := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		callsMu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		callsMu.Unlock()
		time.Sleep(40 * time.Millisecond)
		tag := path.Base(path.Dir(path.Dir(request.URL.Path)))
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]any{
			"state": "preparation", "season": "2026-08",
			"clans":  []map[string]any{{"tag": tag, "name": tag}},
			"rounds": []map[string]any{},
		})
		callsMu.Lock()
		active--
		callsMu.Unlock()
	}))
	defer server.Close()

	clientConfig := clashy.DefaultClientConfig()
	clientConfig.BaseURL = server.URL + "/v1"
	clientConfig.LookupCache = false
	clientConfig.UpdateCache = false
	client, err := clashy.NewClient(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	store := newMemoryWarStore()
	store.targets = targets
	domain := &warsDomain{
		name: cwlDomainName, mode: cwlMode, store: store,
		targets: newMemoryWarTargetSource(targets), now: func() time.Time { return now },
		scheduled: make(map[string]time.Time),
	}
	app := &platform.App{
		Config: platform.Config{MockDB: true, CWLRequestsPerSecond: 100, TargetPageMultiplier: 5},
		Clash:  client, Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil),
	}
	limiter, err := newTrackingLimiter(100)
	if err != nil {
		t.Fatal(err)
	}
	if err := domain.syncCWLGroups(t.Context(), app, limiter, limiter); err != nil {
		t.Fatal(err)
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if maxActive < 2 {
		t.Fatalf("global CWL group requests remained sequential: max concurrent = %d", maxActive)
	}
}

func TestMemoryWarStoreShiftMaintenance(t *testing.T) {
	store := newMemoryWarStore()
	now := time.Now().UTC()
	err := store.Store(context.Background(), models.WarIngest{
		Schedules: []models.WarScheduleRow{
			{ScheduleKey: "#A-#B-1", WarID: 1, SourceClanTag: "#A", OpponentTag: "#B",
				PrepTime: now, EndTime: now.Add(time.Hour), NextRunAt: now.Add(time.Hour),
			},
			{ScheduleKey: "#C-#D-1", WarID: 2, SourceClanTag: "#C", OpponentTag: "#D",
				PrepTime: now.Add(-2 * time.Hour), EndTime: now.Add(-time.Hour), NextRunAt: now.Add(-time.Hour),
			},
		},
		PlayerTimers: []models.PlayerTimerRow{
			{PlayerTag: "#P1", EventType: "war", EventKey: "#A-#B-1", ExpiresAt: now.Add(time.Hour)},
			{PlayerTag: "#P2", EventType: "war", EventKey: "#A-#B-1", ExpiresAt: now.Add(time.Hour)},
			{PlayerTag: "#P3", EventType: "war", EventKey: "#C-#D-1", ExpiresAt: now.Add(-time.Hour)},
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
	if got := store.playerTimers["#P1|war|#A-#B-1"].ExpiresAt; !got.Equal(now.Add(time.Hour + 2*time.Minute)) {
		t.Fatalf("live timer shifted = %s", got)
	}
	if got := store.playerTimers["#P2|war|#A-#B-1"].ExpiresAt; !got.Equal(now.Add(time.Hour + 2*time.Minute)) {
		t.Fatalf("second participant shifted = %s", got)
	}
	if got := store.playerTimers["#P3|war|#C-#D-1"].ExpiresAt; !got.Equal(now.Add(-time.Hour)) {
		t.Fatalf("expired timer shifted = %s", got)
	}
}

func TestOfficialMaintenanceClassification(t *testing.T) {
	maintenance500 := &clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 500, Message: "Game maintenance"}}
	if !isOfficialMaintenance(maintenance500) {
		t.Fatal("official maintenance response must start maintenance")
	}
	if !isOfficialMaintenance(&clashy.Maintenance{HTTPException: &clashy.HTTPException{Status: 503}}) {
		t.Fatal("typed maintenance response must start maintenance")
	}
	for _, err := range []error{
		&clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 502}},
		&clashy.GatewayError{HTTPException: &clashy.HTTPException{Status: 500, Message: "proxy error"}},
		&clashy.Forbidden{HTTPException: &clashy.HTTPException{Status: 403}},
		errors.New("transport failure"),
	} {
		if isOfficialMaintenance(err) {
			t.Fatalf("non-maintenance error started maintenance: %v", err)
		}
	}
}

func TestShiftActiveWarMaintenanceSQLScopesSchedulesAndTimers(t *testing.T) {
	for _, fragment := range []string{
		"WITH shifted_wars", "WHERE end_time > now()", "RETURNING schedule_key", "timer.expires_at > now()", "timer.event_key IN",
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
