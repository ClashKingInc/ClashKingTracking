//go:build script_internal_tests

package scripts

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"clashking_tracking/models"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
)

func TestCWLTagDiffIgnoresExistingDuplicatesAndPlaceholders(t *testing.T) {
	got := newCWLWarTags([]string{"#A"}, []string{"#A", "#0", "", "#B", "#B", "#C"})
	if !reflect.DeepEqual(got, []string{"#B", "#C"}) {
		t.Fatal(got)
	}
}

func TestCWLMemoryPagesTerminateEvenAtExactPageBoundary(t *testing.T) {
	source := newMemoryWarTargetSource([]models.BasicClanRow{{Tag: "#A"}, {Tag: "#B"}, {Tag: "#C"}, {Tag: "#D"}})
	for _, want := range []int{2, 2, 0, 2, 2, 0} {
		page, err := source.NextCWLDiscoveryTargetBatch(t.Context(), 2)
		if err != nil || len(page) != want {
			t.Fatal(page, err)
		}
	}
}

// Actual local Valkey -> HTTP -> Timescale handoff. No provider requests.
func TestCWLHandoffStoresEndedWarOnceAndSchedulesFutureAtPlus30(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" || os.Getenv("TEST_VALKEY_ADDR") == "" {
		t.Skip("requires disposable SQL and local Valkey")
	}
	pool, err := pgxpool.New(t.Context(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{os.Getenv("TEST_VALKEY_ADDR")}, SelectDB: 15})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	exists, err := client.Do(t.Context(), client.B().Arbitrary("EXISTS").Keys(cwlWarQueueKey).Build()).AsInt64()
	if err != nil || exists != 0 {
		t.Fatalf("test queue must be empty: %d %v", exists, err)
	}
	defer client.Do(context.Background(), client.B().Arbitrary("DEL").Keys(cwlWarQueueKey).Build())
	_, err = pool.Exec(t.Context(), `INSERT INTO cwl_groups(cwl_id,season,rounds,state) VALUES ('handoff00001','2026-09-03','[]','ended')`)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	defer pool.Exec(context.Background(), `DELETE FROM war_schedule WHERE war_tag IN ('#D0NE','#FUTURE'); DELETE FROM wars WHERE war_tag IN ('#D0NE','#FUTURE'); DELETE FROM cwl_groups WHERE cwl_id='handoff00001'`)
	ended := dueWarTestSchedule(now.Add(-time.Hour))
	ended.WarTag = "#D0NE"
	future := dueWarTestSchedule(now.Add(time.Hour))
	future.WarTag = "#FUTURE"
	calls := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tag := r.URL.Path[len("/v1/clanwarleagues/wars/"):]
		calls[tag]++
		w.Header().Set("Content-Type", "application/json")
		if tag == "#D0NE" {
			json.NewEncoder(w).Encode(dueWarTestPayload(ended, "warEnded"))
		} else {
			json.NewEncoder(w).Encode(dueWarTestPayload(future, "inWar"))
		}
	}))
	defer server.Close()
	domain, app, _ := newDueWarTestDomain(t, server.URL)
	domain.store = &timescaleWarStore{pool: pool}
	app.Valkey = client
	app.Config.WarArchiveRequestsPerSecond = 1
	app.Config.MockDB = true // Suppress unrelated notification publication only; the store above is real SQL.
	for _, tag := range []string{"#D0NE", "#FUTURE"} {
		if err := enqueueCWLWarTags(t.Context(), app, "handoff00001", []string{tag}); err != nil {
			t.Fatal(err)
		}
		if err := domain.hydrateCWLPass(t.Context(), app, pool); err != nil {
			t.Fatal(err)
		}
	}
	// Replaying a job after a lost acknowledgement must not refetch the payload.
	if err := enqueueCWLWarTags(t.Context(), app, "handoff00001", []string{"#D0NE"}); err != nil {
		t.Fatal(err)
	}
	if err := domain.hydrateCWLPass(t.Context(), app, pool); err != nil {
		t.Fatal(err)
	}
	if calls["#D0NE"] != 1 || calls["#FUTURE"] != 1 {
		t.Fatal(calls)
	}
	queued, err := client.Do(t.Context(), client.B().Arbitrary("ZCARD").Keys(cwlWarQueueKey).Build()).AsInt64()
	if err != nil || queued != 0 {
		t.Fatalf("unacknowledged jobs: %d %v", queued, err)
	}
	var count int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM wars WHERE war_tag='#D0NE'`).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	var next time.Time
	if err := pool.QueryRow(t.Context(), `SELECT next_run_at FROM war_schedule WHERE war_tag='#FUTURE'`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if !next.Equal(future.EndTime.Add(30 * time.Minute)) {
		t.Fatalf("next=%s", next)
	}
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM war_schedule WHERE war_tag='#D0NE'`).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
}
