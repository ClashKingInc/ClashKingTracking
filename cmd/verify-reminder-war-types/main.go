// Exercise real reminder clock creation and cleanup against disposable stores.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"encoding/json"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	valkey "github.com/valkey-io/valkey-go"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func main() {
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" || os.Getenv("CLASHKING_DISPOSABLE_VALKEY") != "1" || !strings.Contains(dsn, "@127.0.0.1:") || !strings.HasPrefix(addr, "127.0.0.1:") {
		panic("disposable local stores required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	_, e = db.Exec(ctx, `INSERT INTO servers(id,name,last_command_at) VALUES('999999999999991901','War filter fixture',now());
 INSERT INTO reminders(server_id,type,type_name,clan_tag,webhook_token,minutes_remaining,war_type_names)
 SELECT '999999999999991901',1,'War','#PYLQRRJC','',m,ARRAY['Random','Friendly','CWL'] FROM unnest(ARRAY[720,240,60]) m;
 INSERT INTO war_schedule(schedule_key,source_clan_tag,opponent_tag,prep_time,end_time,next_run_at,war_type)
 SELECT 'case-'||t,'#PYLQRRJC','#PYLQ',now(),now()+interval '2 days',now()+interval '1 day',t FROM unnest(ARRAY['random','friendly','cwl']) t;`)
	must(e)
	var requests atomic.Int64
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "no provider access expected", 500)
	}))
	defer provider.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = provider.URL
	cfg.LookupCache = false
	cfg.UpdateCache = false
	client, e := clashy.NewClient(cfg)
	must(e)
	defer client.Close()
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, ReminderRequestsPerSecond: 10, EventStreamName: "case-filter-investigation", EventStreamReclaimIdleSeconds: 1}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	counts := func() map[string]int {
		run, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- scripts.NewRemindersDomain().Run(run, app) }()
		ready := false
		for !ready && ctx.Err() == nil {
			var n int
			must(db.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE query='LISTEN clashking_tracking_wake_v1'`).Scan(&n))
			ready = n > 0
			if !ready {
				select {
				case err := <-done:
					panic(err)
				case <-time.After(50 * time.Millisecond):
				}
			}
		}
		if !ready {
			panic("scheduler startup timeout")
		}
		rows, err := db.Query(ctx, `SELECT s.war_type,count(j.schedule_key)::int FROM war_schedule s LEFT JOIN war_reminder_jobs j USING(schedule_key) GROUP BY s.war_type`)
		must(err)
		out := map[string]int{}
		for rows.Next() {
			var typ string
			var n int
			must(rows.Scan(&typ, &n))
			out[typ] = n
		}
		must(rows.Err())
		rows.Close()
		stop()
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			panic("scheduler did not stop")
		}
		return out
	}
	expect := func(actual, want map[string]int) {
		for _, typ := range []string{"random", "friendly", "cwl"} {
			if actual[typ] != want[typ] {
				panic(fmt.Sprintf("%s clocks: got %d, want %d", typ, actual[typ], want[typ]))
			}
		}
	}
	all := map[string]int{"random": 3, "friendly": 3, "cwl": 3}
	titleCase := counts()
	expect(titleCase, all)
	_, e = db.Exec(ctx, `UPDATE reminders SET war_type_names=ARRAY['cWl'] WHERE clan_tag='#PYLQRRJC'`)
	must(e)
	restricted := counts()
	expect(restricted, map[string]int{"cwl": 3})
	_, e = db.Exec(ctx, `UPDATE reminders SET war_type_names=ARRAY['rAnDoM','fRiEnDlY','CwL'] WHERE clan_tag='#PYLQRRJC'`)
	must(e)
	mixed := counts()
	expect(mixed, all)
	_, e = db.Exec(ctx, `UPDATE reminders SET war_type_names='{}'::text[] WHERE clan_tag='#PYLQRRJC'`)
	must(e)
	empty := counts()
	expect(empty, all)
	_, e = db.Exec(ctx, `UPDATE reminders SET war_type_names=ARRAY['r%'] WHERE clan_tag='#PYLQRRJC'`)
	must(e)
	exact := counts()
	expect(exact, map[string]int{})
	if requests.Load() != 0 {
		panic("unexpected provider request")
	}
	result := map[string]any{"environment": "real reminders domain, disposable canonical Timescale and Valkey", "production_writes": 0, "configured_offsets_minutes": []int{720, 240, 60}, "display_case_jobs": titleCase, "restricted_mixed_case_jobs": restricted, "mixed_case_jobs": mixed, "empty_filter_jobs": empty, "wildcard_label_is_not_a_match": exact, "external_provider_requests": requests.Load()}

	raw, e := json.MarshalIndent(result, "", "  ")
	must(e)
	fmt.Println(string(raw))
	if p := os.Getenv("CLASHKING_E2E_RESULT"); p != "" {
		must(os.WriteFile(p, append(raw, '\n'), 0644))
	}
}
