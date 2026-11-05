// Exercise real reminder startup across pages with a single SQL connection.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"fmt"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/valkey-io/valkey-go"
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
func check(ok bool, s string) {
	if !ok {
		panic(s)
	}
}
func main() {
	dsn, addr := os.Getenv("TEST_DATABASE_URL"), os.Getenv("TEST_VALKEY_ADDR")
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && os.Getenv("CLASHKING_DISPOSABLE_VALKEY") == "1" && strings.Contains(dsn, "@127.0.0.1:") && strings.HasPrefix(addr, "127.0.0.1:"), "disposable loopback stores required")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	cache, e := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	must(e)
	defer cache.Close()
	_, e = db.Exec(ctx, `INSERT INTO servers(id,name) VALUES('999999999999993101','Startup fixture');
 INSERT INTO reminders(server_id,type,type_name,clan_tag,webhook_token,minutes_remaining) VALUES('999999999999993101',1,'War','#PYLQ','',60);
 INSERT INTO war_schedule(schedule_key,source_clan_tag,opponent_tag,prep_time,end_time,next_run_at,war_type)
 SELECT 'startup-'||lpad(i::text,4,'0'),'#PYLQ','#PYYY',now(),now()+interval '2 days',now()+interval '1 day','random' FROM generate_series(1,501)i;`)
	must(e)
	// An audience removal must still remove an already scheduled job. The
	// other unrelated wars have no work and must not delay reminder startup.
	_, e = db.Exec(ctx, `INSERT INTO war_schedule(schedule_key,source_clan_tag,opponent_tag,prep_time,end_time,next_run_at,war_type)
	SELECT 'unrelated-'||lpad(i::text,4,'0'),'#PPPP','#YYYY',now(),now()+interval '2 days',now()+interval '1 day','random' FROM generate_series(1,501)i;
	INSERT INTO war_reminder_jobs(schedule_key,minutes_remaining,run_at) VALUES('unrelated-0501',60,now()+interval '1 day');`)
	must(e)
	var providerRequests atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerRequests.Add(1)
		http.Error(w, "unexpected provider request", 500)
	}))
	defer provider.Close()
	cfg := clashy.DefaultClientConfig()
	cfg.BaseURL = provider.URL
	cfg.LookupCache = false
	cfg.UpdateCache = false
	client, e := clashy.NewClient(cfg)
	must(e)
	defer client.Close()
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	// A retained cursor would monopolize this pool and deadlock the first write.
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn + separator + "pool_max_conns=1", ReminderRequestsPerSecond: 10, EventStreamName: "reminder-startup-fixture", EventStreamReclaimIdleSeconds: 1}, Clash: client, Valkey: cache, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker(), Availability: platform.NewAvailabilityGate(nil)}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- scripts.NewRemindersDomain().Run(runCtx, app) }()
	count, stale := 0, 1
	for (count != 501 || stale != 0) && ctx.Err() == nil {
		must(db.QueryRow(ctx, "SELECT count(*) FROM war_reminder_jobs WHERE schedule_key LIKE 'startup-%'").Scan(&count))
		must(db.QueryRow(ctx, "SELECT count(*) FROM war_reminder_jobs WHERE schedule_key LIKE 'unrelated-%'").Scan(&stale))
		time.Sleep(25 * time.Millisecond)
	}
	check(count == 501 && stale == 0, "startup must reconcile both pages and clean removed audiences with one connection")
	stop()
	select {
	case e := <-done:
		if e != nil && e != context.Canceled {
			panic(e)
		}
	case <-time.After(10 * time.Second):
		panic("reminder service did not stop")
	}
	check(providerRequests.Load() == 0, "startup must not call an external provider")
	fmt.Println("PASS actual reminder startup: 501 audience wars across pages, removed-audience job cleanup, one SQL connection; no provider requests")
}
