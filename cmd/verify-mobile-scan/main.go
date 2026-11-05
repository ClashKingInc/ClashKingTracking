// Run the real notification scan against the canonical disposable database.
// No devices are seeded, so retry/campaign completion cannot send a push.
package main

import (
	"clashking_tracking/internal/platform"
	"clashking_tracking/scripts"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"os"
	"strings"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func check(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func main() {
	dsn := os.Getenv("TEST_DATABASE_URL")
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && strings.Contains(dsn, "@127.0.0.1:"), "disposable loopback database required")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	var devices int
	must(db.QueryRow(ctx, "SELECT count(*) FROM mobile_push_devices").Scan(&devices))
	check(devices == 0, "device-free database required")
	ids := map[string]string{}
	for _, slug := range []string{"retry-due", "retry-fresh", "scheduled-post", "expired-post"} {
		status := "live"
		if slug == "scheduled-post" {
			status = "scheduled"
		}
		var id string
		must(db.QueryRow(ctx, `INSERT INTO admin_posts(slug,title,summary,status,starts_at,ends_at) VALUES($1,'Local scan fixture','Local only',$2,now()-interval '1 hour',CASE WHEN $1='expired-post' THEN now()-interval '1 minute' END) RETURNING id::text`, slug, status).Scan(&id))
		ids[slug] = id
	}
	for _, slug := range []string{"retry-due", "retry-fresh"} {
		age := time.Hour
		if slug == "retry-fresh" {
			age = 0
		}
		_, err = db.Exec(ctx, `INSERT INTO admin_post_delivery_attempts(post_id,attempt_number,trigger,status,attempted_at) VALUES($1,1,'publish','failed',$2)`, ids[slug], time.Now().UTC().Add(-age))
		must(err)
	}
	var campaign string
	must(db.QueryRow(ctx, `INSERT INTO admin_notification_campaigns(campaign_key,title,body,status,trigger_type,send_at) VALUES('local-scan','Fixture','Local only','scheduled','manual',now()-interval '1 hour') RETURNING id::text`).Scan(&campaign))
	_, err = db.Exec(ctx, `INSERT INTO admin_campaign_delivery_attempts(campaign_id,scheduled_for,status,attempted_at) VALUES($1,(now()-interval '1 hour')::date,'processing',now()-interval '10 minutes')`, campaign)
	must(err)
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, MobilePushScanSeconds: 1, RunOnce: true}, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker()}
	must(scripts.NewMobilePushDomain().Run(ctx, app))
	var sent bool
	must(db.QueryRow(ctx, "SELECT push_sent_at IS NOT NULL FROM admin_posts WHERE id=$1", ids["retry-due"]).Scan(&sent))
	check(sent, "overdue retry was not completed")
	var count int
	must(db.QueryRow(ctx, "SELECT count(*) FROM admin_post_delivery_attempts WHERE post_id=$1", ids["retry-fresh"]).Scan(&count))
	check(count == 1, "fresh retry must remain deferred")
	var status string
	must(db.QueryRow(ctx, "SELECT status FROM admin_posts WHERE id=$1", ids["scheduled-post"]).Scan(&status))
	check(status == "live", "scheduled post not published")
	must(db.QueryRow(ctx, "SELECT status FROM admin_posts WHERE id=$1", ids["expired-post"]).Scan(&status))
	check(status == "expired", "post not expired")
	must(db.QueryRow(ctx, "SELECT status FROM admin_campaign_delivery_attempts WHERE campaign_id=$1", campaign).Scan(&status))
	check(status == "no_audience", "stale campaign claim not completed")
	fmt.Println("PASS actual mobile scan: delayed retry, fresh retry guard, stale campaign recovery, post publish/expiry; no devices or push requests")
}
