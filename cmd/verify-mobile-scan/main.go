// Run the real notification scan against the canonical disposable database.
// No devices are seeded, so post/campaign completion cannot send a push.
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
	for _, slug := range []string{"queued", "queued-after-success", "failed", "partial", "processing", "scheduled-post", "expired-post"} {
		status := "live"
		if slug == "scheduled-post" {
			status = "scheduled"
		}
		var id string
		must(db.QueryRow(ctx, `INSERT INTO admin_posts(slug,title,summary,status,starts_at,ends_at) VALUES($1,'Local scan fixture','Local only',$2,now()-interval '1 hour',CASE WHEN $1='expired-post' THEN now()-interval '1 minute' END) RETURNING id::text`, slug, status).Scan(&id))
		ids[slug] = id
	}
	for _, status := range []string{"failed", "partial", "processing"} {
		_, err = db.Exec(ctx, `INSERT INTO admin_post_delivery_attempts(post_id,attempt_number,trigger,status,attempted_at) VALUES($1,1,'publish',$2,now()-interval '1 hour')`, ids[status], status)
		must(err)
	}
	for _, slug := range []string{"queued", "queued-after-success"} {
		// Historical attempt counts and a previous successful send must not block
		// a new explicit request. No replay should occur on the next scan.
		_, err = db.Exec(ctx, `INSERT INTO admin_post_delivery_attempts(post_id,attempt_number,trigger,status) VALUES($1,5,'manual','queued')`, ids[slug])
		must(err)
	}
	_, err = db.Exec(ctx, `UPDATE admin_posts SET push_sent_at=now()-interval '1 hour' WHERE id=$1`, ids["queued-after-success"])
	must(err)
	var campaign string
	must(db.QueryRow(ctx, `INSERT INTO admin_notification_campaigns(campaign_key,title,body,status,trigger_type,send_at) VALUES('local-scan','Fixture','Local only','scheduled','manual',now()-interval '1 hour') RETURNING id::text`).Scan(&campaign))
	_, err = db.Exec(ctx, `INSERT INTO admin_campaign_delivery_attempts(campaign_id,scheduled_for,status,attempted_at) VALUES($1,(now()-interval '1 hour')::date,'processing',now()-interval '10 minutes')`, campaign)
	must(err)
	app := &platform.App{Config: platform.Config{TimescaleURL: dsn, MobilePushScanSeconds: 1, RunOnce: true}, Logger: slog.New(slog.NewTextHandler(os.Stderr, nil)), Stats: platform.NewTracker()}
	must(scripts.NewMobilePushDomain().Run(ctx, app))
	// Run a second real service cycle to detect unintended automatic replays.
	must(scripts.NewMobilePushDomain().Run(ctx, app))
	for _, slug := range []string{"queued", "queued-after-success", "failed", "partial", "processing"} {
		var count int
		var status string
		must(db.QueryRow(ctx, "SELECT count(*) FROM admin_post_delivery_attempts WHERE post_id=$1", ids[slug]).Scan(&count))
		must(db.QueryRow(ctx, "SELECT status FROM admin_post_delivery_attempts WHERE post_id=$1", ids[slug]).Scan(&status))
		check(count == 1, "scan created an automatic retry for "+slug)
		expected := slug
		if strings.HasPrefix(slug, "queued") {
			expected = "no_audience"
		}
		check(status == expected, "unexpected attempt status for "+slug+": "+status)
	}
	var status string
	must(db.QueryRow(ctx, "SELECT status FROM admin_posts WHERE id=$1", ids["scheduled-post"]).Scan(&status))
	check(status == "live", "scheduled post not published")
	must(db.QueryRow(ctx, "SELECT status FROM admin_posts WHERE id=$1", ids["expired-post"]).Scan(&status))
	check(status == "expired", "post not expired")
	must(db.QueryRow(ctx, "SELECT status FROM admin_campaign_delivery_attempts WHERE campaign_id=$1", campaign).Scan(&status))
	check(status == "no_audience", "stale campaign claim not completed")
	fmt.Println("PASS actual mobile scan: explicit queued attempt 5, resend after success, no failed/partial/interrupted replay, stale campaign recovery, post publish/expiry; no devices or push requests")
}
