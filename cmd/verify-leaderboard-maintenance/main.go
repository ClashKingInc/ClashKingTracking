// Exercise the real maintenance service with canonical disposable PostgreSQL.
package main

import (
	"clashking_tracking/scripts"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"time"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func check(ok bool, msg string) {
	if !ok {
		panic(msg)
	}
}
func main() {
	check(os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1", "disposable fixture required")
	dsn := os.Getenv("TEST_DATABASE_URL")
	check(strings.Contains(dsn, "@127.0.0.1:"), "loopback database required")
	ctx := context.Background()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	_, e = db.Exec(ctx, `INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received,location_id)
 VALUES('#COUNT1','Count one',true,0,1,'',0,0,987601),('#COUNT2','Count fifty',true,0,50,'',0,0,NULL)`)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO basic_player(tag,name,townhall_level,league_id,trophies) VALUES('#MAINT','Before',16,105000036,5000)`)
	must(e)
	_, e = db.Exec(ctx, "INSERT INTO tracking_scheduled_jobs(job,period,completed_at) VALUES('ranked_closeout',(clock_timestamp() AT TIME ZONE 'UTC')::date,clock_timestamp())")
	must(e)
	lock, e := db.Acquire(ctx)
	must(e)
	defer lock.Release()
	_, e = lock.Exec(ctx, "SELECT pg_advisory_lock(73194201)")
	must(e)
	must(scripts.MaintainLeaderboardSnapshots(ctx, db))
	var n int
	must(db.QueryRow(ctx, "SELECT count(*) FROM tracking_scheduled_jobs WHERE job IN ('player_board_refresh','statistics_refresh')").Scan(&n))
	check(n == 0, "deferred work marked complete")
	_, e = lock.Exec(ctx, "SELECT pg_advisory_unlock(73194201)")
	must(e)
	must(scripts.MaintainLeaderboardSnapshots(ctx, db))
	must(db.QueryRow(ctx, "SELECT count(*) FROM tracking_scheduled_jobs WHERE job IN ('player_board_refresh','statistics_refresh') AND completed_at IS NOT NULL").Scan(&n))
	check(n == 2, "both snapshot families did not complete")
	var count int
	must(db.QueryRow(ctx, "SELECT clan_count FROM api_clan_distribution_counts WHERE dimension='location' AND bucket=987601").Scan(&count))
	check(count == 1, "clan distributions did not publish through maintenance")
	_, e = db.Exec(ctx, "UPDATE basic_clan SET location_id=987602,member_count=6 WHERE tag='#COUNT1'")
	must(e)
	must(scripts.MaintainLeaderboardSnapshots(ctx, db))
	must(db.QueryRow(ctx, "SELECT clan_count FROM api_clan_distribution_counts WHERE dimension='location' AND bucket=987601").Scan(&count))
	check(count == 1, "fresh clan distribution was unexpectedly refreshed")
	_, e = db.Exec(ctx, "UPDATE tracking_scheduled_jobs SET completed_at=clock_timestamp()-interval '31 minutes' WHERE job='statistics_refresh'")
	must(e)
	must(scripts.MaintainLeaderboardSnapshots(ctx, db))
	must(db.QueryRow(ctx, "SELECT clan_count FROM api_clan_distribution_counts WHERE dimension='location' AND bucket=987602").Scan(&count))
	check(count == 1, "due clan distribution did not refresh")
	var oldStats time.Time
	must(db.QueryRow(ctx, "SELECT completed_at FROM tracking_scheduled_jobs WHERE job='statistics_refresh'").Scan(&oldStats))
	_, e = db.Exec(ctx, "UPDATE basic_player SET name='After' WHERE tag='#MAINT'; UPDATE tracking_scheduled_jobs SET completed_at=clock_timestamp()-interval '31 minutes' WHERE job='player_board_refresh'")
	must(e)
	must(scripts.MaintainLeaderboardSnapshots(ctx, db))
	for _, view := range []string{"player_townhall_leaderboards", "player_league_leaderboards"} {
		var name string
		must(db.QueryRow(ctx, "SELECT name FROM "+view+" WHERE tag='#MAINT'").Scan(&name))
		check(name == "After", "player boards did not refresh after thirty minutes")
	}
	var newStats time.Time
	must(db.QueryRow(ctx, "SELECT completed_at FROM tracking_scheduled_jobs WHERE job='statistics_refresh'").Scan(&newStats))
	check(newStats.Equal(oldStats), "fresh statistics unnecessarily refreshed")
	_, e = db.Exec(ctx, "UPDATE tracking_scheduled_jobs SET completed_at=clock_timestamp()-interval '31 minutes' WHERE job='player_board_refresh'")
	must(e)
	var previous time.Time
	must(db.QueryRow(ctx, "SELECT completed_at FROM tracking_scheduled_jobs WHERE job='player_board_refresh'").Scan(&previous))
	tx, e := lock.Begin(ctx)
	must(e)
	_, e = tx.Exec(ctx, "REFRESH MATERIALIZED VIEW player_townhall_leaderboards")
	must(e)
	e = scripts.MaintainLeaderboardSnapshots(ctx, db)
	check(e != nil, "blocked refresh did not respect lock timeout")
	must(tx.Rollback(ctx))
	var afterFailure time.Time
	must(db.QueryRow(ctx, "SELECT completed_at FROM tracking_scheduled_jobs WHERE job='player_board_refresh'").Scan(&afterFailure))
	check(afterFailure.Equal(previous), "failed refresh marked complete")
	must(scripts.MaintainLeaderboardSnapshots(ctx, db))
	must(db.QueryRow(ctx, "SELECT completed_at FROM tracking_scheduled_jobs WHERE job='player_board_refresh'").Scan(&afterFailure))
	check(afterFailure.After(previous), "failed work did not retry")
	fmt.Println("PASS real leaderboard maintenance: shared lock, both snapshot families, 30-minute due time, independent schedule, bounded lock failure and retry")
}
