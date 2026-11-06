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

	// A blocked later output must roll back both shared source counts and every
	// earlier public output. Then the same real service retries the whole set.
	_, e = db.Exec(ctx, `INSERT INTO basic_player(tag,name,townhall_level,league_id,trophies) VALUES('#SECOND','Second',17,NULL,0);
 INSERT INTO join_leave_history(time,type,clan_tag,player_tag,townhall_level) VALUES('2020-01-01 12:00Z','join','#CLAN','#SECOND',17);
 INSERT INTO wars(war_id,clan_tag,opponent_tag,prep_time,start_time,end_time,size,war_type,state) VALUES(1,'#CLAN','#OTHER','2020-01-01 01:00Z','2020-01-02 01:00Z','2020-01-03 01:00Z',15,'random','warEnded');
 UPDATE tracking_scheduled_jobs SET completed_at=clock_timestamp()-interval '31 minutes' WHERE job='statistics_refresh'`)
	must(e)
	tx, e = lock.Begin(ctx)
	must(e)
	_, e = tx.Exec(ctx, "REFRESH MATERIALIZED VIEW api_global_counts")
	must(e)
	e = scripts.MaintainLeaderboardSnapshots(ctx, db)
	check(e != nil, "blocked count publication unexpectedly succeeded")
	must(tx.Rollback(ctx))
	var shared, publicCount int64
	must(db.QueryRow(ctx, "SELECT row_count FROM stats_player_count_snapshot WHERE scope='total'").Scan(&shared))
	must(db.QueryRow(ctx, "SELECT sum(total_count) FROM townhall_counts").Scan(&publicCount))
	check(shared == 1 && publicCount == 1, "failed publication leaked part of the new count generation")
	must(scripts.MaintainLeaderboardSnapshots(ctx, db))
	var players, joins, wars int64
	must(db.QueryRow(ctx, "SELECT player_count,total_join_leaves,wars_stored FROM api_global_counts").Scan(&players, &joins, &wars))
	check(players == 2 && joins == 1 && wars == 1, "late history or shared population counts differ")
	_, e = db.Exec(ctx, `DELETE FROM join_leave_history WHERE player_tag='#SECOND'; UPDATE wars SET end_time='2020-01-04 01:00Z' WHERE war_id=1; UPDATE tracking_scheduled_jobs SET completed_at=clock_timestamp()-interval '31 minutes' WHERE job='statistics_refresh'`)
	must(e)
	must(scripts.MaintainLeaderboardSnapshots(ctx, db))
	must(db.QueryRow(ctx, "SELECT player_count,total_join_leaves,wars_stored FROM api_global_counts").Scan(&players, &joins, &wars))
	check(players == 2 && joins == 0 && wars == 1, "historical deletion or date correction was missed")
	fmt.Println("PASS shared count publication: atomic rollback/retry, late historical insertion, deletion and moved date")
	fmt.Println("PASS real leaderboard maintenance: shared lock, both snapshot families, 30-minute due time, independent schedule, bounded lock failure and retry")
}
