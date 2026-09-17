//go:build league_analytics_integration

package scripts

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
)

func TestPlayerBoardRefreshAndWeeklyReset(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" {
		t.Skip("schema-owned disposable fixture required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, e := pool.Exec(ctx, sql, args...); e != nil {
			t.Fatal(e)
		}
	}
	exec(`INSERT INTO basic_player(tag,name,townhall_level,league_id,trophies)
 SELECT '#RESET'||lpad(n::text,5,'0'),'Reset',16,105000033,3000 FROM generate_series(1,5001) n;
 INSERT INTO basic_player(tag,name,townhall_level,league_id,trophies) VALUES
 ('#RESETLEGEND','Legend',16,105000036,6000),('#RESETUNKNOWN','Unknown',16,NULL,3000),('#RESETUNRANKED','Unranked',16,105000000,99);`)
	now := time.Date(2026, 9, 21, 16, 35, 0, 0, time.UTC)
	period := dayStart(now)
	batch := func(at time.Time) (bool, error) {
		tx, e := pool.Begin(ctx)
		if e != nil {
			return false, e
		}
		defer tx.Rollback(ctx)
		return resetRankedTrophyBatchTx(ctx, tx, period, at)
	}
	if _, e := batch(now); e == nil {
		t.Fatal("reset without closeout")
	}
	exec(`INSERT INTO tracking_scheduled_jobs(job,period,completed_at) VALUES('ranked_closeout',$1,now())`, period)
	if _, e := batch(now.Add(time.Hour)); e == nil {
		t.Fatal("reset outside window")
	}
	done, e := batch(now)
	if e != nil || done {
		t.Fatalf("first batch: %v %v", done, e)
	}
	var remaining int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM basic_player WHERE tag LIKE '#RESET%' AND league_id BETWEEN 105000000 AND 105000035 AND trophies<>0`).Scan(&remaining); e != nil || remaining != 2 {
		t.Fatalf("batch remaining=%d: %v", remaining, e)
	}
	// Each call opens a fresh transaction, matching a restart resuming the stored cursor.
	for i := 0; i < 3 && !done; i++ {
		done, e = batch(now)
		if e != nil {
			t.Fatal(e)
		}
	}
	if !done {
		t.Fatal("reset did not finish")
	}
	var preserved int
	if e = pool.QueryRow(ctx, `SELECT sum(trophies) FROM basic_player WHERE tag IN ('#RESETLEGEND','#RESETUNKNOWN')`).Scan(&preserved); e != nil || preserved != 9000 {
		t.Fatalf("excluded players changed: %d %v", preserved, e)
	}
	exec(`UPDATE basic_player SET trophies=123 WHERE tag='#RESET00001'`)
	if done, e = batch(now); e != nil || !done {
		t.Fatalf("repeat: %v %v", done, e)
	}
	var trophies int
	if e = pool.QueryRow(ctx, `SELECT trophies FROM basic_player WHERE tag='#RESET00001'`).Scan(&trophies); e != nil || trophies != 123 {
		t.Fatalf("repeated reset: %d %v", trophies, e)
	}
	store := &timescaleLeaderboardStore{pool: pool}
	clans := &timescaleScheduledStore{pool: pool}
	group := currentClanRankingGroup{RankingType: "home", LocationID: "global", Rows: []currentClanRankingRow{{ClanTag: "#CLANRESET", Rank: 1, Points: 100}}}
	if _, e = clans.ReplaceCurrentClanRankingGroup(ctx, group); e != nil {
		t.Fatal(e)
	}
	group.Rows[0].Points = 200
	if _, e = clans.ReplaceCurrentClanRankingGroup(ctx, group); e != nil {
		t.Fatal(e)
	}
	var points int
	if e = pool.QueryRow(ctx, `SELECT points FROM clan_rankings_current WHERE clan_tag='#CLANRESET' AND ranking_type='home' AND location_id='global'`).Scan(&points); e != nil || points != 200 {
		t.Fatalf("clan ranking writer: %d %v", points, e)
	}
	if e = store.playerLeaderboardMaintenance(ctx); e != nil {
		t.Fatal(e)
	}
	exec(`UPDATE basic_player SET trophies=456 WHERE tag='#RESET00001'`)
	if e = store.playerLeaderboardMaintenance(ctx); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT trophies FROM player_league_leaderboards WHERE tag='#RESET00001'`).Scan(&trophies); e != nil || trophies != 123 {
		t.Fatalf("refreshed before six hours: %d %v", trophies, e)
	}
	exec(`UPDATE tracking_scheduled_jobs SET completed_at=now()-interval '7 hours' WHERE job='player_board_refresh'`)
	if e = store.playerLeaderboardMaintenance(ctx); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT trophies FROM player_league_leaderboards WHERE tag='#RESET00001'`).Scan(&trophies); e != nil || trophies != 456 {
		t.Fatalf("six-hour refresh: %d %v", trophies, e)
	}
}
