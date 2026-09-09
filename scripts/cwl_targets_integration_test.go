package scripts

import (
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// Exercise the production predicate against canonical Goose tables. All test
// records are transaction-local and rolled back, including on test failure.
func TestCWLTargetsMidSeasonDatabase(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" {
		t.Skip("requires disposable Goose fixture")
	}
	conn, err := pgx.Connect(t.Context(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(t.Context())
	tx, err := conn.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	_, err = tx.Exec(t.Context(), `INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received,cwl_league_id) VALUES ('#LOCAL_A','local',true,0,15,'',0,0,48000012),('#LOCAL_B','local',true,0,15,'',0,0,48000012),('#LOCAL_PRIVATE','local',false,0,0,'',0,0,NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	count := func(day string, want int) {
		t.Helper()
		q := strings.ReplaceAll(cwlTargetPredicateSQL, "now()", "TIMESTAMPTZ '2026-09-"+day+" 12:00:00+00'")
		var got int
		if err := tx.QueryRow(t.Context(), "SELECT count(*) FROM basic_clan WHERE tag IN ('#LOCAL_A','#LOCAL_B','#LOCAL_PRIVATE') AND "+q).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("day %s: targets %d, want %d", day, got, want)
		}
	}
	for _, day := range []string{"01", "03", "08", "15"} {
		count(day, 3)
	}
	count("16", 0)
	_, err = tx.Exec(t.Context(), `INSERT INTO cwl_groups(cwl_id,season,rounds,state) VALUES ('localgroup12','2026-09-01','[]','inWar'); INSERT INTO cwl_group_clans(cwl_id,clan_tag) VALUES ('localgroup12','#LOCAL_A'),('localgroup12','#LOCAL_B')`)
	if err != nil {
		t.Fatal(err)
	}
	count("08", 2)
	_, err = tx.Exec(t.Context(), `INSERT INTO war_schedule(schedule_key,source_clan_tag,opponent_tag,prep_time,end_time,next_run_at,war_type,war_tag) VALUES ('local-cwl-war','#LOCAL_A','#LOCAL_B','2026-09-07 12:00:00+00','2026-09-09 12:00:00+00','2026-09-09 12:00:00+00','cwl','#LOCAL_WAR'),('local-cwl-next-war','#LOCAL_A','#LOCAL_B','2026-09-08 12:00:00+00','2026-09-10 12:00:00+00','2026-09-10 12:00:00+00','cwl','#LOCAL_NEXT_WAR')`)
	if err != nil {
		t.Fatal(err)
	}
	count("08", 1)
	_, err = tx.Exec(t.Context(), `UPDATE war_schedule SET end_time='2026-09-08 11:59:59+00' WHERE schedule_key='local-cwl-war'`)
	if err != nil {
		t.Fatal(err)
	}
	count("08", 2)
	_, err = tx.Exec(t.Context(), `UPDATE cwl_groups SET state='ended' WHERE cwl_id='localgroup12'`)
	if err != nil {
		t.Fatal(err)
	}
	count("08", 1)
}
