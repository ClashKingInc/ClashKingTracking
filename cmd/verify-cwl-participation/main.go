// Exercise the actual manual publication pipeline against disposable SQL.
package main

import (
	"clashking_tracking/internal/wararchive"
	"clashking_tracking/scripts"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func main() {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" || !strings.Contains(dsn, "@127.0.0.1:") {
		panic("disposable SQL required")
	}
	ctx := context.Background()
	db, e := pgxpool.New(ctx, dsn)
	must(e)
	defer db.Close()
	_, e = db.Exec(ctx, `INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,war_size,rounds,state) VALUES('testcwl00001','2026-09',48000022,15,'[]','ended')`)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO cwl_group_clans(cwl_id,clan_tag) VALUES('testcwl00001','#PYL'),('testcwl00001','#PYQ')`)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO cwl_group_members(cwl_id,clan_tag,tag,town_hall) VALUES('testcwl00001','#PYL','#PYY',18),('testcwl00001','#PYQ','#PYP',17)`)
	must(e)
	var pack int64
	must(db.QueryRow(ctx, `INSERT INTO war_archive_packs(status,source) VALUES('uploaded','live') RETURNING pack_id`).Scan(&pack))
	_, e = db.Exec(ctx, `INSERT INTO wars(clan_tag,opponent_tag,prep_time,start_time,end_time,size,war_type,state,war_tag,archive_pack_id,archive_offset,archive_compressed_bytes) VALUES('#PYL','#PYQ','2026-09-01','2026-09-02','2026-09-03',15,'cwl','warEnded','#PYR',$1,0,1)`, pack)
	must(e)
	_, e = db.Exec(ctx, `UPDATE cwl_groups SET rounds='[{"warTags":["#PYR"]}]' WHERE cwl_id='testcwl00001'`)
	must(e)
	reader := func(context.Context, scripts.CWLArchiveLocator) (wararchive.War, error) {
		return wararchive.War{Clan: wararchive.Clan{Members: []wararchive.Member{{Tag: "#PYY", TownhallLevel: 18, Attacks: []wararchive.Attack{{DefenderTag: "#PYP", Stars: 3}}}}}, Opponent: wararchive.Clan{Members: []wararchive.Member{{Tag: "#PYP", TownhallLevel: 18, Attacks: []wararchive.Attack{{DefenderTag: "#PYY", Stars: 2}}}}}}, nil
	}
	for i := 0; i < 2; i++ {
		r, e := scripts.RebuildCWLParticipation(ctx, db, "2026-09", reader)
		must(e)
		if len(r.Buckets) != 1 || r.Buckets[0].GroupCount != 1 || r.Buckets[0].ClanCount != 2 || r.Buckets[0].RegisteredPlayerCount != 2 || len(r.Buckets[0].TownhallCounts) != 2 || r.Buckets[0].FinalizedWars != 1 || r.Buckets[0].ArchivedWars != 1 || r.Buckets[0].SameTHHitRates[0].Attacks != 2 || r.Buckets[0].SameTHHitRates[0].ThreeStars != 1 {
			panic("incorrect participation")
		}
	}
	var n int
	must(db.QueryRow(ctx, "SELECT count(*) FROM cwl_participation WHERE season='2026-09'").Scan(&n))
	if n != 1 {
		panic("not idempotent")
	}
	_, e = db.Exec(ctx, `INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,war_size,rounds,state) VALUES('testcwl00002','2026-09',48000022,15,'[]','ended');INSERT INTO cwl_group_clans(cwl_id,clan_tag) VALUES('testcwl00002','#PYL')`)
	must(e)
	_, e = scripts.RebuildCWLParticipation(ctx, db, "2026-09", nil)
	if e == nil {
		panic("duplicate clan accepted")
	}
	must(db.QueryRow(ctx, "SELECT clan_count FROM cwl_participation WHERE season='2026-09'").Scan(&n))
	if n != 2 {
		panic("failed rebuild replaced valid summary")
	}
	fmt.Println("PASS CWL publication: source counts, TH distribution, idempotent rebuild, duplicate rejection preserves published data")
}
