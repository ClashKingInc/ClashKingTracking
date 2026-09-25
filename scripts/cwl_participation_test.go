package scripts

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"clashking_tracking/internal/wararchive"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCWLSeasonLabel(t *testing.T) {
	for _, input := range []string{"2026-09", "2026-09-01", "2026-09-03"} {
		got, err := CWLSeasonLabel(input)
		if err != nil || got != "2026-09" {
			t.Fatalf("label(%q) = %q, %v", input, got, err)
		}
	}
	for _, input := range []string{"2026-00", "2026-13", "202x-09", "2026-9"} {
		if _, err := CWLSeasonLabel(input); err == nil {
			t.Errorf("accepted %q", input)
		}
	}
	if nextCWLSeason("2026-12") != "2027-01" {
		t.Fatal("December did not roll to January")
	}
}

func TestCWLParticipationDoesNotReadLowerLeagueArchives(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" {
		t.Skip("requires disposable Goose fixture")
	}
	pool, err := pgxpool.New(t.Context(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = pool.Exec(t.Context(), `
		INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,war_size,rounds,state)
		VALUES ('cwlread00001','2026-09-01',48000018,15,'[["#LOWER"]]'::jsonb,'ended');
		INSERT INTO cwl_group_clans(cwl_id,clan_tag) VALUES ('cwlread00001','#TESTA');
		INSERT INTO cwl_group_members(cwl_id,clan_tag,tag,town_hall)
		VALUES ('cwlread00001','#TESTA','#P0Y',18);`)
	if err != nil {
		t.Fatal(err)
	}
	var packID int64
	if err := pool.QueryRow(t.Context(), `INSERT INTO war_archive_packs(source,status,war_count) VALUES('live','uploaded',1) RETURNING pack_id`).Scan(&packID); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(t.Context(), `
		INSERT INTO wars(clan_tag,opponent_tag,prep_time,start_time,end_time,size,war_type,state,war_tag,archive_pack_id,archive_offset,archive_compressed_bytes)
		VALUES('#TESTA','#TESTB','2026-09-04T00:00:00Z','2026-09-05T00:00:00Z','2026-09-06T00:00:00Z',15,'cwl','warEnded','#LOWER',$1,0,1)`, packID)
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	report, err := RebuildCWLParticipation(t.Context(), pool, "2026-09", func(context.Context, CWLArchiveLocator) (wararchive.War, error) {
		reads++
		return wararchive.War{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if reads != 0 || len(report.Buckets) != 1 || report.Buckets[0].FinalizedWars != 1 || report.Buckets[0].ArchivedWars != 0 || report.Buckets[0].SameTHHitRates != nil {
		t.Fatalf("lower league archive reads=%d report=%+v", reads, report)
	}
}

func TestCWLFinalizedWarQueryUsesStoredNestedRounds(t *testing.T) {
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
	_, err = tx.Exec(t.Context(), `INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,war_size,rounds,state) VALUES ('cwltst000001','2026-09-01',48000022,15,'[["#CWLFIXA","#CWLFIXB"]]'::jsonb,'ended')`)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []struct{ tag, state string }{{"#CWLFIXA", "warEnded"}, {"#CWLFIXB", "warended"}} {
		_, err = tx.Exec(t.Context(), `INSERT INTO wars(clan_tag,opponent_tag,prep_time,start_time,end_time,size,war_type,state,war_tag) VALUES('#TESTA','#TESTB','2026-09-04T00:00:00Z','2026-09-05T00:00:00Z','2026-09-06T00:00:00Z',15,'cwl',$1,$2)`, value.state, value.tag)
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := tx.Query(t.Context(), cwlFinalizedWarsSQL, "2026-09", "2026-10")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var league, size int
		var id int64
		var pack, offset *int64
		var bytes *int
		if err := rows.Scan(&league, &size, &id, &pack, &offset, &bytes); err != nil {
			t.Fatal(err)
		}
		if league == 48000022 && size == 15 {
			count++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("finalized fixture wars = %d, want 2", count)
	}
	rows.Close()
	var duplicateClans, duplicateMembers int64
	if err := tx.QueryRow(t.Context(), cwlDuplicateCountsSQL, "2026-09", "2026-10").Scan(&duplicateClans, &duplicateMembers); err != nil {
		t.Fatal(err)
	}
	if duplicateClans != 0 || duplicateMembers != 0 {
		t.Fatalf("unexpected duplicate source: clans=%d members=%d", duplicateClans, duplicateMembers)
	}
	_, err = tx.Exec(t.Context(), `
		INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,war_size,rounds,state)
		VALUES ('cwltst000002','2026-09-02',48000021,15,'[]'::jsonb,'ended');
		INSERT INTO cwl_group_clans(cwl_id,clan_tag)
		VALUES ('cwltst000001','#TESTA'),('cwltst000002','#TESTA');
		INSERT INTO cwl_group_members(cwl_id,clan_tag,tag,town_hall)
		VALUES ('cwltst000001','#TESTA','#PLAYER',18),('cwltst000002','#TESTA','#PLAYER',18);`)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow(t.Context(), cwlDuplicateCountsSQL, "2026-09", "2026-10").Scan(&duplicateClans, &duplicateMembers); err != nil {
		t.Fatal(err)
	}
	if duplicateClans != 1 || duplicateMembers != 1 {
		t.Fatalf("duplicate source: clans=%d members=%d, want 1 each", duplicateClans, duplicateMembers)
	}
}

func TestCWLParticipationCountsOnlyEqualTHFromBothSides(t *testing.T) {
	bucket := CWLParticipationBucket{CWLLeagueID: 48000022}
	war := wararchive.War{
		Clan: wararchive.Clan{Members: []wararchive.Member{
			{Tag: "#A", TownhallLevel: 17, Attacks: []wararchive.Attack{{DefenderTag: "#B", Stars: 3}, {DefenderTag: "#C", Stars: 2}}},
		}},
		Opponent: wararchive.Clan{Members: []wararchive.Member{
			{Tag: "#B", TownhallLevel: 17, Attacks: []wararchive.Attack{{DefenderTag: "#A", Stars: 1}}},
			{Tag: "#C", TownhallLevel: 18},
		}},
	}
	addCWLSameTHHits(&bucket, war)
	if len(bucket.SameTHHitRates) != 1 || bucket.SameTHHitRates[0] != (CWLHitRate{Level: 17, Attacks: 2, ThreeStars: 1}) {
		t.Fatalf("equal TH hits = %+v", bucket.SameTHHitRates)
	}
	b, err := json.Marshal(CWLParticipationBucket{SameTHHitRates: nil})
	if err != nil || string(b) == "" {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(b, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["same_th_hitrates"] != nil {
		t.Fatalf("unavailable hit rates must be null: %s", b)
	}
}
