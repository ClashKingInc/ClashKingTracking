package scripts

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// This fixture is intentionally large enough to catch a return to correlated
// whole-history scans while remaining cheap on a disposable local database.
func TestCWLQueriesStaySeasonScopedAndArchiveIgnoresCWLHistoryAtHistoricVolume(t *testing.T) {
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

	if _, err := tx.Exec(t.Context(), `
		INSERT INTO basic_clan(tag,name,public_war_log,war_wins,member_count,badge_token,troops_donated,troops_received,cwl_league_id)
		SELECT '#PERF_' || lpad(i::text, 8, '0'), 'performance', false, 0, 0, '', 0, 0, 48000000
		FROM generate_series(1, 25000) AS i;
		INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,rounds,state)
		SELECT 'hist' || lpad(i::text, 8, '0'),
		       to_char(DATE '2018-01-01' + ((i % 60) || ' months')::interval, 'YYYY-MM'),
		       48000010 + (i % 10),
		       jsonb_build_array(jsonb_build_object('warTags', jsonb_build_array('#HIST_' || i))),
		       'ended'
		FROM generate_series(1, 20000) AS i;
	`); err != nil {
		t.Fatal(err)
	}

	assertTargetQueryFast := func(name, query string) {
		t.Helper()
		query = replaceSQLNow(query, "2026-09-08 12:00:00+00")
		started := time.Now()
		rows, queryErr := tx.Query(t.Context(), query, "", 501)
		if queryErr != nil {
			t.Fatal(queryErr)
		}
		count := 0
		for rows.Next() {
			var tag, clanName string
			var leagueID int
			if err := rows.Scan(&tag, &clanName, &leagueID); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			count++
		}
		rows.Close()
		if count == 0 {
			t.Fatalf("%s returned no targets", name)
		}
		if elapsed := time.Since(started); elapsed > 3*time.Second {
			t.Fatalf("%s took %s at local historic volume", name, elapsed)
		}
	}
	assertTargetQueryFast("CWL discovery", cwlDiscoveryTargetsSQL)

	if _, err := tx.Exec(t.Context(), `
		INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,rounds,state)
		SELECT 'curg' || lpad(i::text, 8, '0'), '2026-09-01', 48000012, '[]'::jsonb, 'inWar'
		FROM generate_series(1, 1000) AS i;
		INSERT INTO cwl_group_clans(cwl_id,clan_tag)
		SELECT 'curg' || lpad(i::text, 8, '0'), '#PERF_' || lpad(i::text, 8, '0')
		FROM generate_series(1, 1000) AS i;
	`); err != nil {
		t.Fatal(err)
	}
	assertTargetQueryFast("CWL known-group refresh", cwlRefreshTargetsSQL)

	var packID int64
	if err := tx.QueryRow(t.Context(), `INSERT INTO war_archive_packs(source,status) VALUES('live','building') RETURNING pack_id`).Scan(&packID); err != nil {
		t.Fatal(err)
	}
	insertArchiveQueryWar(t, tx, packID, 900000001, "random", "", "2026-09-08 12:00:00+00")
	started := time.Now()
	rows, err := tx.Query(t.Context(), claimArchivePackWarsSQL, packID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		var warID int32
		var end time.Time
		var warType string
		var payload []byte
		if err := rows.Scan(&warID, &end, &warType, &payload); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		count++
	}
	rows.Close()
	if count != 1 {
		t.Fatalf("ordinary pack rows = %d, want 1", count)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("ordinary-only pack consulted unrelated CWL history for %s", elapsed)
	}

	insertArchiveQueryWar(t, tx, packID, 900000002, "cwl", "#EDGE", "2026-09-01 00:30:00+00")
	rows, err = tx.Query(t.Context(), claimArchivePackWarsSQL, packID)
	if err != nil {
		t.Fatal(err)
	}
	warTypes := map[int32]string{}
	for rows.Next() {
		var warID int32
		var end time.Time
		var warType string
		var payload []byte
		if err := rows.Scan(&warID, &end, &warType, &payload); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		warTypes[warID] = warType
	}
	rows.Close()
	if warTypes[900000001] != "random" || warTypes[900000002] != "cwl" || len(warTypes) != 2 {
		t.Fatalf("pack claim did not preserve ordinary and CWL wars: %#v", warTypes)
	}
}

func replaceSQLNow(query, timestamp string) string {
	return strings.ReplaceAll(query, "now()", "TIMESTAMPTZ '"+timestamp+"'")
}

func insertArchiveQueryWar(t *testing.T, tx pgx.Tx, packID int64, warID int32, warType, warTag, end string) {
	t.Helper()
	var nullableWarTag any
	if warTag != "" {
		nullableWarTag = warTag
	}
	_, err := tx.Exec(t.Context(), `
		INSERT INTO wars(war_id,clan_tag,opponent_tag,prep_time,start_time,end_time,size,war_type,state,war_tag)
		VALUES($1,'#A','#B',$4::timestamptz - interval '2 days',$4::timestamptz - interval '1 day',$4,15,$2,'warEnded',$3)
	`, warID, warType, nullableWarTag, end)
	if err == nil {
		_, err = tx.Exec(t.Context(), `INSERT INTO war_archive_pending(war_id,end_time,payload,pack_id) VALUES($1,$2,'{}'::jsonb,$3)`, warID, end, packID)
	}
	if err != nil {
		t.Fatal(err)
	}
}
