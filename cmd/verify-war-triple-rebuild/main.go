// Actual CLI/service replay verification against disposable Timescale and an
// HTTP archive origin. It never calls a production service or scores table.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"clashking_tracking/internal/wararchive"
	"github.com/jackc/pgx/v5"
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

type input struct {
	id  int32
	war wararchive.War
	typ string
}

func main() {
	ctx := context.Background()
	dsn := os.Getenv("TEST_DATABASE_URL")
	check(dsn != "", "requires disposable integration TEST_DATABASE_URL")
	db, err := pgx.Connect(ctx, dsn)
	must(err)
	defer db.Close(ctx)
	var before int64
	must(db.QueryRow(ctx, `SELECT(SELECT count(*) FROM war_triple_streak_results)+(SELECT count(*) FROM war_triple_streaks)`).Scan(&before))
	dir, err := os.MkdirTemp("", "ck-war-replay-e2e-")
	must(err)
	defer os.RemoveAll(dir)
	executable := filepath.Join(dir, "rebuild")
	build := exec.Command("go", "build", "-o", executable, "./cmd/rebuild-war-triple-packs")
	build.Stdout = os.Stdout
	build.Stderr = os.Stderr
	must(build.Run())
	asOf := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	cutoff := asOf.AddDate(0, -1, 0)
	fixture := func(end time.Time, stars int) wararchive.War {
		war := wararchive.War{State: "warEnded", TeamSize: 2, AttacksPerMember: 2, PreparationStartTime: end.Add(-47 * time.Hour), StartTime: end.Add(-24 * time.Hour), EndTime: end, Clan: wararchive.Clan{Tag: "#AA"}, Opponent: wararchive.Clan{Tag: "#BB"}}
		for _, side := range []struct {
			clan          *wararchive.Clan
			prefix, other string
		}{{&war.Clan, "A", "B"}, {&war.Opponent, "B", "A"}} {
			for i := 1; i <= 2; i++ {
				side.clan.Members = append(side.clan.Members, wararchive.Member{Tag: fmt.Sprintf("#%s%d", side.prefix, i), Name: side.prefix + strconv.Itoa(i), TownhallLevel: 16, Attacks: []wararchive.Attack{{DefenderTag: "#" + side.other + "1", Stars: stars, Order: i*2 - 1}, {DefenderTag: "#" + side.other + "2", Stars: stars, Order: i * 2}}})
			}
		}
		return war
	}
	run := func(name string, inputs []input) (map[string]any, [][]string, error) {
		builder, e := wararchive.NewPackBuilder(1)
		if e != nil {
			return nil, nil, e
		}
		defer builder.Close()
		manifest := filepath.Join(dir, name+".csv")
		f, e := os.Create(manifest)
		if e != nil {
			return nil, nil, e
		}
		writer := csv.NewWriter(f)
		for _, row := range inputs {
			loc, e := builder.Add(row.id, row.war)
			if e != nil {
				return nil, nil, e
			}
			e = writer.Write([]string{"1", strconv.Itoa(int(row.id)), strconv.FormatInt(loc.Offset, 10), strconv.Itoa(loc.CompressedBytes), strconv.FormatInt(row.war.EndTime.Unix(), 10), "", row.typ})
			if e != nil {
				return nil, nil, e
			}
		}
		writer.Flush()
		if e = writer.Error(); e != nil {
			return nil, nil, e
		}
		f.Close()
		requests := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			check(r.URL.Path == "/packs/000001.pack", "incorrect archive path")
			requests++
			w.Write(builder.Bytes())
		}))
		defer server.Close()
		out := filepath.Join(dir, name)
		command := exec.Command(executable, "--index-file", manifest, "--output-dir", out, "--as-of", asOf.Format(time.RFC3339), "--archive-base", server.URL, "--staging-dsn", dsn)
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
		e = command.Run()
		if e != nil {
			return nil, nil, e
		}
		check(requests == 1, "downloaded same archive pack repeatedly")
		raw, e := os.ReadFile(filepath.Join(out, "coverage.json"))
		if e != nil {
			return nil, nil, e
		}
		var coverage map[string]any
		e = json.Unmarshal(raw, &coverage)
		if e != nil {
			return nil, nil, e
		}
		preview, e := os.Open(filepath.Join(out, "preview.csv"))
		if e != nil {
			return nil, nil, e
		}
		defer preview.Close()
		rows, e := csv.NewReader(preview).ReadAll()
		return coverage, rows, e
	}
	// Deliberately nonchronological: the staging suffix must not depend on pack order.
	old := fixture(cutoff.Add(-time.Second), 3)
	boundary := fixture(cutoff, 3)
	reset := fixture(asOf.Add(-48*time.Hour), 2)
	win := fixture(asOf.Add(-24*time.Hour), 3)
	cwl := fixture(asOf.Add(-time.Hour), 3)
	cwl.WarTag = "#CWL"
	cwl.AttacksPerMember = 1
	friendly := fixture(asOf.Add(-2*time.Hour), 3)
	friendly.StartTime = friendly.EndTime.Add(-time.Hour)
	coverage, rows, err := run("complete", []input{{1, win, "random"}, {2, old, "random"}, {3, reset, "random"}, {4, boundary, "random"}, {5, win, "random"}, {6, cwl, "cwl"}, {7, friendly, "random"}})
	must(err)
	check(coverage["manifest_scorable"] == true, "complete source unexpectedly unavailable")
	check(coverage["duplicate_rows"] == float64(1), "canonical alias counted twice")
	check(coverage["canonical_wars"] == float64(3), "calendar boundary or exclusion incorrect")
	for _, row := range rows[1:] {
		check(row[2] == "2" && row[3] == "t", "latest reset did not bound qualified suffix")
		check(row[5] != "", "source name lost during fingerprint normalization")
	}
	unknown := fixture(asOf.Add(-36*time.Hour), 3)
	unknown.AttacksPerMember = 0
	coverage, rows, err = run("unknown", []input{{10, reset, "random"}, {11, unknown, "random"}, {12, win, "random"}})
	must(err)
	check(coverage["manifest_scorable"] == false && coverage["unavailable_keys"] == float64(4), "unknown allowance was bridged")
	for _, row := range rows[1:] {
		check(row[2] == "" && row[3] == "f", "unknown allowance fabricated a score/reset")
	}
	newerReset := fixture(asOf.Add(-12*time.Hour), 2)
	newerWin := fixture(asOf.Add(-6*time.Hour), 3)
	coverage, rows, err = run("recovered", []input{{20, unknown, "random"}, {21, newerWin, "random"}, {22, newerReset, "random"}})
	must(err)
	check(coverage["manifest_scorable"] == true, "actual later reset failed to restore exactness")
	for _, row := range rows[1:] {
		check(row[2] == "2" && row[3] == "t", "recovery did not retain exact new suffix")
	}
	conflicted := win
	conflicted.Clan.Members = append([]wararchive.Member(nil), win.Clan.Members...)
	conflicted.Clan.Members[0].Attacks = append([]wararchive.Attack(nil), win.Clan.Members[0].Attacks...)
	conflicted.Clan.Members[0].Attacks[0].Stars = 1
	_, _, err = run("conflict", []input{{30, win, "random"}, {31, conflicted, "random"}})
	check(err != nil, "conflicting game-war sources accepted")
	mixed := fixture(asOf.Add(-3*time.Hour), 3)
	mixed.Opponent.Members[1].TownhallLevel = 15
	mixed.Clan.Members[0].Attacks[1].Stars = 0
	_, mixedRows, e := run("partial-credits", []input{{50, reset, "random"}, {51, mixed, "random"}})
	must(e)
	partial, neutral := false, false
	for _, row := range mixedRows[1:] {
		if row[0] == "#A1" && row[1] == "16" {
			partial = row[2] == "1" && row[3] == "t"
		}
		if row[0] == "#B2" && row[1] == "15" {
			neutral = row[2] == "0" && row[3] == "t"
		}
	}
	check(partial && neutral, "replay lost partial credit or neutral war")
	asOf = time.Date(2024, 3, 31, 12, 0, 0, 0, time.UTC)
	leapBoundary := time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC)
	coverage, rows, err = run("leap-calendar", []input{{40, fixture(leapBoundary, 3), "random"}, {41, fixture(leapBoundary.Add(-time.Second), 3), "random"}})
	must(err)
	check(coverage["canonical_wars"] == float64(1) && coverage["cutoff"] == leapBoundary.Format(time.RFC3339), "leap cutoff differs from PostgreSQL calendar semantics")
	for _, row := range rows[1:] {
		check(row[2] == "2" && row[3] == "t", "leap boundary result incorrect")
	}
	var after int64
	must(db.QueryRow(ctx, `SELECT(SELECT count(*) FROM war_triple_streak_results)+(SELECT count(*) FROM war_triple_streaks)`).Scan(&after))
	check(after == before, "preview modified scored/derived tables")
	fmt.Println("PASS actual archive-rebuild CLI: calendar1m-boundary=true leap-calendar=true regular-only=true partial-credit=true neutral=true canonical-alias=true suffix-reset=true unknown-barrier=true actual-reset-recovery=true conflict-withheld=true one-fetch-per-pack=true score-tables-unchanged=true")
}
