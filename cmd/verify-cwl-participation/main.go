// Exercise the actual manual command with disposable SQL and an HTTP archive.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"

	"clashking_tracking/internal/wararchive"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
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
	_, e = db.Exec(ctx, `INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,war_size,rounds,state) VALUES('testcwl00001','2026-09',48000022,15,'[{"warTags":["#PYR"]}]','ended')`)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO cwl_group_clans(cwl_id,clan_tag) VALUES('testcwl00001','#PYL'),('testcwl00001','#PYQ')`)
	must(e)
	_, e = db.Exec(ctx, `INSERT INTO cwl_group_members(cwl_id,clan_tag,tag,town_hall) VALUES('testcwl00001','#PYL','#PYY',18),('testcwl00001','#PYQ','#PYP',17)`)
	must(e)
	war := wararchive.War{Clan: wararchive.Clan{Members: []wararchive.Member{{Tag: "#PYY", TownhallLevel: 18, Attacks: []wararchive.Attack{{DefenderTag: "#PYP", Stars: 3}}}}}, Opponent: wararchive.Clan{Members: []wararchive.Member{{Tag: "#PYP", TownhallLevel: 18, Attacks: []wararchive.Attack{{DefenderTag: "#PYY", Stars: 2}}}}}}
	raw, e := json.Marshal(war)
	must(e)
	encoder, e := zstd.NewWriter(nil, zstd.WithEncoderDict(wararchive.Dictionary))
	must(e)
	frame := encoder.EncodeAll(raw, nil)
	encoder.Close()
	archive := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.Header.Get("Range") != fmt.Sprintf("bytes=0-%d", len(frame)-1) {
			http.Error(w, "unexpected archive request", 400)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(frame)))
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(frame)-1, len(frame)))
		w.WriteHeader(206)
		w.Write(frame)
	}))
	defer archive.Close()
	var pack int64
	must(db.QueryRow(ctx, `INSERT INTO war_archive_packs(status,source) VALUES('uploaded','live') RETURNING pack_id`).Scan(&pack))
	_, e = db.Exec(ctx, `INSERT INTO wars(clan_tag,opponent_tag,prep_time,start_time,end_time,size,war_type,state,war_tag,archive_pack_id,archive_offset,archive_compressed_bytes) VALUES('#PYL','#PYQ','2026-09-01','2026-09-02','2026-09-03',15,'cwl','warEnded','#PYR',$1,0,$2)`, pack, len(frame))
	must(e)
	u, e := url.Parse(dsn)
	must(e)
	password, _ := u.User.Password()
	run := func() error {
		cmd := exec.Command("go", "run", "./cmd/backfill-cwl-participation", "--seasons", "2026-09", "--apply")
		cmd.Env = append(os.Environ(), "TIMESCALE_HOST="+u.Hostname(), "TIMESCALE_PORT="+u.Port(), "TIMESCALE_USERNAME="+u.User.Username(), "TIMESCALE_PASSWORD="+password, "TIMESCALE_DATABASE="+strings.TrimPrefix(u.Path, "/"), "TIMESCALE_SSLMODE=disable", "WAR_ARCHIVE_S3_ENDPOINT="+archive.URL, "R2_ACCESS_KEY_ID=fixture", "R2_SECRET_ACCESS_KEY=fixture")
		output, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("command: %w: %s", err, output)
		}
		return nil
	}
	for i := 0; i < 2; i++ {
		must(run())
		var groups, clans, players, finished, archived, attacks, triples int
		must(db.QueryRow(ctx, `SELECT group_count,clan_count,registered_player_count,finalized_wars,archived_wars,(same_th_hitrates->0->>'attacks')::int,(same_th_hitrates->0->>'three_stars')::int FROM cwl_participation WHERE season='2026-09'`).Scan(&groups, &clans, &players, &finished, &archived, &attacks, &triples))
		if groups != 1 || clans != 2 || players != 2 || finished != 1 || archived != 1 || attacks != 2 || triples != 1 {
			panic("incorrect published totals")
		}
	}
	_, e = db.Exec(ctx, `INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,war_size,rounds,state) VALUES('testcwl00002','2026-09',48000022,15,'[]','ended');INSERT INTO cwl_group_clans(cwl_id,clan_tag) VALUES('testcwl00002','#PYL')`)
	must(e)
	if run() == nil {
		panic("duplicate accepted")
	}
	var clans int
	must(db.QueryRow(ctx, `SELECT clan_count FROM cwl_participation WHERE season='2026-09'`).Scan(&clans))
	if clans != 2 {
		panic("valid publication replaced after failure")
	}
	fmt.Println("PASS actual CWL command: SQL counts, HTTP archive decoding, same-TH triples, idempotency and failed-publication preservation")
}
