// Manual full-source replay. The source SQL transaction is read-only; all
// scoring writes are restricted to the canonical loopback development DB.
package main

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"clashking_tracking/internal/wararchive"
	"clashking_tracking/scripts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/klauspost/compress/zstd"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	days := flag.Int("days", 30, "completed-war lookback")
	apply := flag.Bool("apply", false, "write only to loopback development DB")
	limit := flag.Int("pack-limit", 0, "optional bounded verification; zero scans all packs")
	container := flag.String("source-container", "", "explicit source Timescale container")
	flag.Parse()
	if *days < 1 || *days > 365 || !regexp.MustCompile(`^timescale-[a-z0-9-]+$`).MatchString(*container) {
		return fmt.Errorf("invalid days or source container")
	}
	ctx := context.Background()
	until := time.Now().UTC()
	from := until.AddDate(0, 0, -*days)
	// Use source-assigned IDs and locators. Pack frames deliberately omit IDs;
	// guessing them from offsets would collide and corrupt repeatable scoring.
	sql := fmt.Sprintf(`BEGIN READ ONLY;
 SET LOCAL statement_timeout='180s';
 COPY (SELECT COALESCE(w.archive_pack_id,0),w.war_id,COALESCE(w.archive_offset,0),COALESCE(w.archive_compressed_bytes,0),floor(extract(epoch FROM w.end_time))::bigint,COALESCE(encode(convert_to(p.payload::text,'UTF8'),'base64'),'')
 FROM wars w LEFT JOIN war_archive_pending p USING(war_id,end_time)
 WHERE w.war_type='random' AND w.state='warEnded' AND w.end_time>='%s' AND w.end_time<='%s'
 ORDER BY COALESCE(w.archive_pack_id,0),w.archive_offset,w.war_id) TO STDOUT WITH CSV;
 ROLLBACK;`, from.Format(time.RFC3339Nano), until.Format(time.RFC3339Nano))
	source, err := os.CreateTemp("", "ck-war-index-*.csv")
	if err != nil {
		return err
	}
	defer os.Remove(source.Name())
	defer source.Close()
	command := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "root@152.53.82.182", fmt.Sprintf(`docker exec -i %s sh -c 'exec psql -X -q -A -t -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d clashking'`, *container))
	command.Stdin = strings.NewReader(sql)
	command.Stdout = source
	command.Stderr = os.Stderr
	if err = command.Run(); err != nil {
		return fmt.Errorf("read-only source export: %w", err)
	}
	if _, err = source.Seek(0, 0); err != nil {
		return err
	}
	fmt.Printf("source window=%s..%s apply=%t\n", from.Format(time.RFC3339), until.Format(time.RFC3339), *apply)
	pool, err := pgxpool.New(ctx, "postgres://clashking_local:clashking_local@127.0.0.1:54330/clashking_dev")
	if err != nil {
		return err
	}
	defer pool.Close()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(wararchive.Dictionary), zstd.WithDecoderMaxMemory(32<<20))
	if err != nil {
		return err
	}
	defer decoder.Close()
	client := &http.Client{Timeout: 90 * time.Second}
	reader := csv.NewReader(source)
	var pack int64 = -1
	var data []byte
	var batch [][]any
	states := make(map[streakKey]*streakSuffix)
	packs, wars, skipped, qualified, resets := 0, 0, 0, 0, 0
	flush := func() error {
		fmt.Printf("packs=%d wars=%d excluded=%d qualified=%d resets=%d\n", packs, wars, skipped, qualified, resets)
		return nil
	}
	for {
		row, e := reader.Read()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if len(row) != 6 {
			return fmt.Errorf("unexpected source index columns")
		}
		nums := make([]int64, 5)
		for i := range nums {
			nums[i], e = strconv.ParseInt(row[i], 10, 64)
			if e != nil {
				return e
			}
		}
		if nums[0] != pack {
			if e = flush(); e != nil {
				return e
			}
			if *limit > 0 && packs >= *limit {
				break
			}
			pack = nums[0]
			data = nil
			if pack > 0 {
				url := "https://wars.clashk.ing/" + wararchive.ObjectKey(pack)
				req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
				resp, e := client.Do(req)
				if e != nil {
					return e
				}
				data, e = io.ReadAll(io.LimitReader(resp.Body, 128<<20))
				resp.Body.Close()
				if e != nil {
					return e
				}
				if resp.StatusCode != 200 || len(data) == 0 || len(data) >= 128<<20 {
					return fmt.Errorf("pack %d download invalid: status=%d bytes=%d", pack, resp.StatusCode, len(data))
				}
				packs++
			}
		}
		var raw []byte
		if pack == 0 {
			raw, e = base64.StdEncoding.DecodeString(strings.ReplaceAll(row[5], "\n", ""))
		} else {
			offset, size := nums[2], nums[3]
			if offset < 0 || size <= 0 || offset+size > int64(len(data)) {
				return fmt.Errorf("war %d pack %d invalid locator", nums[1], pack)
			}
			raw, e = decoder.DecodeAll(data[offset:offset+size], nil)
		}
		if e != nil {
			return e
		}
		if len(raw) == 0 {
			return fmt.Errorf("war %d has neither pack nor pending payload", nums[1])
		}
		war, e := wararchive.Unmarshal(raw)
		if e != nil {
			return e
		}
		end := time.Unix(nums[4], 0).UTC()
		if war.EndTime.Unix() != end.Unix() {
			return fmt.Errorf("war %d frame end does not match index", nums[1])
		}
		outcomes, e := scripts.ScoreWarTripleStreaks(war, "random")
		if e != nil {
			return e
		}
		wars++
		if len(outcomes) == 0 {
			skipped++
		}
		for _, o := range outcomes {
			if o.Qualified {
				qualified++
			} else {
				resets++
			}
			key := streakKey{o.Member.Tag, o.Member.TownhallLevel}
			state := states[key]
			if state == nil {
				state = &streakSuffix{}
				states[key] = state
			}
			state.add(scoredWar{int32(nums[1]), end, o.Qualified, o.Member.Name, o.Clan})
		}
	}
	if err = flush(); err != nil {
		return err
	}
	// Only the latest reset and successful suffix affect the current streak.
	// Retain those exact source events, not millions of redundant earlier fails.
	// Existing local events remain untouched; SQL still merges newer live data.
	stored := 0
	for key, state := range states {
		appendRow := func(row scoredWar) {
			batch = append(batch, []any{key.tag, key.th, row.id, row.end, row.qualified, row.name, row.clan})
		}
		if state.reset != nil {
			appendRow(*state.reset)
		}
		for _, row := range state.wins {
			appendRow(row)
		}
		if len(batch) >= 50000 {
			if *apply {
				if err = store(ctx, pool, batch); err != nil {
					return err
				}
			}
			stored += len(batch)
			batch = nil
			fmt.Printf("materialized=%d player_keys=%d\n", stored, len(states))
		}
	}
	if len(batch) > 0 {
		if *apply {
			if err = store(ctx, pool, batch); err != nil {
				return err
			}
		}
		stored += len(batch)
	}
	fmt.Printf("DONE packs=%d wars=%d excluded=%d qualified=%d resets=%d apply=%t\n", packs, wars, skipped, qualified, resets, *apply)
	return nil
}

type streakKey struct {
	tag string
	th  int
}
type scoredWar struct {
	id         int32
	end        time.Time
	qualified  bool
	name, clan string
}
type streakSuffix struct {
	reset *scoredWar
	wins  map[int32]scoredWar
}

func later(a, b scoredWar) bool { return a.end.After(b.end) || (a.end.Equal(b.end) && a.id > b.id) }
func (s *streakSuffix) add(row scoredWar) {
	if row.qualified {
		if s.reset != nil && !later(row, *s.reset) {
			return
		}
		if s.wins == nil {
			s.wins = make(map[int32]scoredWar)
		}
		s.wins[row.id] = row
		return
	}
	if s.reset != nil && !later(row, *s.reset) {
		return
	}
	s.reset = &row
	for id, win := range s.wins {
		if !later(win, row) {
			delete(s.wins, id)
		}
	}
}

func store(ctx context.Context, pool *pgxpool.Pool, rows [][]any) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	// A bounded temporary batch commits atomically. Replay
	// never clears existing history, so a failure or rerun cannot erase progress.
	_, err = tx.Exec(ctx, `CREATE TEMP TABLE triple_replay (LIKE war_triple_streak_results INCLUDING DEFAULTS) ON COMMIT DROP`)
	if err != nil {
		return err
	}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{"triple_replay"}, []string{"player_tag", "townhall_level", "war_id", "war_end_time", "qualified", "player_name", "clan_tag"}, pgx.CopyFromRows(rows))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO war_triple_streak_results SELECT * FROM triple_replay ON CONFLICT DO NOTHING;
 CREATE TEMP TABLE triple_keys ON COMMIT DROP AS SELECT DISTINCT player_tag,townhall_level FROM triple_replay;
 CREATE UNIQUE INDEX ON triple_keys(player_tag,townhall_level);
 ANALYZE triple_keys;
 WITH ranked AS (
 SELECT r.*,row_number() OVER(PARTITION BY r.player_tag,r.townhall_level ORDER BY r.war_end_time DESC,r.war_id DESC) AS position,
 count(*) FILTER(WHERE NOT r.qualified) OVER(PARTITION BY r.player_tag,r.townhall_level ORDER BY r.war_end_time DESC,r.war_id DESC ROWS UNBOUNDED PRECEDING) AS resets
 FROM war_triple_streak_results r JOIN triple_keys k USING(player_tag,townhall_level)
 ), totals AS (
 SELECT player_tag,townhall_level,(count(*) FILTER(WHERE qualified AND resets=0)*2)::integer AS streak FROM ranked GROUP BY player_tag,townhall_level
 ) INSERT INTO war_triple_streaks(player_tag,townhall_level,streak,last_attack_time,player_name,clan_tag)
 SELECT r.player_tag,r.townhall_level,t.streak,r.war_end_time,r.player_name,r.clan_tag FROM ranked r JOIN totals t USING(player_tag,townhall_level) WHERE r.position=1
 ON CONFLICT(player_tag,townhall_level) DO UPDATE SET streak=EXCLUDED.streak,last_attack_time=EXCLUDED.last_attack_time,player_name=EXCLUDED.player_name,clan_tag=EXCLUDED.clan_tag`)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
