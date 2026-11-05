// Manual, local-only archive replay. The default is a coverage report; --apply
// writes outcomes using the exact producer called by the live war finalizer.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"clashking_tracking/internal/wararchive"
	"clashking_tracking/scripts"
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
	days := flag.Int("days", 60, "completed regular-war lookback")
	apply := flag.Bool("apply", false, "write results to the local database")
	dsn := flag.String("dsn", "postgres://clashking_local:clashking_local@127.0.0.1:54330/clashking_dev", "local database only")
	flag.Parse()
	u, err := url.Parse(*dsn)
	if err != nil {
		return err
	}
	if u.Hostname() != "127.0.0.1" || u.Port() != "54330" || u.Path != "/clashking_dev" {
		return fmt.Errorf("only the canonical loopback development database is allowed")
	}
	if *days < 1 || *days > 365 {
		return fmt.Errorf("days must be 1..365")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, *dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	cutoff := time.Now().UTC().AddDate(0, 0, -*days)
	var total, pending, packed int
	err = pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE p.payload IS NOT NULL),count(*) FILTER(WHERE p.payload IS NULL AND w.archive_pack_id IS NOT NULL)
 FROM wars w LEFT JOIN war_archive_pending p USING(war_id,end_time)
 WHERE w.war_type='random' AND w.state='warEnded' AND w.end_time >= $1 AND w.end_time<=now()`, cutoff).Scan(&total, &pending, &packed)
	if err != nil {
		return err
	}
	fmt.Printf("local coverage days=%d regular_wars=%d pending=%d packed=%d missing=%d apply=%t\n", *days, total, pending, packed, total-pending-packed, *apply)
	if !*apply {
		return nil
	}
	if total != pending+packed {
		return fmt.Errorf("archive coverage incomplete; no writes performed")
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderDicts(wararchive.Dictionary), zstd.WithDecoderMaxMemory(32<<20))
	if err != nil {
		return err
	}
	defer decoder.Close()
	client := &http.Client{Timeout: 15 * time.Second}
	lastEnd := cutoff
	var lastID int32
	processed := 0
	for {
		rows, err := pool.Query(ctx, `SELECT w.war_id,w.end_time,p.payload,w.archive_pack_id,w.archive_offset,w.archive_compressed_bytes FROM wars w LEFT JOIN war_archive_pending p USING(war_id,end_time)
   WHERE w.war_type='random' AND w.state='warEnded' AND w.end_time<=now() AND (w.end_time,w.war_id)>($1,$2)
   ORDER BY w.end_time,w.war_id LIMIT 100`, lastEnd, lastID)
		if err != nil {
			return err
		}
		type record struct {
			id           int32
			end          time.Time
			payload      []byte
			pack, offset *int64
			size         *int
		}
		var batch []record
		for rows.Next() {
			var r record
			if err = rows.Scan(&r.id, &r.end, &r.payload, &r.pack, &r.offset, &r.size); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, r)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		for _, r := range batch {
			raw := r.payload
			if len(raw) == 0 {
				if r.pack == nil || r.offset == nil || r.size == nil || *r.size <= 0 || *r.size > 2<<20 {
					return fmt.Errorf("war %d has no valid archive locator", r.id)
				}
				req, _ := http.NewRequestWithContext(ctx, "GET", "https://wars.clashk.ing/"+wararchive.ObjectKey(*r.pack), nil)
				end := *r.offset + int64(*r.size) - 1
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", *r.offset, end))
				resp, e := client.Do(req)
				if e != nil {
					return e
				}
				frame, e := io.ReadAll(io.LimitReader(resp.Body, int64(*r.size)+1))
				resp.Body.Close()
				if e != nil {
					return e
				}
				var from, to, length int64
				_, e = fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &from, &to, &length)
				if resp.StatusCode != 206 || e != nil || from != *r.offset || to != end || length <= end || len(frame) != *r.size {
					return fmt.Errorf("war %d archive range mismatch", r.id)
				}
				raw, e = decoder.DecodeAll(frame, nil)
				if e != nil {
					return e
				}
			}
			war, e := wararchive.Unmarshal(raw)
			if e != nil {
				return e
			}
			tx, e := pool.Begin(ctx)
			if e != nil {
				return e
			}
			e = scripts.RecordWarTripleStreaks(ctx, tx, r.id, r.end, war, "random")
			if e != nil {
				tx.Rollback(ctx)
				return e
			}
			if e = tx.Commit(ctx); e != nil {
				return e
			}
			lastEnd, lastID = r.end, r.id
			processed++
		}
		fmt.Printf("replayed=%d last_war=%d\n", processed, lastID)
	}
	fmt.Printf("done replayed=%d; streaks are observed lower bounds at the lookback boundary\n", processed)
	return nil
}
