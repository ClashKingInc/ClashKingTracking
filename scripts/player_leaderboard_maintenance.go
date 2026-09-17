package scripts

import (
	"context"
	"errors"
	"fmt"
	"time"

	"clashking_tracking/internal/platform"
	"github.com/jackc/pgx/v5"
)

// UTC, after the 05:12 closeout and before the 17:00 tournament opening.
// Never catch up a missed destructive reset outside this half-hour window.
func rankedResetWindow(now time.Time) (time.Time, bool) {
	now = now.UTC()
	day := dayStart(now)
	return day, now.Weekday() == time.Monday && !now.Before(day.Add(16*time.Hour+30*time.Minute)) && now.Before(day.Add(17*time.Hour))
}

func (s *timescaleLeaderboardStore) runPlayerLeaderboardMaintenance(ctx context.Context, app *platform.App) {
	for {
		if err := s.playerLeaderboardMaintenance(ctx); err != nil && ctx.Err() == nil {
			app.Logger.Error("player leaderboard maintenance failed", "err", err)
			app.Stats.SetReady("leaderboards.maintenance", false, err.Error())
		} else if ctx.Err() == nil {
			app.Stats.SetReady("leaderboards.maintenance", true, "")
		}
		if err := sleepOrDone(ctx, time.Minute); err != nil {
			return
		}
	}
}

func (s *timescaleLeaderboardStore) playerLeaderboardMaintenance(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	// Session lock serializes maintenance across overlapping scheduled deployments.
	var locked bool
	if err = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(73194201)").Scan(&locked); err != nil || !locked {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, e := conn.Exec(unlockCtx, "SELECT pg_advisory_unlock(73194201)"); e != nil {
			_ = conn.Conn().Close(unlockCtx)
		}
	}()
	var now time.Time
	if err = conn.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return err
	}
	if period, due := rankedResetWindow(now); due {
		for {
			done, e := resetRankedTrophyBatch(ctx, conn.Conn(), period)
			if e != nil {
				return e
			}
			if done {
				break
			}
		}
	}
	var due bool
	err = conn.QueryRow(ctx, `SELECT NOT EXISTS (
   SELECT 1 FROM tracking_scheduled_jobs refresh WHERE job='player_board_refresh'
   AND completed_at > clock_timestamp()-interval '6 hours'
   AND completed_at >= COALESCE((SELECT max(completed_at) FROM tracking_scheduled_jobs WHERE job='ranked_trophy_reset'),'-infinity'::timestamptz)
 )`).Scan(&due)
	if err != nil || !due {
		return err
	}
	for _, view := range []string{"player_townhall_leaderboards", "player_league_leaderboards"} {
		var populated bool
		if err = conn.QueryRow(ctx, "SELECT ispopulated FROM pg_matviews WHERE schemaname='public' AND matviewname=$1", view).Scan(&populated); err != nil {
			return err
		}
		query := "REFRESH MATERIALIZED VIEW "
		if populated {
			query += "CONCURRENTLY "
		}
		if _, err = conn.Exec(ctx, query+view); err != nil {
			return err
		}
	}
	_, err = conn.Exec(ctx, `INSERT INTO tracking_scheduled_jobs(job,period,completed_at)
 VALUES('player_board_refresh',(clock_timestamp() AT TIME ZONE 'UTC')::date,clock_timestamp())
 ON CONFLICT(job,period) DO UPDATE SET completed_at=EXCLUDED.completed_at`)
	return err
}

type rankedResetConnection interface {
	Begin(context.Context) (pgx.Tx, error)
}

func resetRankedTrophyBatch(ctx context.Context, conn rankedResetConnection, period time.Time) (bool, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return false, err
	}
	return resetRankedTrophyBatchTx(ctx, tx, period, now)
}

// Separated from the database clock for deterministic safe-window tests.
func resetRankedTrophyBatchTx(ctx context.Context, tx pgx.Tx, period, now time.Time) (bool, error) {
	current, allowed := rankedResetWindow(now)
	if !allowed || !current.Equal(period) {
		return false, errors.New("weekly trophy reset safe window closed; remaining rows left unchanged")
	}
	var closeoutDone bool
	var err error
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tracking_scheduled_jobs WHERE job='ranked_closeout' AND period=$1 AND completed_at IS NOT NULL)`, period).Scan(&closeoutDone); err != nil {
		return false, err
	}
	if !closeoutDone {
		return false, fmt.Errorf("weekly trophy reset waiting for ranked closeout %s", period.Format("2006-01-02"))
	}
	if _, err = tx.Exec(ctx, `INSERT INTO tracking_scheduled_jobs(job,period) VALUES('ranked_trophy_reset',$1) ON CONFLICT DO NOTHING`, period); err != nil {
		return false, err
	}
	var lastTag string
	var completed *time.Time
	if err = tx.QueryRow(ctx, `SELECT last_tag,completed_at FROM tracking_scheduled_jobs WHERE job='ranked_trophy_reset' AND period=$1 FOR UPDATE`, period).Scan(&lastTag, &completed); err != nil {
		return false, err
	}
	if completed != nil {
		return true, nil
	}
	var next string
	var count int
	err = tx.QueryRow(ctx, `WITH batch AS MATERIALIZED (
 SELECT tag FROM basic_player WHERE tag>$1 AND league_id BETWEEN 105000000 AND 105000035 AND trophies<>0 ORDER BY tag LIMIT 5000
 ), changed AS (UPDATE basic_player p SET trophies=0 FROM batch b WHERE p.tag=b.tag
 AND p.league_id BETWEEN 105000000 AND 105000035 AND p.trophies<>0 RETURNING p.tag)
 SELECT COALESCE((SELECT max(tag) FROM batch),$1),(SELECT count(*) FROM changed)`, lastTag).Scan(&next, &count)
	if err != nil {
		return false, err
	}
	done := next == lastTag
	if _, err = tx.Exec(ctx, `UPDATE tracking_scheduled_jobs SET last_tag=$2,completed_at=CASE WHEN $3 THEN clock_timestamp() ELSE NULL END WHERE job='ranked_trophy_reset' AND period=$1`, period, next, done); err != nil {
		return false, err
	}
	return done, tx.Commit(ctx)
}
