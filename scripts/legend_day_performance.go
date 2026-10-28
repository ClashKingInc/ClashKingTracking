package scripts

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RebuildLegendDayPerformance calculates observations only for existing daily
// ranking snapshots. It neither invents historic ranks nor recaptures current
// ranks for old days. The caller must select an appropriate local/production DSN.
func RebuildLegendDayPerformance(ctx context.Context, dsn string, days []time.Time) (int, error) {
	store, err := newTimescaleScheduledStore(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer store.pool.Close()
	writes := 0
	for _, day := range days {
		tx, err := store.pool.Begin(ctx)
		if err != nil {
			return writes, err
		}
		n, err := func() (int, error) {
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, armyFamilyCloseoutLockID); err != nil {
				return 0, err
			}
			n, err := rebuildLegendDayPerformanceTx(ctx, tx, dayStart(day))
			if err != nil {
				return 0, err
			}
			return n, tx.Commit(ctx)
		}()
		if err != nil {
			return writes, fmt.Errorf("Legend performance %s: %w", day.Format("2006-01-02"), err)
		}
		writes += n
	}
	return writes, nil
}

func rebuildLegendDayPerformanceTx(ctx context.Context, tx pgx.Tx, day time.Time) (int, error) {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM legend_rankings_history WHERE day=$1)`, day).Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return 0, fmt.Errorf("ranking snapshot unavailable")
	}
	// Persist the game-day window so readers can reject summaries from the
	// previous 05:10 implementation until those days are rebuilt at 05:00.
	start, end := legendDayWindow(day)
	_, err := tx.Exec(ctx, `
        WITH observed AS (
            SELECT b.player_tag, b.direction,
                   count(*)::integer AS battles,
                   count(*) FILTER (WHERE b.stars=3)::integer AS triples,
                   sum(CASE
                       WHEN b.stars=3 THEN 40
                       WHEN b.stars=2 THEN 16 + least(16, greatest(0, floor((b.destruction_percentage-50)/3.0)::integer))
                       WHEN b.stars=1 THEN 5 + least(10, greatest(0, floor((b.destruction_percentage-1)/9.0)::integer))
                       ELSE least(4, floor(b.destruction_percentage/10.0)::integer)
                   END)::integer AS trophies
            FROM battles_ranked b
            JOIN legend_rankings_history r ON r.tag=b.player_tag AND r.day=$1
            WHERE b.battle_mode=2 AND b.battle_time >= $2 AND b.battle_time < $3
            GROUP BY b.player_tag, b.direction
        )
        UPDATE legend_rankings_history r SET
            observed_attack_count=coalesce(a.battles,0),
            observed_attack_triples=coalesce(a.triples,0),
            observed_attack_trophies=coalesce(a.trophies,0),
            observed_defense_count=coalesce(d.battles,0),
            observed_defense_triples=coalesce(d.triples,0),
            observed_defense_trophies=coalesce(d.trophies,0),
            performance_calculated_at=now()
        FROM legend_rankings_history source
        LEFT JOIN observed a ON a.player_tag=source.tag AND a.direction=1
        LEFT JOIN observed d ON d.player_tag=source.tag AND d.direction=2
        WHERE source.day=$1 AND r.day=source.day AND r.tag=source.tag
    `, day, start, end)
	if err != nil {
		return 0, err
	}
	// Each player contributes once per side. Only exactly eight observations
	// enter the distribution; >8 is an explicit invalid observation, not a day
	// to clamp to 320. Defense trophies are stored as positive loss magnitude.
	// Bucket indices 0..14 cover 0..299 in steps of 20; index 15 includes
	// both 300..319 and 320, with exact perfect days counted separately.
	result, err := tx.Exec(ctx, `
        WITH members AS (
            SELECT r.*, c.cohort
            FROM legend_rankings_history r
            CROSS JOIN (VALUES ('legend_i'),('top_1000'),('top_200')) c(cohort)
            WHERE r.day=$1 AND (c.cohort='legend_i'
                OR c.cohort='top_1000' AND r.global_rank <= 1000
                OR c.cohort='top_200' AND r.global_rank <= 200)
        ), sides AS (
            SELECT m.cohort, side.direction, side.battles, side.triples, side.trophies
            FROM members m
            CROSS JOIN LATERAL (VALUES
                (1, m.observed_attack_count, m.observed_attack_triples, m.observed_attack_trophies),
                (2, m.observed_defense_count, m.observed_defense_triples, m.observed_defense_trophies)
            ) side(direction,battles,triples,trophies)
        ), totals AS (
            SELECT c.cohort,d.direction,
                count(s.battles)::integer AS eligible,
                count(*) FILTER (WHERE battles>0)::integer AS observed,
                count(*) FILTER (WHERE battles=8)::integer AS complete,
                count(*) FILTER (WHERE battles>8)::integer AS invalid,
                coalesce(sum(battles) FILTER (WHERE battles<=8),0)::bigint AS battles,
                coalesce(sum(triples) FILTER (WHERE battles<=8),0)::bigint AS triples,
                count(*) FILTER (WHERE battles=8 AND triples=8)::integer AS perfect,
                coalesce(sum(trophies) FILTER (WHERE battles=8),0)::bigint AS trophies
            FROM (VALUES ('legend_i'),('top_1000'),('top_200')) c(cohort)
            CROSS JOIN (VALUES (1),(2)) d(direction)
            LEFT JOIN sides s ON s.cohort=c.cohort AND s.direction=d.direction
            GROUP BY c.cohort,d.direction
        )
        INSERT INTO legend_day_performance (
            day,cohort,direction,source_window_start,source_window_end,
            eligible_player_count,observed_player_count,complete_player_count,invalid_player_count,
            observed_battle_count,observed_triple_count,perfect_player_count,complete_trophy_sum,trophy_buckets
        )
        SELECT $1,t.cohort,t.direction,$2,$3,t.eligible,t.observed,t.complete,t.invalid,
               t.battles,t.triples,t.perfect,t.trophies,
               ARRAY(SELECT count(s.battles)::integer FROM generate_series(0,15) bucket
                     LEFT JOIN sides s ON s.cohort=t.cohort AND s.direction=t.direction
                         AND s.battles=8 AND least(s.trophies/20,15)=bucket
                     GROUP BY bucket ORDER BY bucket)
        FROM totals t
        ON CONFLICT (day,cohort,direction) DO UPDATE SET
            source_window_start=excluded.source_window_start,source_window_end=excluded.source_window_end,
            eligible_player_count=excluded.eligible_player_count,observed_player_count=excluded.observed_player_count,
            complete_player_count=excluded.complete_player_count,invalid_player_count=excluded.invalid_player_count,
            observed_battle_count=excluded.observed_battle_count,observed_triple_count=excluded.observed_triple_count,
            perfect_player_count=excluded.perfect_player_count,complete_trophy_sum=excluded.complete_trophy_sum,
            trophy_buckets=excluded.trophy_buckets,calculated_at=now()
    `, day, start, end)
	if err != nil {
		return 0, err
	}
	return int(result.RowsAffected()), nil
}
