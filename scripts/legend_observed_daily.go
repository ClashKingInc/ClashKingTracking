package scripts

import (
	"context"
	"fmt"
	"time"

	"github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
)

type observedPlayerDay struct {
	attacks  []legendAttack
	trophies int
}
type observedSide struct {
	observed, complete, invalid, perfect              int
	battles, triples, trophies, destruction, duration int64
	buckets                                           []int32
	stars                                             []int64
	attacks                                           []legendAttack
}

// RebuildObservedLegendDays never requires or fabricates rank membership.
// The CLI supplies a validated local DSN; scheduled closeouts use their own store.
func RebuildObservedLegendDays(ctx context.Context, dsn string, days []time.Time) (int, error) {
	store, err := newTimescaleScheduledStore(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer store.pool.Close()
	static, err := clashy.LoadStaticData()
	if err != nil {
		return 0, err
	}
	writes := 0
	for _, day := range days {
		n, err := store.rebuildObservedLegendDay(ctx, dayStart(day), static)
		if err != nil {
			return writes, fmt.Errorf("observed Legend day %s: %w", day.Format("2006-01-02"), err)
		}
		writes += n
		fmt.Printf("observed Legend day %s rebuilt\n", day.Format("2006-01-02"))
	}
	return writes, nil
}

func (s *timescaleScheduledStore) FinalizeObservedLegendDay(ctx context.Context, day time.Time) (int, error) {
	static, err := clashy.LoadStaticData()
	if err != nil {
		return 0, err
	}
	return s.rebuildObservedLegendDay(ctx, dayStart(day), static)
}

func (s *timescaleScheduledStore) rebuildObservedLegendDay(ctx context.Context, day time.Time, static *clashy.StaticData) (int, error) {
	start, end := legendDayWindow(day)
	if end.After(time.Now().UTC()) {
		return 0, fmt.Errorf("cannot finalize an incomplete Legend day")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, legendCloseoutLockID); err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT player_tag,direction,stars,destruction_percentage,duration_seconds,coalesce(share_code,'')
      FROM battles_ranked WHERE battle_mode=2 AND battle_time >= $1 AND battle_time < $2`, start, end)
	if err != nil {
		return 0, err
	}
	type playerKey struct {
		player    string
		direction int
	}
	players := map[playerKey]*observedPlayerDay{}
	population := map[string]bool{}
	usage := map[string]int64{}
	for rows.Next() {
		var attack legendAttack
		var direction int
		if err = rows.Scan(&attack.player, &direction, &attack.stars, &attack.destruction, &attack.duration, &attack.code); err != nil {
			rows.Close()
			return 0, err
		}
		if direction != 1 && direction != 2 {
			continue
		}
		key := playerKey{attack.player, direction}
		player := players[key]
		if player == nil {
			player = &observedPlayerDay{}
			players[key] = player
		}
		player.attacks = append(player.attacks, attack)
		player.trophies += observedAttackTrophies(attack)
		population[attack.player] = true
		if attack.code != "" {
			usage[attack.code]++
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	// Use the same canonical decoder and classifier as the ranked closeout.
	decoded, err := decodeArmies(static, usage)
	if err != nil {
		return 0, err
	}
	sides := map[int]*observedSide{1: {buckets: make([]int32, 16), stars: make([]int64, 4)}, 2: {buckets: make([]int32, 16), stars: make([]int64, 4)}}
	for key, player := range players {
		side := sides[key.direction]
		side.observed++
		// Exactly eight attacks define a complete day. Do not clamp invalid
		// >8 observations or include them in rates, army counts or buckets.
		if len(player.attacks) > 8 {
			side.invalid++
			continue
		}
		triples := 0
		for _, attack := range player.attacks {
			side.battles++
			side.stars[attack.stars]++
			side.destruction += int64(attack.destruction)
			side.duration += int64(attack.duration)
			if attack.stars == 3 {
				side.triples++
				triples++
			}
		}
		side.attacks = append(side.attacks, player.attacks...)
		if len(player.attacks) == 8 {
			side.complete++
			side.trophies += int64(player.trophies)
			bucket := player.trophies / 20
			if bucket > 15 {
				bucket = 15
			}
			side.buckets[bucket]++
			if triples == 8 {
				side.perfect++
			}
		}
	}
	for direction, side := range sides {
		kinds := countLegendAttackTypes(side.attacks, decoded, static.LookupByID)
		_, err = tx.Exec(ctx, `INSERT INTO legend_observed_daily_stats (
          day,direction,source_window_start,source_window_end,eligible_player_count,observed_player_count,
          complete_player_count,invalid_player_count,observed_battle_count,observed_triple_count,
          perfect_player_count,complete_trophy_sum,trophy_buckets,star_counts,destruction_sum,duration_sum,attack_types
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        ON CONFLICT(day,direction) DO UPDATE SET
          source_window_start=excluded.source_window_start,source_window_end=excluded.source_window_end,
          eligible_player_count=excluded.eligible_player_count,observed_player_count=excluded.observed_player_count,
          complete_player_count=excluded.complete_player_count,invalid_player_count=excluded.invalid_player_count,
          observed_battle_count=excluded.observed_battle_count,observed_triple_count=excluded.observed_triple_count,
          perfect_player_count=excluded.perfect_player_count,complete_trophy_sum=excluded.complete_trophy_sum,
          trophy_buckets=excluded.trophy_buckets,star_counts=excluded.star_counts,
          destruction_sum=excluded.destruction_sum,duration_sum=excluded.duration_sum,attack_types=excluded.attack_types,calculated_at=now()`,
			day, direction, start, end, len(population), side.observed, side.complete, side.invalid, side.battles, side.triples,
			side.perfect, side.trophies, side.buckets, side.stars, side.destruction, side.duration, []int64{kinds.air, kinds.ground, kinds.mixed, kinds.unknown})
		if err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return 2, nil
}

func observedAttackTrophies(a legendAttack) int {
	switch a.stars {
	case 3:
		return 40
	case 2:
		return 16 + min(16, max(0, (a.destruction-50)/3))
	case 1:
		return 5 + min(10, max(0, (a.destruction-1)/9))
	default:
		return min(4, a.destruction/10)
	}
}
