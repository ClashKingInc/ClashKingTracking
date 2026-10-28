// Run the real Legend performance maintenance service against the canonical
// disposable Timescale schema. No mocks, Discord writes, or public requests.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"clashking_tracking/scripts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func assert(ok bool, message string) {
	if !ok {
		panic(message)
	}
}
func main() {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" || !strings.Contains(dsn, "@127.0.0.1:") {
		panic("Disposable loopback fixture required")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	must(err)
	defer db.Close()
	day := time.Now().UTC().Truncate(24 * time.Hour).Add(-4 * 24 * time.Hour)
	start := day.Add(5 * time.Hour)
	tags := []string{"#PYP", "#PYQ", "#PYL", "#PYR", "#PYJ", "#PYC", "#PYG", "#PYV", "#PYU"}
	ranks := []int{1, 201, 1001, 2, 3, 4, 5, 6, 7}
	for i, tag := range tags {
		_, err = db.Exec(ctx, `INSERT INTO legend_rankings_history(day,tag,global_rank,trophies) VALUES($1,$2,$3,5000)`, day, tag, ranks[i])
		must(err)
	}
	insert := func(tag string, direction int, offset time.Duration, stars, destruction int) {
		_, err := db.Exec(ctx, `INSERT INTO battles_ranked
            (player_tag,opponent_tag,battle_time,direction,battle_mode,player_town_hall,opponent_town_hall,stars,destruction_percentage,duration_seconds,looted_resources)
            VALUES($1,'#QQQ',$2,$3,2,18,18,$4,$5,120,CASE WHEN $3::smallint=1 THEN '{}'::jsonb ELSE NULL END) ON CONFLICT DO NOTHING`, tag, start.Add(offset), direction, stars, destruction)
		must(err)
	}
	// Sum boundaries: 0,19,20,299,300,320. Non-three-star totals use
	// seven misses or triples plus one real two-star trophy outcome.
	totals := []int{320, 299, 300, 0, 19, 20}
	for i, total := range totals {
		for battle := 0; battle < 8; battle++ {
			stars, destruction := 0, 0
			if total == 320 || (total >= 299 && battle < 7) {
				stars, destruction = 3, 100
			}
			if battle == 7 && (total == 19 || total == 299) {
				stars, destruction = 2, 59
			}
			if battle == 7 && (total == 20 || total == 300) {
				stars, destruction = 2, 62
			}
			insert(tags[i], 1, time.Duration(battle)*time.Minute, stars, destruction)
		}
	}
	for i := 0; i < 8; i++ {
		insert(tags[0], 2, 12*time.Hour+time.Duration(i)*time.Minute, 3, 100)
	}
	for i := 0; i < 7; i++ {
		insert(tags[1], 2, 12*time.Hour+time.Duration(i)*time.Minute, 3, 100)
		insert(tags[6], 1, time.Duration(i)*time.Minute, 3, 100)
	}
	for i := 0; i < 9; i++ {
		insert(tags[7], 1, time.Duration(i)*time.Minute, 3, 100)
	}
	// These are outside the half-open day window and must not make the
	// perfect day invalid. Reinsert one battle to exercise source idempotence.
	insert(tags[0], 1, -time.Second, 3, 100)
	insert(tags[0], 1, 24*time.Hour, 3, 100)
	insert(tags[0], 1, 0, 3, 100)
	for run := 0; run < 2; run++ {
		count, err := scripts.RebuildLegendDayPerformance(ctx, dsn, []time.Time{day})
		must(err)
		assert(count == 6, "Expected three cohorts and two directions")
		var eligible, observed, complete, invalid, perfect int
		var buckets []int32
		var trophySum int64
		must(db.QueryRow(ctx, `SELECT eligible_player_count,observed_player_count,complete_player_count,invalid_player_count,perfect_player_count,complete_trophy_sum,trophy_buckets FROM legend_day_performance WHERE day=$1 AND cohort='legend_i' AND direction=1`, day).Scan(&eligible, &observed, &complete, &invalid, &perfect, &trophySum, &buckets))
		assert(eligible == 9 && observed == 8 && complete == 6 && invalid == 1 && perfect == 1, "Offense coverage/perfect counts")
		assert(trophySum == 958, "Offense trophy sum")
		assert(len(buckets) == 16 && buckets[0] == 2 && buckets[1] == 1 && buckets[14] == 1 && buckets[15] == 2, "20-trophy bucket boundaries")
		sum := int32(0)
		for _, n := range buckets {
			sum += n
		}
		assert(sum == 6, "Bucket population must equal completed players")
		must(db.QueryRow(ctx, `SELECT observed_player_count,complete_player_count,perfect_player_count FROM legend_day_performance WHERE day=$1 AND cohort='legend_i' AND direction=2`, day).Scan(&observed, &complete, &perfect))
		assert(observed == 2 && complete == 1 && perfect == 1, "Defense completion is independent of offense")
		must(db.QueryRow(ctx, `SELECT complete_player_count FROM legend_day_performance WHERE day=$1 AND cohort='top_200' AND direction=1`, day).Scan(&complete))
		assert(complete == 4, "Snapshot top-200 membership")
	}
	_, err = scripts.RebuildLegendDayPerformance(ctx, dsn, []time.Time{day.Add(-24 * time.Hour)})
	assert(err != nil, "Absent snapshot must not fabricate a day")
	// A second real snapshot moves this player out of the top 200. Historical
	// cohort membership must stay tied to each day's saved rank.
	next := day.Add(24 * time.Hour)
	_, err = db.Exec(ctx, `INSERT INTO legend_rankings_history(day,tag,global_rank,trophies) VALUES($1,$2,1200,5000)`, next, tags[0])
	must(err)
	count, err := scripts.RebuildLegendDayPerformance(ctx, dsn, []time.Time{next})
	must(err)
	assert(count == 6, "Empty cohorts must still produce explicit zero summaries")
	var empty int
	must(db.QueryRow(ctx, `SELECT eligible_player_count FROM legend_day_performance WHERE day=$1 AND cohort='top_200' AND direction=1`, next).Scan(&empty))
	assert(empty == 0, "Empty cohort must stay empty")
	var oldRank, newRank int
	must(db.QueryRow(ctx, `SELECT global_rank FROM legend_rankings_history WHERE day=$1 AND tag=$2`, day, tags[0]).Scan(&oldRank))
	must(db.QueryRow(ctx, `SELECT global_rank FROM legend_rankings_history WHERE day=$1 AND tag=$2`, next, tags[0]).Scan(&newRank))
	assert(oldRank == 1 && newRank == 1200, "Historical rank must remain immutable")
	fmt.Println("PASS: actual Timescale Legend performance closeout; bucket boundaries, both sides, coverage, invalid observations, replay, and snapshot isolation")
}
