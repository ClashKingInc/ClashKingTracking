// Run the real Legend performance maintenance service against the canonical
// disposable Timescale schema. No mocks, Discord writes, or public requests.
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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
	absentWrites, err := scripts.RebuildLegendDayPerformance(ctx, dsn, []time.Time{day.Add(-24 * time.Hour)})
	must(err)
	assert(absentWrites == 0, "Absent snapshot must not fabricate ranked performance")
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
	for run := 0; run < 2; run++ {
		written, err := scripts.RebuildObservedLegendDays(ctx, dsn, []time.Time{day})
		must(err)
		assert(written == 2, "Observed rebuild writes both directions")
		var battles int64
		var complete, invalid int
		must(db.QueryRow(ctx, `SELECT observed_battle_count,complete_player_count,invalid_player_count FROM legend_observed_daily_stats WHERE day=$1 AND direction=1`, day).Scan(&battles, &complete, &invalid))
		assert(battles == 55 && complete == 6 && invalid == 1, "Observed rows exclude the invalid >8-battle day")
	}
	// Without a rank snapshot, observed statistics still rebuild from raw data.
	written, err := scripts.RebuildObservedLegendDays(ctx, dsn, []time.Time{day.Add(-24 * time.Hour)})
	must(err)
	assert(written == 2, "Observed closeout does not require rankings")
	// Full army/item closeouts must work without any historical rank snapshot.
	missingDay := day.Add(-48 * time.Hour)
	for i := 0; i < 8; i++ {
		_, err = db.Exec(ctx, `INSERT INTO battles_ranked(player_tag,opponent_tag,battle_time,direction,battle_mode,player_town_hall,opponent_town_hall,stars,destruction_percentage,duration_seconds,looted_resources,share_code)
		VALUES('#QYQ','#QQQ',$1,1,2,18,18,3,100,120,'{}','u10x8')`, missingDay.Add(5*time.Hour+time.Duration(i)*time.Minute))
		must(err)
	}
	for i := 0; i < 2; i++ {
		_, err = scripts.RebuildLegendCloseouts(ctx, dsn, []time.Time{missingDay})
		must(err)
		var attacks, ranked, setups, performance int
		must(db.QueryRow(ctx, `SELECT attack_count FROM legend_daily_stats WHERE day=$1 AND cohort='legend_i'`, missingDay).Scan(&attacks))
		must(db.QueryRow(ctx, `SELECT count(*) FROM legend_daily_stats WHERE day=$1 AND cohort<>'legend_i'`, missingDay).Scan(&ranked))
		must(db.QueryRow(ctx, `SELECT count(*) FROM army_setup_daily_stats WHERE day=$1 AND rank_limit IS NULL`, missingDay).Scan(&setups))
		must(db.QueryRow(ctx, `SELECT count(*) FROM legend_day_performance WHERE day=$1`, missingDay).Scan(&performance))
		assert(attacks == 8 && ranked == 0 && setups > 0 && performance == 0, "Missing ranks must retain full-cohort totals/setups without fabricated ranked output")
	}
	// Even with a rank snapshot, an observed Legend player missing from that
	// snapshot belongs to All, never to either ranked subset.
	_, err = db.Exec(ctx, `INSERT INTO battles_ranked(player_tag,opponent_tag,battle_time,direction,battle_mode,player_town_hall,opponent_town_hall,stars,destruction_percentage,duration_seconds,looted_resources,share_code)
	VALUES('#QYV','#QQQ',$1,1,2,18,18,3,100,120,'{}','u10x8')`, start.Add(3*time.Hour))
	must(err)
	_, err = scripts.RebuildLegendCloseouts(ctx, dsn, []time.Time{day})
	must(err)
	var allAttacks, top1000Attacks, top200Attacks int
	must(db.QueryRow(ctx, `SELECT attack_count FROM legend_daily_stats WHERE day=$1 AND cohort='legend_i'`, day).Scan(&allAttacks))
	must(db.QueryRow(ctx, `SELECT attack_count FROM legend_daily_stats WHERE day=$1 AND cohort='top_1000'`, day).Scan(&top1000Attacks))
	must(db.QueryRow(ctx, `SELECT attack_count FROM legend_daily_stats WHERE day=$1 AND cohort='top_200'`, day).Scan(&top200Attacks))
	assert(allAttacks == 65 && top1000Attacks == 56 && top200Attacks == 48, "All must include unranked observations without contaminating rank cohorts")

	// Exercise the operator command through the actual disposable database.
	var previewBefore, previewAfter string
	snapshot := `SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY cohort),'[]'::jsonb)::text FROM legend_daily_stats s WHERE day=$1`
	must(db.QueryRow(ctx, snapshot, day).Scan(&previewBefore))
	runCLI := func(args ...string) {
		command := exec.CommandContext(ctx, "go", append([]string{"run", "./cmd/rebuild-legend-days"}, args...)...)
		command.Env = append(os.Environ(), "TIMESCALE_HOST=", "TIMESCALE_URL="+dsn)
		output, err := command.CombinedOutput()
		if err != nil {
			panic(fmt.Errorf("operator command: %w: %s", err, output))
		}
	}
	runCLI("--days", day.Format("2006-01-02"))
	must(db.QueryRow(ctx, snapshot, day).Scan(&previewAfter))
	assert(previewBefore == previewAfter, "Operator preview changed derived rows")
	var rawBefore, rawAfter int
	must(db.QueryRow(ctx, "SELECT count(*) FROM battles_ranked").Scan(&rawBefore))
	runCLI("--from", missingDay.Format("2006-01-02"), "--through", day.Format("2006-01-02"), "--apply")
	must(db.QueryRow(ctx, "SELECT count(*) FROM battles_ranked").Scan(&rawAfter))
	assert(rawBefore == rawAfter, "Operator rebuild deleted raw battles")
	fmt.Println("PASS operator CLI: preview leaves rows unchanged; bounded multi-day apply retains raw battles")
	fmt.Println("PASS full Legends I without ranks: totals, army setups, no fabricated ranked rows, repeatable rebuild")
	fmt.Println("PASS: actual Timescale Legend performance closeout; bucket boundaries, both sides, coverage, invalid observations, replay, and snapshot isolation")
}
