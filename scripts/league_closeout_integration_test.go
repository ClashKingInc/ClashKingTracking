//go:build league_analytics_integration

package scripts

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"clashking_tracking/models"
)

func TestFinalSchemaBattleIngestAndLegendCloseoutAreIdempotent(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	store, err := newTimescaleBattlelogStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO basic_player(tag,name,league_id,townhall_level,trophies)
		VALUES('#P0','attacker',105000036,18,6000),('#Y2','defender',105000036,17,5900);
		INSERT INTO legend_rankings_current(tag,name,trophies,global_rank)
		VALUES('#P0','attacker',6000,1),('#Y2','defender',5900,2)
	`); err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 9, 6, 0, 0, 0, 0, time.UTC)
	shareCode := normalizeArmyShareCode("u100x0s10x0h0e0_1")
	attack := models.BattlelogRow{
		ArmyShareCode: shareCode,
		PlayerTag:     "#P0", OpponentTag: "#Y2", OpponentTH: 17, BattleType: "legend", Attack: true,
		Stars: 3, DestructionPercentage: 100, Duration: 120, Timestamp: day.Add(6 * time.Hour),
	}
	defense := attack
	defense.PlayerTag = "#Y2"
	defense.OpponentTag = "#P0"
	defense.OpponentTH = 18
	defense.Attack = false
	first, err := store.Store(ctx, models.BattlelogIngest{Rows: []models.BattlelogRow{attack}})
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 {
		t.Fatalf("one requested attack inserted %d rows", first)
	}
	defenseInserted, err := store.Store(ctx, models.BattlelogIngest{Rows: []models.BattlelogRow{defense}})
	if err != nil || defenseInserted != 1 {
		t.Fatalf("requested defense inserted %d rows: %v", defenseInserted, err)
	}
	second, err := store.Store(ctx, models.BattlelogIngest{Rows: []models.BattlelogRow{attack, defense}})
	if err != nil {
		t.Fatal(err)
	}
	if first != 1 || second != 0 {
		t.Fatalf("writes = %d then %d, want 1 then 0", first, second)
	}
	var total, attacks int
	if err := store.pool.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE direction=1) FROM battles_ranked`).Scan(&total, &attacks); err != nil {
		t.Fatal(err)
	}
	if total != 2 || attacks != 1 {
		t.Fatalf("perspectives=%d attacks=%d", total, attacks)
	}
	rows, err := store.pool.Query(ctx, `
		SELECT player_tag,direction,player_town_hall,opponent_town_hall
		FROM battles_ranked
		ORDER BY player_tag
	`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type storedPerspective struct {
		playerTag  string
		direction  int16
		playerTH   int
		opponentTH int
	}
	var perspectives []storedPerspective
	for rows.Next() {
		var perspective storedPerspective
		if err := rows.Scan(&perspective.playerTag, &perspective.direction, &perspective.playerTH, &perspective.opponentTH); err != nil {
			t.Fatal(err)
		}
		perspectives = append(perspectives, perspective)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	wantPerspectives := []storedPerspective{
		{playerTag: "#P0", direction: battleDirectionAttack, playerTH: 18, opponentTH: 17},
		{playerTag: "#Y2", direction: battleDirectionDefense, playerTH: 17, opponentTH: 18},
	}
	if !reflect.DeepEqual(perspectives, wantPerspectives) {
		t.Fatalf("perspectives=%#v want=%#v", perspectives, wantPerspectives)
	}

	scheduled := &timescaleScheduledStore{pool: store.pool}
	if _, err := scheduled.FinalizeLegendCloseout(ctx, day); err == nil {
		t.Fatal("closeout without a saved Legend ranking snapshot succeeded")
	}
	snapshotWrites, err := scheduled.CaptureLegendSnapshot(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	if snapshotWrites != 2 {
		t.Fatalf("snapshot writes = %d, want 2", snapshotWrites)
	}
	if _, err := store.pool.Exec(ctx, `
		UPDATE legend_rankings_current SET global_rank = global_rank + 100;
		UPDATE legend_rankings_current
		SET global_rank = CASE tag WHEN '#P0' THEN 2 ELSE 1 END,
		    trophies = trophies + 500
	`); err != nil {
		t.Fatal(err)
	}
	secondSnapshotWrites, err := scheduled.CaptureLegendSnapshot(ctx, day)
	if err != nil {
		t.Fatal(err)
	}
	if secondSnapshotWrites != 0 {
		t.Fatalf("rerun snapshot writes = %d, want preserved historical snapshot", secondSnapshotWrites)
	}
	var preservedRank, preservedTrophies int
	if err := store.pool.QueryRow(ctx, `SELECT global_rank,trophies FROM legend_rankings_history WHERE day=$1 AND tag='#P0'`, day).Scan(&preservedRank, &preservedTrophies); err != nil {
		t.Fatal(err)
	}
	if preservedRank != 1 || preservedTrophies != 6000 {
		t.Fatalf("historical snapshot changed to rank=%d trophies=%d", preservedRank, preservedTrophies)
	}
	for run := 0; run < 2; run++ {
		if _, err := scheduled.FinalizeLegendCloseout(ctx, day); err != nil {
			t.Fatal(err)
		}
	}
	for _, cohort := range legendCloseoutCohorts {
		var attackCount, tripleCount int
		if err := store.pool.QueryRow(ctx, `SELECT attack_count,three_star_count FROM legend_daily_stats WHERE day=$1 AND cohort=$2`, day, cohort).Scan(&attackCount, &tripleCount); err != nil {
			t.Fatal(err)
		}
		if attackCount != 1 || tripleCount != 1 {
			t.Fatalf("%s legend aggregate attacks=%d triples=%d", cohort, attackCount, tripleCount)
		}
	}
	var familyCount, memberCount, historyCount int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM army_families`).Scan(&familyCount); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM army_family_members`).Scan(&memberCount); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM legend_rankings_history WHERE day=$1`, day).Scan(&historyCount); err != nil {
		t.Fatal(err)
	}
	var familyStatsCount int
	if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM army_family_daily_stats WHERE day=$1 AND attack_count=1`, day).Scan(&familyStatsCount); err != nil {
		t.Fatal(err)
	}
	if familyCount != 1 || memberCount != 1 || familyStatsCount != 3 || historyCount != 2 {
		t.Fatalf("families=%d members=%d family cohorts=%d history=%d", familyCount, memberCount, familyStatsCount, historyCount)
	}

	officialDate := day.AddDate(0, 0, 1)
	officialRows := []models.PlayerTrophyHistoryRow{{
		LocationID: "global", Date: officialDate, PlayerTag: "#P0", PlayerName: "attacker",
		ExpLevel: 250, Trophies: 6100, AttackWins: 8, DefenseWins: 4, Rank: 1,
	}}
	if _, err := scheduled.ReplaceLeaderboardHistory(ctx, []leaderboardHistoryGroup{{
		Kind: leaderboardHistoryPlayerHomeTrophies, LocationID: "global", Date: officialDate, Rows: officialRows,
	}}); err != nil {
		t.Fatal(err)
	}
	var officialName string
	if err := store.pool.QueryRow(ctx, `
		SELECT player_name FROM leaderboard_history_player_home
		WHERE location_id='global' AND date=$1 AND player_tag='#P0'
	`, officialDate).Scan(&officialName); err != nil {
		t.Fatal(err)
	}
	if officialName != "attacker" {
		t.Fatalf("official history player_name = %q", officialName)
	}
}

func TestFinalSchemaRankedCloseoutCountsAttackPerspectiveOnce(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required")
	}
	ctx := context.Background()
	store, err := newTimescaleBattlelogStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO basic_player(tag,name,league_id,townhall_level,trophies)
		VALUES('#Q8','ranked attacker',105000034,18,5500),('#G9','ranked defender',105000034,18,5400)
	`); err != nil {
		t.Fatal(err)
	}
	seasonStart := time.Date(2026, 9, 7, 5, 0, 0, 0, time.UTC)
	shareCode := normalizeArmyShareCode("u100x0s10x0h0e0_1")
	row := models.BattlelogRow{
		ArmyShareCode: shareCode,
		PlayerTag:     "#Q8", OpponentTag: "#G9", OpponentTH: 18, BattleType: "ranked", Attack: true,
		Stars: 2, DestructionPercentage: 90, Duration: 150, Timestamp: seasonStart.Add(time.Hour),
	}
	if _, err := store.Store(ctx, models.BattlelogIngest{Rows: []models.BattlelogRow{row}}); err != nil {
		t.Fatal(err)
	}
	scheduled := &timescaleScheduledStore{pool: store.pool}
	members := []models.RankedLeagueGroupMemberRow{
		{SeasonID: seasonStart.Unix(), GroupTag: "#R0", LeagueTierID: 105000034, PlayerTag: "#Q8", PlayerName: "ranked attacker", Placement: 1, LeagueTrophies: 5500, TownHall: 18, MaximumBattleCount: 20, AttackWinCount: 1, AttackStarCount: 2},
		{SeasonID: seasonStart.Unix(), GroupTag: "#R0", LeagueTierID: 105000034, PlayerTag: "#G9", PlayerName: "ranked defender", Placement: 2, LeagueTrophies: 5400, TownHall: 18, MaximumBattleCount: 20, DefenseLossCount: 1, DefenseStarCount: 2},
	}
	if _, err := scheduled.StoreRankedLeagueGroup(ctx, members); err != nil {
		t.Fatal(err)
	}
	for run := 0; run < 2; run++ {
		if _, err := scheduled.FinalizeRankedTournament(ctx, seasonStart.Unix()); err != nil {
			t.Fatal(err)
		}
	}
	var attacks, twoStars int
	if err := store.pool.QueryRow(ctx, `SELECT attack_count,two_star_count FROM league_hitrate_stats WHERE period_kind='ranked_season' AND period_start=$1`, seasonStart).Scan(&attacks, &twoStars); err != nil {
		t.Fatal(err)
	}
	if attacks != 1 || twoStars != 1 {
		t.Fatalf("ranked hitrate attacks=%d two-stars=%d", attacks, twoStars)
	}
	var groupCount, players, participants int
	if err := store.pool.QueryRow(ctx, `SELECT group_count,distinct_player_count,participating_player_count FROM ranked_league_tier_stats WHERE season_id=$1`, seasonStart.Unix()).Scan(&groupCount, &players, &participants); err != nil {
		t.Fatal(err)
	}
	if groupCount != 1 || players != 2 || participants != 1 {
		t.Fatalf("groups=%d players=%d participants=%d", groupCount, players, participants)
	}
}
