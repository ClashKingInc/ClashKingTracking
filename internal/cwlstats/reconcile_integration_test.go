//go:build integration

package cwlstats

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReconcileScopesIdempotencyAndIndependentJoins(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" {
		t.Skip("requires disposable Timescale")
	}
	pool, err := pgxpool.New(t.Context(), os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	now := time.Now().UTC()
	current, previous := CurrentAndPreviousUTC(now)[0], CurrentAndPreviousUTC(now)[1]
	old := time.Date(now.Year(), now.Month()-2, 1, 0, 0, 0, 0, time.UTC).Format("2006-01")
	ids := []string{"STATCURR0001", "STATPREV0001", "STATOLD00001"}
	t.Cleanup(func() {
		_, _ = pool.Exec(t.Context(), `DELETE FROM cwl_group_members WHERE cwl_id=ANY($1); DELETE FROM cwl_group_clans WHERE cwl_id=ANY($1); DELETE FROM cwl_groups WHERE cwl_id=ANY($1); DELETE FROM cwl_season_statistics WHERE season=ANY($2)`, ids, []string{current, previous, old})
	})
	for index, season := range []string{current, previous, old} {
		if _, err := pool.Exec(t.Context(), `INSERT INTO cwl_groups(cwl_id,season,cwl_league_id,rounds,state,war_size) VALUES($1,$2,48000010,'[]','ended',15)`, ids[index], season); err != nil {
			t.Fatal(err)
		}
	}
	for _, clan := range []string{"#P0Y", "#P2Y"} {
		if _, err := pool.Exec(t.Context(), `INSERT INTO cwl_group_clans(cwl_id,clan_tag) VALUES($1,$2)`, ids[0], clan); err != nil {
			t.Fatal(err)
		}
	}
	for index, player := range []string{"#P0Y", "#P2Y", "#P8Y"} {
		if _, err := pool.Exec(t.Context(), `INSERT INTO cwl_group_members(cwl_id,clan_tag,tag,town_hall) VALUES($1,'#P0Y',$2,$3)`, ids[0], player, 18-index); err != nil {
			t.Fatal(err)
		}
	}
	if err := Reconcile(t.Context(), pool, []string{current, previous}); err != nil {
		t.Fatal(err)
	}
	want := []int64{1, 2, 3}
	if got := readCounts(t, pool, current); !reflect.DeepEqual(got, want) {
		t.Fatalf("current counts = %#v, want %#v; joins multiplied an aggregate", got, want)
	}
	if got := readSeasonCount(t, pool, old); got != 0 {
		t.Fatalf("current/previous refresh touched old season: %d rows", got)
	}
	first := readCounts(t, pool, current)
	if err := Reconcile(t.Context(), pool, []string{current, previous}); err != nil {
		t.Fatal(err)
	}
	if second := readCounts(t, pool, current); !reflect.DeepEqual(second, first) {
		t.Fatalf("second reconciliation changed totals: %#v then %#v", first, second)
	}
	if err := Reconcile(t.Context(), pool, nil); err != nil {
		t.Fatal(err)
	}
	if got := readSeasonCount(t, pool, old); got != 1 {
		t.Fatalf("all-season reconciliation produced %d old rows, want 1", got)
	}
}

func readCounts(t *testing.T, pool *pgxpool.Pool, season string) []int64 {
	t.Helper()
	var groups, clans, players int64
	if err := pool.QueryRow(t.Context(), `SELECT group_count,clan_count,registered_player_count FROM cwl_season_statistics WHERE season=$1`, season).Scan(&groups, &clans, &players); err != nil {
		t.Fatal(err)
	}
	return []int64{groups, clans, players}
}

func readSeasonCount(t *testing.T, pool *pgxpool.Pool, season string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(t.Context(), `SELECT count(*) FROM cwl_season_statistics WHERE season=$1`, season).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
