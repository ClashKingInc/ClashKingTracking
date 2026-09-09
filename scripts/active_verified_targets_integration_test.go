//go:build integration

package scripts

import (
	"context"
	"os"
	"slices"
	"testing"
)

func TestBattlelogTargetsFollowDurableVerifiedAppActivity(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" {
		t.Fatal("integration test requires disposable canonical Timescale")
	}
	ctx := context.Background()
	store, err := newTimescaleBattlelogStore(ctx, os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.pool.Exec(ctx, `
		INSERT INTO basic_player (tag, name, league_id, townhall_level)
		VALUES ('#9PPY', 'Active app player', 105000035, 8),
		       ('#9PP0', 'Stale app player', 105000035, 18)
		ON CONFLICT (tag) DO NOTHING;
		INSERT INTO player_links (tag, user_id, source, is_verified, last_login)
		VALUES ('#9PPY', '700000000000000001', 'fixture', true, now()),
		       ('#9PP0', '700000000000000002', 'fixture', true, now() - interval '8 days')
		ON CONFLICT (tag) DO UPDATE SET
			is_verified = EXCLUDED.is_verified,
			last_login = EXCLUDED.last_login;
	`); err != nil {
		t.Fatal(err)
	}

	tags, err := store.LoadTargets(ctx, "standard")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(tags, "#9PPY") {
		t.Fatal("active verified app player was not admitted to standard battlelog tracking")
	}
	if slices.Contains(tags, "#9PP0") {
		t.Fatal("stale verified app player remained in standard battlelog tracking")
	}
}
