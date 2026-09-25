//go:build script_internal_tests

package scripts

import (
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRosterAutomationEventOffsetPostgres(t *testing.T) {
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") != "1" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("requires disposable authoritative Goose schema")
	}
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	const server = "953456789012345678"
	const roster = "ad135787-67c3-40a3-8ca3-d20364532980"
	now := time.Now().UTC().Truncate(time.Second)
	if _, err = pool.Exec(ctx, `INSERT INTO servers (id, name) VALUES ($1, 'Offset fixture')`, server); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO rosters (id, server_id, alias, roster_type, signup_scope, event_start_time) VALUES ($1, $2, 'One time', 'clan', 'anyone', $3)`, roster, server, now.Add(48*time.Hour).Unix()); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO roster_automation_rules (automation_id, server_id, roster_id, action_type, scheduled_at, event_offset_days) VALUES ('offset-fixture', $1, $2, 'roster_post', $3, -2)`, server, roster, now.Add(30*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	store := &timescaleRosterAutomationStore{pool: pool}
	rows, err := store.ClaimDue(ctx, now.Add(-time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatal("claimed before event-relative due time")
	}
	rows, err = store.ClaimDue(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].ScheduledAt.Equal(now) {
		t.Fatalf("unexpected relative execution: %+v", rows)
	}
	if err = store.MarkDispatched(ctx, rows[0], now); err != nil {
		t.Fatal(err)
	}
	rows, err = store.ClaimDue(ctx, now.Add(time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatal("one-time event repeated")
	}
	if _, err = pool.Exec(ctx, `UPDATE rosters SET event_start_time = $2 WHERE id = $1`, roster, now.Add(72*time.Hour).Unix()); err != nil {
		t.Fatal(err)
	}
	rows, err = store.ClaimDue(ctx, now.Add(24*time.Hour), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !rows[0].ScheduledAt.Equal(now.Add(24*time.Hour)) {
		t.Fatalf("event reschedule was not followed: %+v", rows)
	}
}
