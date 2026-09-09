package cwlstats

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// CurrentAndPreviousUTC returns the two calendar months whose partial CWL
// statistics are refreshed by the weekly scheduler.
func CurrentAndPreviousUTC(now time.Time) []string {
	current := time.Date(now.UTC().Year(), now.UTC().Month(), 1, 0, 0, 0, 0, time.UTC)
	return []string{current.Format("2006-01"), current.AddDate(0, -1, 0).Format("2006-01")}
}

// Reconcile delegates slot-safe, lock-serialized replacement to the schema
// function. A nil season slice selects every season represented in source or
// aggregate storage; a non-empty slice replaces only those seasons.
func Reconcile(ctx context.Context, pool *pgxpool.Pool, seasons []string) error {
	if pool == nil {
		return fmt.Errorf("CWL season statistics pool is nil")
	}
	if seasons != nil && len(seasons) == 0 {
		return fmt.Errorf("CWL season statistics requires at least one season or nil for all")
	}
	_, err := pool.Exec(ctx, `CALL public.reconcile_cwl_season_statistics($1::text[])`, seasons)
	if err != nil {
		return fmt.Errorf("reconcile CWL season statistics: %w", err)
	}
	return nil
}
