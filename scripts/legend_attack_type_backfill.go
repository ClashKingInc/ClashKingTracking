package scripts

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"time"

	"github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LegendAttackTypeBackfillTarget must match both the connection string and the
// connected PostgreSQL server. It deliberately contains no credentials.
type LegendAttackTypeBackfillTarget struct {
	Host     string
	Port     uint16
	Database string
}

type LegendAttackTypeBackfillRow struct {
	Day           string                   `json:"day"`
	Cohort        string                   `json:"cohort"`
	StoredAttacks int64                    `json:"storedAttacks"`
	SourceAttacks int64                    `json:"sourceAttacks"`
	Air           int64                    `json:"air"`
	Ground        int64                    `json:"ground"`
	Mixed         int64                    `json:"mixed"`
	Unknown       int64                    `json:"unknown"`
	Changed       bool                     `json:"changed"`
	Previous      LegendAttackTypePrevious `json:"previous"`
}

type LegendAttackTypePrevious struct {
	Air     *int64 `json:"air"`
	Ground  *int64 `json:"ground"`
	Mixed   *int64 `json:"mixed"`
	Unknown *int64 `json:"unknown"`
}

// BackfillLegendAttackTypes updates only the four attack-type columns on
// existing finalized Legend-day rows. Dry run is the default: apply must be
// explicitly true. All selected days are validated before any UPDATE and the
// whole operation uses one advisory-locked transaction.
func BackfillLegendAttackTypes(ctx context.Context, dsn string, target LegendAttackTypeBackfillTarget, days []time.Time, apply bool) ([]LegendAttackTypeBackfillRow, error) {
	if err := validateLegendBackfillTarget(dsn, target); err != nil {
		return nil, err
	}
	if err := validateLegendBackfillDays(days, time.Now().UTC()); err != nil {
		return nil, err
	}
	static, err := clashy.LoadStaticData()
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	opts := pgx.TxOptions{IsoLevel: pgx.RepeatableRead}
	if !apply {
		opts.AccessMode = pgx.ReadOnly
	}
	tx, err := pool.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var database string
	if err = tx.QueryRow(ctx, `SELECT current_database()`).Scan(&database); err != nil {
		return nil, err
	}
	// A loopback/Docker published port can differ from the server's internal
	// inet_server_port(). The URL port is checked before connecting; verify the
	// database identity on the connected server here.
	if database != target.Database {
		return nil, fmt.Errorf("connected PostgreSQL database does not match expected target")
	}
	var migrated bool
	if err = tx.QueryRow(ctx, `SELECT COALESCE((SELECT is_applied FROM public.goose_db_version WHERE version_id=45 ORDER BY id DESC LIMIT 1), false)`).Scan(&migrated); err != nil {
		return nil, fmt.Errorf("check migration 045: %w", err)
	}
	if !migrated {
		return nil, errors.New("migration 045 is not applied; refusing Legend attack-type backfill")
	}
	if apply {
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, armyFamilyCloseoutLockID); err != nil {
			return nil, err
		}
	}
	if _, err = tx.Exec(ctx, `SET LOCAL search_path = public`); err != nil {
		return nil, err
	}
	result := make([]LegendAttackTypeBackfillRow, 0, len(days)*3)
	for _, day := range days {
		rows, readErr := legendAttackTypeBackfillDay(ctx, tx, static, day)
		if readErr != nil {
			return nil, fmt.Errorf("Legend day %s: %w", day.Format("2006-01-02"), readErr)
		}
		result = append(result, rows...)
	}
	if !apply {
		return result, nil // deferred rollback guarantees no write on dry run
	}
	for _, row := range result {
		if !row.Changed {
			continue
		}
		command, updateErr := tx.Exec(ctx, `UPDATE public.legend_daily_stats
			SET air_attack_count=$3,ground_attack_count=$4,mixed_attack_count=$5,unknown_attack_count=$6
			WHERE day=$1::date AND cohort=$2 AND attack_count=$7
			AND air_attack_count IS NOT DISTINCT FROM $8::bigint
			AND ground_attack_count IS NOT DISTINCT FROM $9::bigint
			AND mixed_attack_count IS NOT DISTINCT FROM $10::bigint
			AND unknown_attack_count IS NOT DISTINCT FROM $11::bigint`,
			row.Day, row.Cohort, row.Air, row.Ground, row.Mixed, row.Unknown, row.StoredAttacks,
			row.Previous.Air, row.Previous.Ground, row.Previous.Mixed, row.Previous.Unknown)
		if updateErr != nil {
			return nil, updateErr
		}
		if command.RowsAffected() != 1 {
			return nil, fmt.Errorf("Legend day %s cohort %s changed before guarded update", row.Day, row.Cohort)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func validateLegendBackfillTarget(dsn string, target LegendAttackTypeBackfillTarget) error {
	if target.Host == "" || target.Database == "" || target.Port == 0 {
		return errors.New("expected host, port, and database are all required")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return errors.New("a PostgreSQL URL is required")
	}
	if parsed.Hostname() != target.Host || parsed.Port() != fmt.Sprint(target.Port) || parsed.Path != "/"+target.Database {
		return errors.New("PostgreSQL URL host, port, or database does not match the explicit target")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return errors.New("invalid PostgreSQL connection configuration")
	}
	if config.ConnConfig.Host != target.Host || config.ConnConfig.Port != target.Port || config.ConnConfig.Database != target.Database || len(config.ConnConfig.Fallbacks) != 0 {
		return errors.New("PostgreSQL URL does not match the explicit host, port, and database target")
	}
	return nil
}

func validateLegendBackfillDays(days []time.Time, now time.Time) error {
	if len(days) == 0 || len(days) > 14 {
		return errors.New("specify between 1 and 14 finalized Legend days")
	}
	latest := now.UTC().Add(-(5 * time.Hour)).Truncate(24 * time.Hour).Add(-24 * time.Hour)
	for index, day := range days {
		if !day.Equal(dayStart(day)) || day.After(latest) {
			return fmt.Errorf("Legend day %s is not a finalized UTC day", day.Format("2006-01-02"))
		}
		if index > 0 && !day.Equal(days[index-1].Add(24*time.Hour)) {
			return errors.New("Legend days must be consecutive and ascending")
		}
	}
	return nil
}

func legendAttackTypeBackfillDay(ctx context.Context, tx pgx.Tx, static *clashy.StaticData, day time.Time) ([]LegendAttackTypeBackfillRow, error) {
	var snapshot bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.legend_rankings_history WHERE day=$1::date)`, day).Scan(&snapshot); err != nil {
		return nil, err
	}
	if !snapshot {
		return nil, errors.New("ranking snapshot is missing")
	}
	rows, err := tx.Query(ctx, `SELECT cohort,attack_count,air_attack_count,ground_attack_count,mixed_attack_count,unknown_attack_count
		FROM public.legend_daily_stats WHERE day=$1::date ORDER BY cohort`, day)
	if err != nil {
		return nil, err
	}
	stored := make(map[string]LegendAttackTypeBackfillRow)
	for rows.Next() {
		var row LegendAttackTypeBackfillRow
		row.Day = day.Format("2006-01-02")
		if err = rows.Scan(&row.Cohort, &row.StoredAttacks, &row.Previous.Air, &row.Previous.Ground, &row.Previous.Mixed, &row.Previous.Unknown); err != nil {
			rows.Close()
			return nil, err
		}
		stored[row.Cohort] = row
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	for _, cohort := range legendCloseoutCohorts {
		if _, exists := stored[cohort]; !exists {
			return nil, fmt.Errorf("stored %s aggregate is missing", cohort)
		}
	}
	start, end := legendDayWindow(day)
	attacks, usage, err := readLegendAttacks(ctx, tx, start, end, stored["top_100"].Cohort != "")
	if err != nil {
		return nil, err
	}
	decoded, err := decodeArmies(static, usage)
	if err != nil {
		return nil, err
	}
	byCohort := make(map[string][]legendAttack, len(stored))
	for _, attack := range attacks {
		byCohort[attack.cohort] = append(byCohort[attack.cohort], attack)
	}
	cohorts := make([]string, 0, len(stored))
	for cohort := range stored {
		cohorts = append(cohorts, cohort)
	}
	sort.Strings(cohorts)
	result := make([]LegendAttackTypeBackfillRow, 0, len(stored))
	for _, cohort := range cohorts {
		row := stored[cohort]
		types := countLegendAttackTypes(byCohort[cohort], decoded, static.LookupByID)
		row.SourceAttacks = types.air + types.ground + types.mixed + types.unknown
		if row.SourceAttacks != row.StoredAttacks {
			return nil, fmt.Errorf("%s source attacks %d do not match stored attacks %d", cohort, row.SourceAttacks, row.StoredAttacks)
		}
		row.Air, row.Ground, row.Mixed, row.Unknown = types.air, types.ground, types.mixed, types.unknown
		row.Changed = !sameLegendAttackTypes(row.Previous, types)
		result = append(result, row)
	}
	return result, nil
}

func sameLegendAttackTypes(previous LegendAttackTypePrevious, current legendAttackTypes) bool {
	prior := [4]*int64{previous.Air, previous.Ground, previous.Mixed, previous.Unknown}
	values := [4]int64{current.air, current.ground, current.mixed, current.unknown}
	for index, value := range prior {
		if value == nil || *value != values[index] {
			return false
		}
	}
	return true
}
