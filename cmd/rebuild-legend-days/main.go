package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"clashking_tracking/scripts"
	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	_ = godotenv.Load()
	daysFlag := flag.String("days", "", "comma-separated completed UTC day labels (maximum seven)")
	from := flag.String("from", "", "first completed UTC day, inclusive")
	through := flag.String("through", "", "last completed UTC day, inclusive (maximum seven per invocation)")
	target := flag.String("target", "local", "local or production")
	expectedHost := flag.String("expect-host", "", "required exact DSN hostname for production")
	expectedSystem := flag.String("expect-system-id", "", "required PostgreSQL system identifier for production")
	apply := flag.Bool("apply", false, "apply derived-data rebuilds; default previews source availability")
	observedOnly := flag.Bool("observed-only", false, "rebuild observed daily statistics without rank snapshots")
	performanceOnly := flag.Bool("performance-only", false, "rebuild only rank-snapshot performance")
	timeout := flag.Duration("day-timeout", 10*time.Minute, "maximum processing time per day (up to one hour)")
	flag.Parse()
	if *observedOnly && *performanceOnly {
		return fmt.Errorf("choose only one partial rebuild mode")
	}
	if *timeout <= 0 || *timeout > time.Hour {
		return fmt.Errorf("day-timeout must be positive and at most one hour")
	}
	dsn := timescaleURLFromEnvironment()
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Scheme != "postgres" && parsed.Scheme != "postgresql") {
		return fmt.Errorf("a PostgreSQL URL is required")
	}
	for key := range parsed.Query() {
		if key != "sslmode" {
			return fmt.Errorf("refusing PostgreSQL URL query override %q", key)
		}
	}
	switch *target {
	case "local":
		if err := requireLocalDevDatabase(dsn); err != nil {
			return err
		}
	case "production":
		if *expectedHost == "" || parsed.Hostname() != *expectedHost || parsed.Path != "/clashking" || *expectedSystem == "" {
			return fmt.Errorf("production requires --expect-host matching the DSN, database clashking, and --expect-system-id")
		}
	default:
		return fmt.Errorf("target must be local or production")
	}
	labels := *daysFlag
	if *from != "" || *through != "" {
		if labels != "" || *from == "" || *through == "" {
			return fmt.Errorf("use --days or both --from and --through")
		}
		start, err := time.Parse("2006-01-02", *from)
		if err != nil {
			return err
		}
		end, err := time.Parse("2006-01-02", *through)
		if err != nil {
			return err
		}
		if end.Before(start) || end.Sub(start) > 6*24*time.Hour {
			return fmt.Errorf("date range must contain one to seven days")
		}
		var parts []string
		for day := start; !day.After(end); day = day.AddDate(0, 0, 1) {
			parts = append(parts, day.Format("2006-01-02"))
		}
		labels = strings.Join(parts, ",")
	}
	days, err := parseDays(labels)
	if err != nil {
		return err
	}
	if len(days) > 7 {
		return fmt.Errorf("at most seven days may be processed per invocation")
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })
	if err = requireCompletedDays(days, time.Now().UTC()); err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	inspectCtx, inspectCancel := context.WithTimeout(ctx, 30*time.Second)
	conn, err := pgx.Connect(inspectCtx, dsn)
	if err != nil {
		inspectCancel()
		return fmt.Errorf("connect to selected Timescale: %w", err)
	}
	defer conn.Close(context.Background())
	var database string
	err = conn.QueryRow(inspectCtx, "SELECT current_database()").Scan(&database)
	if err == nil && database != strings.TrimPrefix(parsed.Path, "/") {
		err = fmt.Errorf("connected database differs from selected target")
	}
	if err == nil && *target == "production" {
		var system string
		err = conn.QueryRow(inspectCtx, "SELECT system_identifier::text FROM pg_control_system()").Scan(&system)
		if err == nil && system != *expectedSystem {
			err = fmt.Errorf("PostgreSQL system identifier does not match the approved target")
		}
	}
	if err == nil {
		_, err = conn.Exec(inspectCtx, "SET default_transaction_read_only=on; SET statement_timeout='30s'")
	}
	inspectCancel()
	if err != nil {
		return err
	}
	fmt.Printf("target=%s host=%s database=%s apply=%t days=%d\n", *target, parsed.Hostname(), database, *apply, len(days))
	for _, day := range days {
		start, end := day.Add(5*time.Hour), day.Add(29*time.Hour)
		previewCtx, previewCancel := context.WithTimeout(ctx, 30*time.Second)
		var attacks, defenses, ranks int64
		err = conn.QueryRow(previewCtx, `SELECT count(*) FILTER(WHERE direction=1),count(*) FILTER(WHERE direction=2),
   (SELECT count(*) FROM legend_rankings_history WHERE day=$1)
   FROM battles_ranked WHERE battle_mode=2 AND battle_time >= $2 AND battle_time < $3`, day, start, end).Scan(&attacks, &defenses, &ranks)
		previewCancel()
		if err != nil {
			return fmt.Errorf("preview %s: %w", day.Format("2006-01-02"), err)
		}
		fmt.Printf("day=%s raw_attacks=%d raw_defenses=%d rank_snapshots=%d\n", day.Format("2006-01-02"), attacks, defenses, ranks)
	}
	if !*apply {
		fmt.Println("Preview only; no derived data changed. Repeat with --apply to rebuild these exact days.")
		return nil
	}
	// Target/date checks finish before any writes. The rebuilders preserve raw
	// battles and use transactional derived-data updates; interruption is retryable.
	// Ranked subsets remain absent on days without historical rankings.
	for _, day := range days {
		dayStarted := time.Now()
		dayCtx, dayCancel := context.WithTimeout(ctx, *timeout)
		var writes int
		if *observedOnly {
			writes, err = scripts.RebuildObservedLegendDays(dayCtx, dsn, []time.Time{day})
		} else if *performanceOnly {
			writes, err = scripts.RebuildLegendDayPerformance(dayCtx, dsn, []time.Time{day})
		} else {
			writes, err = scripts.RebuildLegendCloseouts(dayCtx, dsn, []time.Time{day})
		}
		dayCancel()
		if err != nil {
			return fmt.Errorf("rebuild %s: %w", day.Format("2006-01-02"), err)
		}
		fmt.Printf("rebuilt day=%s derived_writes=%d\n", day.Format("2006-01-02"), writes)
		fmt.Printf("rebuild_timing day=%s elapsed_ms=%d\n", day.Format("2006-01-02"), time.Since(dayStarted).Milliseconds())
	}
	return nil
}

func requireCompletedDays(days []time.Time, now time.Time) error {
	latest := now.UTC().Add(-(5 * time.Hour)).Truncate(24 * time.Hour).Add(-24 * time.Hour)
	for _, day := range days {
		if day.After(latest) {
			return fmt.Errorf("day %s is not complete; latest completed Legend day is %s", day.Format("2006-01-02"), latest.Format("2006-01-02"))
		}
	}
	return nil
}

func timescaleURLFromEnvironment() string {
	host := strings.TrimSpace(os.Getenv("TIMESCALE_HOST"))
	if host == "" {
		dsn := strings.TrimSpace(os.Getenv("TIMESCALE_URL"))
		return dsn
	}
	parsed := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(os.Getenv("TIMESCALE_USERNAME"), os.Getenv("TIMESCALE_PASSWORD")),
		Host:   net.JoinHostPort(host, strings.TrimSpace(os.Getenv("TIMESCALE_PORT"))),
		Path:   "/" + strings.TrimSpace(os.Getenv("TIMESCALE_DATABASE")),
	}
	query := url.Values{}
	query.Set("sslmode", strings.TrimSpace(os.Getenv("TIMESCALE_SSLMODE")))
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func requireLocalDevDatabase(dsn string) error {
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Scheme != "postgres" {
		return fmt.Errorf("TIMESCALE_URL must be a PostgreSQL URL")
	}
	host := parsed.Hostname()
	if host != "localhost" && net.ParseIP(host) == nil {
		return fmt.Errorf("refusing non-local Timescale host %q", host)
	}
	if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() {
		return fmt.Errorf("refusing non-loopback Timescale host %q", host)
	}
	if os.Getenv("CLASHKING_DISPOSABLE_TIMESCALE") == "1" && parsed.Path == "/clashking_test" {
		return nil
	}
	if parsed.Port() != "54330" {
		return fmt.Errorf("refusing Timescale port %q; the disposable local stack uses 54330", parsed.Port())
	}
	if parsed.EscapedPath() != "/clashking_dev" {
		return fmt.Errorf("refusing Timescale database %q; the disposable local database is clashking_dev", strings.TrimPrefix(parsed.Path, "/"))
	}
	for key := range parsed.Query() {
		if key != "sslmode" {
			return fmt.Errorf("refusing PostgreSQL URL query override %q", key)
		}
	}
	return nil
}

func parseDays(value string) ([]time.Time, error) {
	parts := strings.Split(strings.TrimSpace(value), ",")
	if len(parts) == 0 || parts[0] == "" {
		return nil, fmt.Errorf("--days is required")
	}
	days := make([]time.Time, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		label := strings.TrimSpace(part)
		day, err := time.Parse("2006-01-02", label)
		if err != nil {
			return nil, fmt.Errorf("invalid day %q: %w", label, err)
		}
		if seen[label] {
			return nil, fmt.Errorf("duplicate day %q", label)
		}
		seen[label] = true
		days = append(days, day)
	}
	return days, nil
}
