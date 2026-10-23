package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"clashking_tracking/scripts"
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
	daysFlag := flag.String("days", "", "comma-separated completed UTC Legend days")
	flag.Parse()
	dsn := timescaleURLFromEnvironment()
	if err := requireLocalDevDatabase(dsn); err != nil {
		return err
	}
	days, err := parseDays(*daysFlag)
	if err != nil {
		return err
	}
	if err := requireCompletedDays(days, time.Now().UTC()); err != nil {
		return err
	}
	writes, err := scripts.RebuildDailyArmySetups(context.Background(), dsn, days)
	if err != nil {
		return err
	}
	fmt.Printf("rebuilt setup observations for %d Legend days with %d rows\n", len(days), writes)
	return nil
}

func timescaleURLFromEnvironment() string {
	host := strings.TrimSpace(os.Getenv("TIMESCALE_HOST"))
	if host == "" {
		return strings.TrimSpace(os.Getenv("TIMESCALE_URL"))
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

func requireCompletedDays(days []time.Time, now time.Time) error {
	latest := now.UTC().Add(-(5*time.Hour + 10*time.Minute)).Truncate(24 * time.Hour).Add(-24 * time.Hour)
	for _, day := range days {
		if day.After(latest) {
			return fmt.Errorf("day %s is not complete; latest completed Legend day is %s", day.Format("2006-01-02"), latest.Format("2006-01-02"))
		}
	}
	return nil
}
