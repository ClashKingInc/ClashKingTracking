package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
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
	start := flag.String("start", "", "first finalized UTC Legend day (YYYY-MM-DD)")
	end := flag.String("end", "", "last finalized UTC Legend day (YYYY-MM-DD), inclusive")
	expectedHost := flag.String("expected-host", "", "exact expected PostgreSQL host from the connection URL")
	expectedPort := flag.Uint("expected-port", 0, "exact expected PostgreSQL port")
	expectedDatabase := flag.String("expected-database", "", "exact expected PostgreSQL database")
	apply := flag.Bool("apply", false, "write only the four Legend attack-type columns after complete validation")
	confirmDatabase := flag.String("confirm-database", "", "required with --apply; must repeat the expected database name")
	flag.Parse()
	if *expectedPort > 65535 {
		return errors.New("expected port is outside the TCP port range")
	}
	if *apply && (*confirmDatabase == "" || *confirmDatabase != *expectedDatabase) {
		return errors.New("--apply requires --confirm-database matching --expected-database")
	}
	days, err := parseDayRange(*start, *end)
	if err != nil {
		return err
	}
	dsn := timescaleURLFromEnvironment()
	if dsn == "" {
		return errors.New("Timescale connection configuration is missing")
	}
	rows, err := scripts.BackfillLegendAttackTypes(context.Background(), dsn, scripts.LegendAttackTypeBackfillTarget{
		Host: *expectedHost, Port: uint16(*expectedPort), Database: *expectedDatabase,
	}, days, *apply)
	if err != nil {
		return err
	}
	changed := 0
	for _, row := range rows {
		if row.Changed {
			changed++
		}
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		DryRun      bool                                  `json:"dryRun"`
		Days        int                                   `json:"days"`
		Rows        int                                   `json:"rows"`
		ChangedRows int                                   `json:"changedRows"`
		Breakdown   []scripts.LegendAttackTypeBackfillRow `json:"breakdown"`
	}{DryRun: !*apply, Days: len(days), Rows: len(rows), ChangedRows: changed, Breakdown: rows})
}

func parseDayRange(first, last string) ([]time.Time, error) {
	start, err := time.Parse("2006-01-02", first)
	if err != nil || start.Format("2006-01-02") != first {
		return nil, errors.New("--start must be an exact YYYY-MM-DD date")
	}
	end, err := time.Parse("2006-01-02", last)
	if err != nil || end.Format("2006-01-02") != last || end.Before(start) {
		return nil, errors.New("--end must be an exact YYYY-MM-DD date on or after --start")
	}
	if end.Sub(start) >= 14*24*time.Hour {
		return nil, errors.New("backfill range is limited to 14 consecutive days")
	}
	days := make([]time.Time, 0, int(end.Sub(start)/(24*time.Hour))+1)
	for day := start; !day.After(end); day = day.Add(24 * time.Hour) {
		days = append(days, day)
	}
	return days, nil
}

func timescaleURLFromEnvironment() string {
	host := strings.TrimSpace(os.Getenv("TIMESCALE_HOST"))
	if host == "" {
		return strings.TrimSpace(os.Getenv("TIMESCALE_URL"))
	}
	port := strings.TrimSpace(os.Getenv("TIMESCALE_PORT"))
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return ""
	}
	value := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(os.Getenv("TIMESCALE_USERNAME"), os.Getenv("TIMESCALE_PASSWORD")),
		Host:   net.JoinHostPort(host, port),
		Path:   "/" + strings.TrimSpace(os.Getenv("TIMESCALE_DATABASE")),
	}
	query := url.Values{}
	query.Set("sslmode", strings.TrimSpace(os.Getenv("TIMESCALE_SSLMODE")))
	value.RawQuery = query.Encode()
	return value.String()
}
