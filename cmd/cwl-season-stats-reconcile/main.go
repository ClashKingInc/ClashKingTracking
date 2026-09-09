package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"clashking_tracking/internal/cwlstats"
	"clashking_tracking/internal/platform"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var scope string
	flag.StringVar(&scope, "scope", "all", "reconciliation scope: all or current_previous")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg := platform.LoadWithArgs([]string{"--script", "scheduled"})
	if cfg.TimescaleURL == "" {
		return fmt.Errorf("TIMESCALE_* connection variables are required")
	}
	pool, err := pgxpool.New(ctx, cfg.TimescaleURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	seasons, err := selectedSeasons(scope)
	if err != nil {
		return err
	}
	if err := cwlstats.Reconcile(ctx, pool, seasons); err != nil {
		return err
	}
	fmt.Printf("reconciled CWL season statistics for %s scope\n", scope)
	return nil
}

func selectedSeasons(scope string) ([]string, error) {
	switch scope {
	case "all":
		return nil, nil
	case "current_previous":
		return cwlstats.CurrentAndPreviousUTC(timeNow()), nil
	default:
		return nil, fmt.Errorf("unsupported --scope %q; use all or current_previous", scope)
	}
}

var timeNow = func() time.Time { return time.Now() }
