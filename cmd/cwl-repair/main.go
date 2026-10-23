// cwl-repair defaults to a database-enforced read-only inventory.
// This one-time repair deliberately does not infer completion from saved wars.
// Apply is restricted to loopback development databases.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"clashking_tracking/scripts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("CWL_REPAIR_DATABASE_URL"), "database URL (or CWL_REPAIR_DATABASE_URL)")
	apply := flag.Bool("apply-local", false, "mark every non-ended group ended, without war checks; loopback databases only")
	flag.Parse()
	if err := run(*dsn, *apply); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(dsn string, apply bool) error {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("invalid database connection configuration")
	}
	if dsn == "" {
		return fmt.Errorf("an explicit database URL is required")
	}
	if apply && config.ConnConfig.Host != "127.0.0.1" && config.ConnConfig.Host != "localhost" && config.ConnConfig.Host != "::1" {
		return fmt.Errorf("--apply-local only permits a loopback database")
	}
	for _, fallback := range config.ConnConfig.Fallbacks {
		if apply && fallback.Host != "127.0.0.1" && fallback.Host != "localhost" && fallback.Host != "::1" {
			return fmt.Errorf("--apply-local only permits loopback fallback databases")
		}
	}
	config.MaxConns = 1
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close()
	ids, err := scripts.RepairCWLStates(ctx, pool, apply)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(struct {
		Applied  bool     `json:"applied"`
		Count    int      `json:"count"`
		GroupIDs []string `json:"groupIds"`
	}{apply, len(ids), ids})
}
