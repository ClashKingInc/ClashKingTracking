package platform

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDatabasePoolConfiguration(t *testing.T) {
	for _, dsn := range []string{
		"postgres://test:secret@localhost/test?sslmode=disable&pool_max_conns=99",
		"host=localhost user=test password='secret' dbname=test pool_max_conns=99",
	} {
		for _, stats := range []bool{false, true} {
			p := DatabasePoolConfig{MaxConnsByScript: map[string]int{"scheduled": 1}}
			configured, err := p.connectionString(dsn, "scheduled", stats)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := pgxpool.ParseConfig(configured)
			if err != nil {
				t.Fatal("could not parse configured DSN")
			}
			if cfg.MaxConns != 1 || cfg.MinConns != 0 || cfg.MaxConnIdleTime != 5*time.Minute || cfg.MaxConnLifetime != 30*time.Minute || cfg.MaxConnLifetimeJitter != 5*time.Minute || cfg.ConnConfig.ConnectTimeout != 10*time.Second {
				t.Fatal("configured limits were not applied")
			}
			if cfg.ConnConfig.Password != "secret" {
				t.Fatal("credentials changed")
			}
			for key := range cfg.ConnConfig.RuntimeParams {
				if key == "pool_max_conns" {
					t.Fatal("pool option leaked to direct listener")
				}
			}
		}
	}
}

func TestDatabasePoolRejectsInvalidSettings(t *testing.T) {
	for _, cfg := range []DatabasePoolConfig{
		{DefaultMaxConns: -1}, {MinConns: 3}, {MaxIdleTime: "bad"},
		{MaxLifetime: "-1m"}, {ConnectTimeoutSeconds: -1},
		{MaxConnsByScript: map[string]int{"scheduled": 0}},
	} {
		if _, err := cfg.connectionString("postgres://test:secret@localhost/test", "scheduled", false); err == nil {
			t.Fatal("invalid settings accepted")
		}
	}
}

func TestStatsPoolDoesNotInheritOperationalMaximum(t *testing.T) {
	settings := DatabasePoolConfig{MaxConnsByScript: map[string]int{"globalclans": 6}, StatsMaxConns: 1}
	for _, stats := range []bool{false, true} {
		dsn, err := settings.connectionString("postgres://test@localhost/test", "globalclans", stats)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal("parse configured connection")
		}
		want := int32(6)
		if stats {
			want = 1
		}
		if cfg.MaxConns != want {
			t.Fatalf("stats=%v: got %d, want %d", stats, cfg.MaxConns, want)
		}
	}
}

func TestRepositoryDatabasePoolSettings(t *testing.T) {
	cfg, err := loadConfigFile("../../config.json")
	if err != nil {
		t.Fatal(err)
	}
	for script, expected := range map[string]int{"globalclans": 6, "battlelogs": 6, "scheduled": 1, "notifications": 1, "trackedplayers": 2} {
		dsn, err := cfg.DatabasePools.connectionString("postgres://test@localhost/test", script, false)
		if err != nil {
			t.Fatal(err)
		}
		parsed, err := pgxpool.ParseConfig(dsn)
		if err != nil || int(parsed.MaxConns) != expected {
			t.Fatalf("incorrect limit for %s", script)
		}
	}
}
