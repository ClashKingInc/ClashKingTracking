package platform

import (
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Limits apply to each pool, not to a process or the whole database. Composite
// scripts create multiple operational pools; stats and LISTEN are additional.
type DatabasePoolConfig struct {
	DefaultMaxConns       int            `json:"default_max_conns"`
	MaxConnsByScript      map[string]int `json:"max_conns_by_script"`
	StatsMaxConns         int            `json:"stats_max_conns"`
	MinConns              int            `json:"min_conns"`
	MaxIdleTime           string         `json:"max_idle_time"`
	MaxLifetime           string         `json:"max_lifetime"`
	MaxLifetimeJitter     string         `json:"max_lifetime_jitter"`
	ConnectTimeoutSeconds int            `json:"connect_timeout_seconds"`
}

func (p DatabasePoolConfig) connectionString(dsn, script string, stats bool) (string, error) {
	max := p.DefaultMaxConns
	if max == 0 {
		max = 2
	}
	if value, ok := p.MaxConnsByScript[script]; ok {
		max = value
	}
	if stats {
		max = p.StatsMaxConns
		if max == 0 {
			max = 1
		}
	}
	if max < 1 || p.MinConns < 0 || p.MinConns > max {
		return "", errors.New("invalid database pool connection limits")
	}
	idle, lifetime, jitter := p.MaxIdleTime, p.MaxLifetime, p.MaxLifetimeJitter
	if idle == "" {
		idle = "5m"
	}
	if lifetime == "" {
		lifetime = "30m"
	}
	if jitter == "" {
		jitter = "5m"
	}
	timeout := p.ConnectTimeoutSeconds
	if timeout == 0 {
		timeout = 10
	}
	if timeout < 1 {
		return "", errors.New("invalid database connect timeout")
	}
	values := map[string]string{
		"pool_max_conns": strconv.Itoa(max), "pool_min_conns": strconv.Itoa(p.MinConns),
		"pool_max_conn_idle_time": idle, "pool_max_conn_lifetime": lifetime,
		"pool_max_conn_lifetime_jitter": jitter, "connect_timeout": strconv.Itoa(timeout),
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", errors.New("invalid database connection URL")
		}
		q := u.Query()
		for key, value := range values {
			q.Set(key, value)
		}
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		for key, value := range values {
			dsn += " " + key + "='" + strings.ReplaceAll(strings.ReplaceAll(value, "\\", "\\\\"), "'", "\\'") + "'"
		}
	}
	parsed, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return "", errors.New("invalid database pool configuration")
	}
	if parsed.MaxConnIdleTime <= 0 || parsed.MaxConnLifetime <= 0 || parsed.MaxConnLifetimeJitter < 0 {
		return "", errors.New("invalid database pool durations")
	}
	return dsn, nil
}
