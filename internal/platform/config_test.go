//go:build platform_internal_tests

package platform

import (
	"os"
	"testing"
)

func TestLoadWithArgsReadsConfigJSON(t *testing.T) {
	clearConfigEnv(t)
	writeConfig(t, `{
		"grpc_addr": ":9191",
		"proxy_url": "http://proxy-json",
		"valkey_addr": "valkey-json:6379",
		"target_page_multiplier": 9,
		"r2": {
			"mock_upload": true,
			"requests_per_second": 123
		},
		"stats": {
			"timescale_flush_seconds": 22
		},
		"events": {
			"stream": "tracking:events",
			"group": "events-group",
			"consumer": "events-1",
			"retention_seconds": 300,
			"batch_size": 50,
			"reclaim_idle_seconds": 30
		},
		"globalclans": {
			"priority_requests_per_second": 123,
			"non_priority_requests_per_second": 45
		},
		"battlelogs": {
			"requests_per_second": 11,
			"priority_requests_per_second": 3,
			"checkpoint_ttl_days": 34,
			"first_seen_lookback_days": 56
		},
		"wars": {
			"requests_per_second": 99,
			"cwl_sync_seconds": 22
		},
		"botclans": {
			"requests_per_second": 77,
			"target_refresh_seconds": 3800,
			"snapshot_prefix": "botclans:test:",
			"cwl_state_snapshot": "test-cwlstate"
		},
		"botplayers": {
			"requests_per_second": 88
		},
		"basicplayers": {
			"requests_per_second": 30
		},
		"leaderboards": {
			"requests_per_second": 66,
			"interval_seconds": 600,
			"limit": 500,
			"null_asset_url": "https://assets/null"
		},
		"scheduled": {
			"interval_seconds": 900
		},
		"giveaways": {
			"scan_seconds": 60
		},
		"reddit": {
			"poll_seconds": 120
		}
	}`)

	cfg := LoadWithArgs(nil)

	if cfg.Script != "" {
		t.Fatalf("script = %q, want empty because config JSON cannot select scripts", cfg.Script)
	}
	if cfg.Enabled("wars") {
		t.Fatalf("script should not be enabled without --script")
	}
	if cfg.ProxyURL != "http://proxy-json" || cfg.ValkeyAddr != "valkey-json:6379" {
		t.Fatalf("json endpoint config was not applied: %+v", cfg)
	}
	if !cfg.R2MockUpload {
		t.Fatalf("r2 mock upload config was not applied: %+v", cfg)
	}
	if cfg.R2RequestsPerSecond != 123 || cfg.StatsTimescaleFlushSeconds != 22 {
		t.Fatalf("runtime stats/r2 config was not applied: %+v", cfg)
	}
	if cfg.GlobalClanPriorityRequestsPerSecond != 123 || cfg.GlobalClanNonPriorityRequestsPerSecond != 45 ||
		cfg.TargetPageMultiplier != 9 {
		t.Fatalf("globalclans config was not applied: %+v", cfg)
	}
	if cfg.BattlelogRequestsPerSecond != 11 || cfg.BattlelogPriorityRequestsPerSecond != 3 ||
		cfg.BattlelogCheckpointTTLDays != 34 || cfg.BattlelogFirstSeenLookbackDays != 56 {
		t.Fatalf("battlelogs config was not applied: %+v", cfg)
	}
	if cfg.WarCWLSyncSeconds != 22 {
		t.Fatalf("wars config was not applied: %+v", cfg)
	}
	if cfg.BotClanRequestsPerSecond != 77 || cfg.BotClanTargetRefreshSeconds != 3800 ||
		cfg.BotClanSnapshotPrefix != "botclans:test:" ||
		cfg.BotClanCWLStateSnapshot != "test-cwlstate" {
		t.Fatalf("botclans config was not applied: %+v", cfg)
	}
	if cfg.EventStreamName != "tracking:events" || cfg.EventStreamBatchSize != 50 ||
		cfg.EventStreamRetentionSeconds != 300 || cfg.EventStreamReclaimIdleSeconds != 30 {
		t.Fatalf("events config was not applied: %+v", cfg)
	}
	if cfg.BotPlayerRequestsPerSecond != 88 || cfg.BasicPlayerRequestsPerSecond != 30 ||
		cfg.LeaderboardRequestsPerSecond != 66 ||
		cfg.LeaderboardIntervalSeconds != 600 || cfg.ScheduledIntervalSeconds != 900 ||
		cfg.LeaderboardLimit != 500 || cfg.LeaderboardNullAssetURL != "https://assets/null" ||
		cfg.GiveawayScanSeconds != 60 || cfg.RedditPollSeconds != 120 {
		t.Fatalf("script config was not applied: %+v", cfg)
	}
}

func TestLoadWithArgsOnlyScriptComesFromCLI(t *testing.T) {
	clearConfigEnv(t)
	writeConfig(t, `{
		"grpc_addr": ":9191",
		"proxy_url": "http://proxy-json",
		"dry_run": true,
		"mock_db": true,
		"target_page_multiplier": 9,
		"globalclans": {
			"priority_requests_per_second": 77,
			"non_priority_requests_per_second": 8
		},
		"battlelogs": {
			"requests_per_second": 4,
			"checkpoint_ttl_days": 15,
			"first_seen_lookback_days": 14
		},
		"wars": {
			"requests_per_second": 50,
			"cwl_sync_seconds": 10
		}
	}`)
	t.Setenv("TARGET_PAGE_MULTIPLIER", "3")
	t.Setenv("GLOBAL_CLAN_PRIORITY_REQUESTS_PER_SECOND", "1")
	t.Setenv("DRY_RUN", "false")
	t.Setenv("PROXY_URL", "http://proxy-env")

	cfg := LoadWithArgs([]string{"--script", "globalclans"})

	if cfg.Script != "globalclans" {
		t.Fatalf("script = %q, want globalclans", cfg.Script)
	}
	if cfg.GlobalClanPriorityRequestsPerSecond != 77 || cfg.TargetPageMultiplier != 9 ||
		cfg.ProxyURL != "http://proxy-json" || !cfg.DryRun {
		t.Fatalf("env should not override config knobs: %+v", cfg)
	}
	if cfg.WarMaxInFlight != RequestConcurrency(cfg.WarRequestsPerSecond) {
		t.Fatalf("war max in-flight = %d, want concurrency %d", cfg.WarMaxInFlight, RequestConcurrency(cfg.WarRequestsPerSecond))
	}
}

func TestLoadWithArgsReadsSecretsFromEnv(t *testing.T) {
	clearConfigEnv(t)
	writeConfig(t, `{
		"grpc_addr": ":9191",
		"proxy_url": "http://proxy-json",
		"target_page_multiplier": 9,
		"globalclans": {
			"priority_requests_per_second": 77,
			"non_priority_requests_per_second": 8
		},
		"battlelogs": {
			"requests_per_second": 4,
			"checkpoint_ttl_days": 15,
			"first_seen_lookback_days": 14
		},
		"wars": {
			"requests_per_second": 50,
			"cwl_sync_seconds": 10
		}
	}`)
	t.Setenv("TIMESCALE_URL", "postgres://secret")
	t.Setenv("R2_ENDPOINT", "https://r2.example")
	t.Setenv("R2_BUCKET", "war-json")
	t.Setenv("R2_SECRET_ACCESS_KEY", "secret")

	cfg := LoadWithArgs([]string{"--script", "wars"})

	if cfg.TimescaleURL != "postgres://secret" || cfg.R2Endpoint != "https://r2.example" ||
		cfg.R2Bucket != "war-json" || cfg.R2SecretAccessKey != "secret" {
		t.Fatalf("secret env was not applied: %+v", cfg)
	}
}

func TestEnabledUsesOnlySelectedScript(t *testing.T) {
	cfg := Config{Script: "globalclans"}
	if !cfg.Enabled("globalclans") || cfg.Enabled("wars") {
		t.Fatalf("only the selected script should be enabled")
	}
}

func writeConfig(t *testing.T, body string) {
	t.Helper()
	t.Chdir(t.TempDir())
	if err := os.WriteFile("config.json", []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	keys := []string{
		"GRPC_ADDR",
		"RUN_ONCE",
		"DRY_RUN",
		"MOCK_DB",
		"TARGET_PAGE_MULTIPLIER",
		"GLOBAL_CLAN_PRIORITY_REQUESTS_PER_SECOND",
		"GLOBAL_CLAN_NON_PRIORITY_REQUESTS_PER_SECOND",
		"GLOBAL_CLAN_REQUESTS_PER_SECOND",
		"GLOBAL_CLAN_ACTIVE_REQUESTS_PER_SECOND",
		"BATTLELOG_REQUESTS_PER_SECOND",
		"BATTLELOG_ROLLUP_FLUSH_ATTACKS",
		"BATTLELOG_CHECKPOINT_TTL_DAYS",
		"BATTLELOG_FIRST_SEEN_LOOKBACK_DAYS",
		"WAR_REQUESTS_PER_SECOND",
		"WAR_CWL_SYNC_SECONDS",
		"EVENT_BUFFER_SIZE",
		"RECENT_EVENT_BUFFER",
		"PROXY_URL",
		"TIMESCALE_URL",
		"VALKEY_ADDR",
		"VALKEY_PASSWORD",
		"R2_ENDPOINT",
		"R2_ACCESS_KEY_ID",
		"R2_SECRET_ACCESS_KEY",
		"R2_BUCKET",
		"REDDIT_CLIENT_ID",
		"REDDIT_CLIENT_SECRET",
		"REDDIT_USERNAME",
		"REDDIT_PASSWORD",
	}
	for _, key := range keys {
		previous, ok := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Cleanup(func() {
				_ = os.Setenv(key, previous)
			})
		} else {
			t.Cleanup(func() {
				_ = os.Unsetenv(key)
			})
		}
	}
}
