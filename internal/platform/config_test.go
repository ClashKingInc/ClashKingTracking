//go:build platform_internal_tests

package platform

import (
	"os"
	"testing"
)

func TestLoadWithArgsReadsConfigJSON(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CLASHKING_PROXY_INTERNAL_ORIGIN", "http://proxy-env")
	t.Setenv("VALKEY_HOST", "valkey-env")
	t.Setenv("VALKEY_PORT", "6380")
	writeConfig(t, `{
		"grpc_addr": ":9191",
		"target_page_multiplier": 9,
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
		"trackedclans": {
			"requests_per_second": 77,
			"target_refresh_seconds": 3800,
			"snapshot_prefix": "trackedclans:test:",
			"cwl_state_snapshot": "test-cwlstate"
		},
		"trackedplayers": {
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
		},
		"roster_automations": {
			"scan_seconds": 17,
			"batch_size": 42
		}
	}`)

	cfg := LoadWithArgs(nil)

	if cfg.Script != "" {
		t.Fatalf("script = %q, want empty because config JSON cannot select scripts", cfg.Script)
	}
	if cfg.Enabled("wars") {
		t.Fatalf("script should not be enabled without --script")
	}
	if cfg.ProxyURL != "http://proxy-env/v1" || cfg.ValkeyAddr != "valkey-env:6380" {
		t.Fatalf("environment endpoint config was not applied: %+v", cfg)
	}
	if cfg.StatsTimescaleFlushSeconds != 22 {
		t.Fatalf("runtime stats config was not applied: %+v", cfg)
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
	if cfg.TrackedClanRequestsPerSecond != 77 || cfg.TrackedClanTargetRefreshSeconds != 3800 ||
		cfg.TrackedClanSnapshotPrefix != "trackedclans:test:" ||
		cfg.TrackedClanCWLStateSnapshot != "test-cwlstate" {
		t.Fatalf("trackedclans config was not applied: %+v", cfg)
	}
	if cfg.EventStreamName != "tracking:events" || cfg.EventStreamBatchSize != 50 ||
		cfg.EventStreamRetentionSeconds != 300 || cfg.EventStreamReclaimIdleSeconds != 30 {
		t.Fatalf("events config was not applied: %+v", cfg)
	}
	if cfg.TrackedPlayerRequestsPerSecond != 88 || cfg.BasicPlayerRequestsPerSecond != 30 ||
		cfg.LeaderboardRequestsPerSecond != 66 ||
		cfg.LeaderboardIntervalSeconds != 600 || cfg.ScheduledIntervalSeconds != 900 ||
		cfg.LeaderboardLimit != 500 || cfg.LeaderboardNullAssetURL != "https://assets/null" ||
		cfg.GiveawayScanSeconds != 60 || cfg.RedditPollSeconds != 120 ||
		cfg.RosterAutomationScanSeconds != 17 || cfg.RosterAutomationBatchSize != 42 {
		t.Fatalf("script config was not applied: %+v", cfg)
	}
}

func TestLoadWithArgsOnlyScriptComesFromCLI(t *testing.T) {
	clearConfigEnv(t)
	writeConfig(t, `{
		"grpc_addr": ":9191",
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
	t.Setenv("CLASHKING_PROXY_INTERNAL_ORIGIN", "http://canonical-proxy")

	cfg := LoadWithArgs([]string{"--script", "globalclans"})

	if cfg.Script != "globalclans" {
		t.Fatalf("script = %q, want globalclans", cfg.Script)
	}
	if cfg.GlobalClanPriorityRequestsPerSecond != 77 || cfg.TargetPageMultiplier != 9 ||
		cfg.ProxyURL != "http://canonical-proxy/v1" || !cfg.DryRun {
		t.Fatalf("operational env knobs or canonical connectivity were not handled correctly: %+v", cfg)
	}
	if cfg.WarMaxInFlight != RequestConcurrency(cfg.WarRequestsPerSecond) {
		t.Fatalf("war max in-flight = %d, want concurrency %d", cfg.WarMaxInFlight, RequestConcurrency(cfg.WarRequestsPerSecond))
	}
}

func TestLoadWithArgsReadsSecretsFromEnv(t *testing.T) {
	clearConfigEnv(t)
	writeConfig(t, `{
		"grpc_addr": ":9191",
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
	t.Setenv("TIMESCALE_HOST", "timescale")
	t.Setenv("TIMESCALE_PORT", "5432")
	t.Setenv("TIMESCALE_USERNAME", "tracking")
	t.Setenv("TIMESCALE_PASSWORD", "p@ss/word")
	t.Setenv("TIMESCALE_DATABASE", "tracking data")
	t.Setenv("TIMESCALE_SSLMODE", "require")
	cfg := LoadWithArgs([]string{"--script", "wars"})

	if cfg.TimescaleURL != "postgres://tracking:p%40ss%2Fword@timescale:5432/tracking%20data?sslmode=require" {
		t.Fatalf("secret env was not applied: %+v", cfg)
	}
}

func TestLoadWithArgsDoesNotAcceptLegacyConnectivityVariables(t *testing.T) {
	clearConfigEnv(t)
	writeConfig(t, `{}`)
	t.Setenv("TIMESCALE_URL", "postgres://legacy")
	t.Setenv("DATABASE_URL", "postgres://legacy")
	t.Setenv("PROXY_URL", "http://legacy-proxy")
	t.Setenv("VALKEY_ADDR", "legacy-valkey:6379")
	t.Setenv("ENCRYPTION_KEY", "legacy-key")

	cfg := LoadWithArgs(nil)
	if cfg.TimescaleURL != "" || cfg.ProxyURL != "" || cfg.ValkeyAddr != "" || cfg.MobilePushTokenKey != "" {
		t.Fatalf("legacy connectivity variables were accepted: %+v", cfg)
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
		"DATABASE_URL",
		"VALKEY_ADDR",
		"ENCRYPTION_KEY",
		"CLASHKING_PROXY_INTERNAL_ORIGIN",
		"TIMESCALE_HOST",
		"TIMESCALE_PORT",
		"TIMESCALE_USERNAME",
		"TIMESCALE_PASSWORD",
		"TIMESCALE_DATABASE",
		"TIMESCALE_SSLMODE",
		"VALKEY_HOST",
		"VALKEY_PORT",
		"VALKEY_PASSWORD",
		"REDDIT_CLIENT_ID",
		"REDDIT_CLIENT_SECRET",
		"REDDIT_USERNAME",
		"REDDIT_PASSWORD",
		"DATA_ENCRYPTION_KEY",
		"MOBILE_PUSH_FCM_PROJECT_ID",
		"MOBILE_PUSH_FCM_SERVICE_ACCOUNT_JSON",
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
