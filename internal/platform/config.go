package platform

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Script                                 string
	HTTPAddr                               string
	GRPCAddr                               string
	ProxyURL                               string
	StatsMongoURI                          string
	StaticMongoURI                         string
	TimescaleURL                           string
	ValkeyAddr                             string
	ValkeyPassword                         string
	TargetPageMultiplier                   int
	GlobalClanPriorityRequestsPerSecond    int
	GlobalClanNonPriorityRequestsPerSecond int
	GlobalClanMaxInFlight                  int
	BattlelogRequestsPerSecond             int
	BattlelogRollupFlushAttacks            int
	BattlelogCheckpointTTLDays             int
	BattlelogFirstSeenLookbackDays         int
	WarRequestsPerSecond                   int
	WarMaxInFlight                         int
	WarCWLSyncSeconds                      int
	BotClanRequestsPerSecond               int
	BotClanSnapshotPrefix                  string
	BotClanCWLStateSnapshot                string
	R2Endpoint                             string
	R2AccessKeyID                          string
	R2SecretAccessKey                      string
	R2Bucket                               string
	R2Prefix                               string
	R2MockUpload                           bool
	EventStreamName                        string
	EventStreamGroup                       string
	EventStreamConsumer                    string
	EventStreamRetentionSeconds            int
	EventStreamBatchSize                   int
	EventStreamReclaimIdleSeconds          int
	BotPlayerRequestsPerSecond             int
	ScheduledIntervalSeconds               int
	GiveawayScanSeconds                    int
	RedditPollSeconds                      int
	RedditLimit                            int
	RedditClientID                         string
	RedditSecret                           string
	RedditUsername                         string
	RedditPassword                         string
	MobilePushAPNSBearerToken              string
	MobilePushAPNSBundleID                 string
	MobilePushFCMBearerToken               string
	MobilePushFCMProjectID                 string
	MobilePushTokenKey                     string
	RunOnce                                bool
	DryRun                                 bool
	MockDB                                 bool
	OTELEnabled                            bool
	OTELServiceName                        string
	OTELExporterOTLPEndpoint               string
}

func Load() Config {
	return LoadWithArgs(os.Args[1:])
}

func LoadWithArgs(args []string) Config {
	// Local .env files are optional; deployed environments can rely entirely on real env vars.
	_ = godotenv.Load()

	cfg, err := loadConfigFile("config.json")
	if err != nil {
		panic(err)
	}
	applySecretEnv(&cfg)
	deriveConfig(&cfg)

	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	script := fs.String("script", cfg.Script, "script/domain to run")
	_ = fs.Parse(args)

	cfg.Script = strings.TrimSpace(*script)
	return cfg
}

func (c Config) Enabled(name string) bool {
	return c.Script == name
}

type jsonConfig struct {
	HTTPAddr             string                `json:"http_addr"`
	GRPCAddr             string                `json:"grpc_addr"`
	ProxyURL             string                `json:"proxy_url"`
	ValkeyAddr           string                `json:"valkey_addr"`
	RunOnce              bool                  `json:"run_once"`
	DryRun               bool                  `json:"dry_run"`
	MockDB               bool                  `json:"mock_db"`
	TargetPageMultiplier int                   `json:"target_page_multiplier"`
	OTEL                 jsonOTELConfig        `json:"otel"`
	R2                   jsonR2Config          `json:"r2"`
	Events               jsonEventsConfig      `json:"events"`
	GlobalClans          jsonGlobalClansConfig `json:"globalclans"`
	Battlelogs           jsonBattlelogsConfig  `json:"battlelogs"`
	Wars                 jsonWarsConfig        `json:"wars"`
	BotClans             jsonBotClansConfig    `json:"botclans"`
	BotPlayers           jsonBotPlayersConfig  `json:"botplayers"`
	Scheduled            jsonScheduledConfig   `json:"scheduled"`
	Giveaways            jsonGiveawaysConfig   `json:"giveaways"`
	Reddit               jsonRedditConfig      `json:"reddit"`
}

type jsonOTELConfig struct {
	Enabled              bool   `json:"enabled"`
	ServiceName          string `json:"service_name"`
	ExporterOTLPEndpoint string `json:"exporter_otlp_endpoint"`
}

type jsonR2Config struct {
	MockUpload bool `json:"mock_upload"`
}

type jsonEventsConfig struct {
	Stream             string `json:"stream"`
	Group              string `json:"group"`
	Consumer           string `json:"consumer"`
	RetentionSeconds   int    `json:"retention_seconds"`
	BatchSize          int    `json:"batch_size"`
	ReclaimIdleSeconds int    `json:"reclaim_idle_seconds"`
}

type jsonGlobalClansConfig struct {
	PriorityRequestsPerSecond    int `json:"priority_requests_per_second"`
	NonPriorityRequestsPerSecond int `json:"non_priority_requests_per_second"`
}

type jsonBattlelogsConfig struct {
	RequestsPerSecond     int `json:"requests_per_second"`
	RollupFlushAttacks    int `json:"rollup_flush_attacks"`
	CheckpointTTLDays     int `json:"checkpoint_ttl_days"`
	FirstSeenLookbackDays int `json:"first_seen_lookback_days"`
}

type jsonWarsConfig struct {
	RequestsPerSecond int `json:"requests_per_second"`
	CWLSyncSeconds    int `json:"cwl_sync_seconds"`
}

type jsonBotClansConfig struct {
	RequestsPerSecond int    `json:"requests_per_second"`
	SnapshotPrefix    string `json:"snapshot_prefix"`
	CWLStateSnapshot  string `json:"cwl_state_snapshot"`
}

type jsonBotPlayersConfig struct {
	RequestsPerSecond int `json:"requests_per_second"`
}

type jsonScheduledConfig struct {
	IntervalSeconds int `json:"interval_seconds"`
}

type jsonGiveawaysConfig struct {
	ScanSeconds int `json:"scan_seconds"`
}

type jsonRedditConfig struct {
	PollSeconds int `json:"poll_seconds"`
	Limit       int `json:"limit"`
}

func loadConfigFile(path string) (Config, error) {
	if strings.TrimSpace(path) == "" {
		return Config{}, errors.New("config path is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %s: %w", path, err)
	}
	var file jsonConfig
	if err := json.Unmarshal(raw, &file); err != nil {
		return Config{}, fmt.Errorf("parse config %s: %w", path, err)
	}
	return Config{
		HTTPAddr:                               file.HTTPAddr,
		GRPCAddr:                               file.GRPCAddr,
		ProxyURL:                               file.ProxyURL,
		ValkeyAddr:                             file.ValkeyAddr,
		RunOnce:                                file.RunOnce,
		DryRun:                                 file.DryRun,
		MockDB:                                 file.MockDB,
		TargetPageMultiplier:                   file.TargetPageMultiplier,
		OTELEnabled:                            file.OTEL.Enabled,
		OTELServiceName:                        file.OTEL.ServiceName,
		OTELExporterOTLPEndpoint:               file.OTEL.ExporterOTLPEndpoint,
		R2MockUpload:                           file.R2.MockUpload,
		EventStreamName:                        file.Events.Stream,
		EventStreamGroup:                       file.Events.Group,
		EventStreamConsumer:                    file.Events.Consumer,
		EventStreamRetentionSeconds:            file.Events.RetentionSeconds,
		EventStreamBatchSize:                   file.Events.BatchSize,
		EventStreamReclaimIdleSeconds:          file.Events.ReclaimIdleSeconds,
		GlobalClanPriorityRequestsPerSecond:    file.GlobalClans.PriorityRequestsPerSecond,
		GlobalClanNonPriorityRequestsPerSecond: file.GlobalClans.NonPriorityRequestsPerSecond,
		BattlelogRequestsPerSecond:             file.Battlelogs.RequestsPerSecond,
		BattlelogRollupFlushAttacks:            file.Battlelogs.RollupFlushAttacks,
		BattlelogCheckpointTTLDays:             file.Battlelogs.CheckpointTTLDays,
		BattlelogFirstSeenLookbackDays:         file.Battlelogs.FirstSeenLookbackDays,
		WarRequestsPerSecond:                   file.Wars.RequestsPerSecond,
		WarCWLSyncSeconds:                      file.Wars.CWLSyncSeconds,
		BotClanRequestsPerSecond:               file.BotClans.RequestsPerSecond,
		BotClanSnapshotPrefix:                  file.BotClans.SnapshotPrefix,
		BotClanCWLStateSnapshot:                file.BotClans.CWLStateSnapshot,
		BotPlayerRequestsPerSecond:             file.BotPlayers.RequestsPerSecond,
		ScheduledIntervalSeconds:               file.Scheduled.IntervalSeconds,
		GiveawayScanSeconds:                    file.Giveaways.ScanSeconds,
		RedditPollSeconds:                      file.Reddit.PollSeconds,
		RedditLimit:                            file.Reddit.Limit,
	}, nil
}

func applySecretEnv(cfg *Config) {
	overrideString(&cfg.StatsMongoURI, "STATS_MONGODB_URI")
	overrideString(&cfg.StaticMongoURI, "STATIC_MONGODB_URI")
	overrideString(&cfg.TimescaleURL, "TIMESCALE_URL")
	overrideString(&cfg.ValkeyPassword, "VALKEY_PASSWORD")
	overrideString(&cfg.R2Endpoint, "R2_ENDPOINT")
	overrideString(&cfg.R2AccessKeyID, "R2_ACCESS_KEY_ID")
	overrideString(&cfg.R2SecretAccessKey, "R2_SECRET_ACCESS_KEY")
	overrideString(&cfg.R2Bucket, "R2_BUCKET")
	overrideString(&cfg.R2Prefix, "R2_PREFIX")
	overrideString(&cfg.RedditClientID, "REDDIT_CLIENT_ID")
	overrideString(&cfg.RedditSecret, "REDDIT_CLIENT_SECRET")
	overrideString(&cfg.RedditUsername, "REDDIT_USERNAME")
	overrideString(&cfg.RedditPassword, "REDDIT_PASSWORD")
	overrideString(&cfg.MobilePushAPNSBearerToken, "MOBILE_PUSH_APNS_BEARER_TOKEN")
	overrideString(&cfg.MobilePushAPNSBundleID, "MOBILE_PUSH_APNS_BUNDLE_ID")
	overrideString(&cfg.MobilePushFCMBearerToken, "MOBILE_PUSH_FCM_BEARER_TOKEN")
	overrideString(&cfg.MobilePushFCMProjectID, "MOBILE_PUSH_FCM_PROJECT_ID")
	overrideString(&cfg.MobilePushTokenKey, "MOBILE_PUSH_TOKEN_KEY")
	if cfg.MobilePushTokenKey == "" {
		overrideString(&cfg.MobilePushTokenKey, "ENCRYPTION_KEY")
	}
}

func deriveConfig(cfg *Config) {
	cfg.GlobalClanMaxInFlight = cfg.GlobalClanPriorityRequestsPerSecond
	cfg.WarMaxInFlight = cfg.WarRequestsPerSecond
	if cfg.BotClanRequestsPerSecond == 0 {
		cfg.BotClanRequestsPerSecond = 950
	}
	if cfg.BotClanSnapshotPrefix == "" {
		cfg.BotClanSnapshotPrefix = "botclans:snapshot:"
	}
	if cfg.BotClanCWLStateSnapshot == "" {
		cfg.BotClanCWLStateSnapshot = "cwlstate"
	}
}

func overrideString(target *string, key string) {
	if value := os.Getenv(key); value != "" {
		*target = value
	}
}
