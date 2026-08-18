package platform

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Script                                 string
	HTTPAddr                               string
	GRPCAddr                               string
	ProxyURL                               string
	TimescaleURL                           string
	ValkeyAddr                             string
	ValkeyPassword                         string
	TargetPageMultiplier                   int
	GlobalClanPriorityRequestsPerSecond    int
	GlobalClanNonPriorityRequestsPerSecond int
	GlobalClanWriteWorkers                 int
	BattlelogRequestsPerSecond             int
	BattlelogPriorityRequestsPerSecond     int
	BattlelogCheckpointTTLDays             int
	BattlelogFirstSeenLookbackDays         int
	WarRequestsPerSecond                   int
	WarDormantRequestsPerSecond            int
	WarMaxInFlight                         int
	WarCWLSyncSeconds                      int
	TrackedClanRequestsPerSecond           int
	TrackedClanTargetRefreshSeconds        int
	TrackedClanSnapshotPrefix              string
	TrackedClanCWLStateSnapshot            string
	CapitalRequestsPerSecond               int
	CapitalTargetRefreshSeconds            int
	CapitalSnapshotPrefix                  string
	StatsTimescaleFlushSeconds             int
	EventStreamName                        string
	EventStreamGroup                       string
	EventStreamConsumer                    string
	EventStreamRetentionSeconds            int
	EventStreamBatchSize                   int
	EventStreamReclaimIdleSeconds          int
	TrackedPlayerRequestsPerSecond         int
	TrackedPlayerTargetRefreshSeconds      int
	BasicPlayerRequestsPerSecond           int
	LeaderboardRequestsPerSecond           int
	LeaderboardIntervalSeconds             int
	LeaderboardLimit                       int
	LeaderboardNullAssetURL                string
	ScheduledIntervalSeconds               int
	GiveawayScanSeconds                    int
	RedditPollSeconds                      int
	RedditClientID                         string
	RedditSecret                           string
	RedditUsername                         string
	RedditPassword                         string
	MobilePushFCMServiceAccountJSON        string
	MobilePushFCMProjectID                 string
	MobilePushTokenKey                     string
	MobilePushScanSeconds                  int
	RosterAutomationScanSeconds            int
	RosterAutomationBatchSize              int
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
	applyEnvironment(&cfg)
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
	HTTPAddr             string                     `json:"http_addr"`
	GRPCAddr             string                     `json:"grpc_addr"`
	RunOnce              bool                       `json:"run_once"`
	DryRun               bool                       `json:"dry_run"`
	MockDB               bool                       `json:"mock_db"`
	TargetPageMultiplier int                        `json:"target_page_multiplier"`
	OTEL                 jsonOTELConfig             `json:"otel"`
	Stats                jsonStatsConfig            `json:"stats"`
	Events               jsonEventsConfig           `json:"events"`
	GlobalClans          jsonGlobalClansConfig      `json:"globalclans"`
	Battlelogs           jsonBattlelogsConfig       `json:"battlelogs"`
	Wars                 jsonWarsConfig             `json:"wars"`
	TrackedClans         jsonTrackedClansConfig     `json:"trackedclans"`
	Capital              jsonCapitalConfig          `json:"capital"`
	TrackedPlayers       jsonTrackedPlayersConfig   `json:"trackedplayers"`
	BasicPlayers         jsonBasicPlayersConfig     `json:"basicplayers"`
	Leaderboards         jsonLeaderboardsConfig     `json:"leaderboards"`
	Scheduled            jsonScheduledConfig        `json:"scheduled"`
	Giveaways            jsonGiveawaysConfig        `json:"giveaways"`
	Reddit               jsonRedditConfig           `json:"reddit"`
	MobilePush           jsonMobilePushConfig       `json:"mobile_push"`
	RosterAutomations    jsonRosterAutomationConfig `json:"roster_automations"`
}

type jsonOTELConfig struct {
	Enabled              bool   `json:"enabled"`
	ServiceName          string `json:"service_name"`
	ExporterOTLPEndpoint string `json:"exporter_otlp_endpoint"`
}

type jsonStatsConfig struct {
	TimescaleFlushSeconds int `json:"timescale_flush_seconds"`
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
	WriteWorkers                 int `json:"write_workers"`
}

type jsonBattlelogsConfig struct {
	RequestsPerSecond         int `json:"requests_per_second"`
	PriorityRequestsPerSecond int `json:"priority_requests_per_second"`
	CheckpointTTLDays         int `json:"checkpoint_ttl_days"`
	FirstSeenLookbackDays     int `json:"first_seen_lookback_days"`
}

type jsonWarsConfig struct {
	RequestsPerSecond        int `json:"requests_per_second"`
	DormantRequestsPerSecond int `json:"dormant_requests_per_second"`
	CWLSyncSeconds           int `json:"cwl_sync_seconds"`
}

type jsonTrackedClansConfig struct {
	RequestsPerSecond    int    `json:"requests_per_second"`
	TargetRefreshSeconds int    `json:"target_refresh_seconds"`
	SnapshotPrefix       string `json:"snapshot_prefix"`
	CWLStateSnapshot     string `json:"cwl_state_snapshot"`
}

type jsonTrackedPlayersConfig struct {
	RequestsPerSecond    int `json:"requests_per_second"`
	TargetRefreshSeconds int `json:"target_refresh_seconds"`
}

type jsonCapitalConfig struct {
	RequestsPerSecond    int    `json:"requests_per_second"`
	TargetRefreshSeconds int    `json:"target_refresh_seconds"`
	SnapshotPrefix       string `json:"snapshot_prefix"`
}

type jsonBasicPlayersConfig struct {
	RequestsPerSecond int `json:"requests_per_second"`
}

type jsonLeaderboardsConfig struct {
	RequestsPerSecond int    `json:"requests_per_second"`
	IntervalSeconds   int    `json:"interval_seconds"`
	Limit             int    `json:"limit"`
	NullAssetURL      string `json:"null_asset_url"`
}

type jsonScheduledConfig struct {
	IntervalSeconds int `json:"interval_seconds"`
}

type jsonGiveawaysConfig struct {
	ScanSeconds int `json:"scan_seconds"`
}

type jsonRedditConfig struct {
	PollSeconds int `json:"poll_seconds"`
}

type jsonMobilePushConfig struct {
	ScanSeconds int `json:"scan_seconds"`
}

type jsonRosterAutomationConfig struct {
	ScanSeconds int `json:"scan_seconds"`
	BatchSize   int `json:"batch_size"`
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
		RunOnce:                                file.RunOnce,
		DryRun:                                 file.DryRun,
		MockDB:                                 file.MockDB,
		TargetPageMultiplier:                   file.TargetPageMultiplier,
		OTELEnabled:                            file.OTEL.Enabled,
		OTELServiceName:                        file.OTEL.ServiceName,
		OTELExporterOTLPEndpoint:               file.OTEL.ExporterOTLPEndpoint,
		StatsTimescaleFlushSeconds:             file.Stats.TimescaleFlushSeconds,
		EventStreamName:                        file.Events.Stream,
		EventStreamGroup:                       file.Events.Group,
		EventStreamConsumer:                    file.Events.Consumer,
		EventStreamRetentionSeconds:            file.Events.RetentionSeconds,
		EventStreamBatchSize:                   file.Events.BatchSize,
		EventStreamReclaimIdleSeconds:          file.Events.ReclaimIdleSeconds,
		GlobalClanPriorityRequestsPerSecond:    file.GlobalClans.PriorityRequestsPerSecond,
		GlobalClanNonPriorityRequestsPerSecond: file.GlobalClans.NonPriorityRequestsPerSecond,
		GlobalClanWriteWorkers:                 file.GlobalClans.WriteWorkers,
		BattlelogRequestsPerSecond:             file.Battlelogs.RequestsPerSecond,
		BattlelogPriorityRequestsPerSecond:     file.Battlelogs.PriorityRequestsPerSecond,
		BattlelogCheckpointTTLDays:             file.Battlelogs.CheckpointTTLDays,
		BattlelogFirstSeenLookbackDays:         file.Battlelogs.FirstSeenLookbackDays,
		WarRequestsPerSecond:                   file.Wars.RequestsPerSecond,
		WarDormantRequestsPerSecond:            file.Wars.DormantRequestsPerSecond,
		WarCWLSyncSeconds:                      file.Wars.CWLSyncSeconds,
		TrackedClanRequestsPerSecond:           file.TrackedClans.RequestsPerSecond,
		TrackedClanTargetRefreshSeconds:        file.TrackedClans.TargetRefreshSeconds,
		TrackedClanSnapshotPrefix:              file.TrackedClans.SnapshotPrefix,
		TrackedClanCWLStateSnapshot:            file.TrackedClans.CWLStateSnapshot,
		CapitalRequestsPerSecond:               file.Capital.RequestsPerSecond,
		CapitalTargetRefreshSeconds:            file.Capital.TargetRefreshSeconds,
		CapitalSnapshotPrefix:                  file.Capital.SnapshotPrefix,
		TrackedPlayerRequestsPerSecond:         file.TrackedPlayers.RequestsPerSecond,
		TrackedPlayerTargetRefreshSeconds:      file.TrackedPlayers.TargetRefreshSeconds,
		BasicPlayerRequestsPerSecond:           file.BasicPlayers.RequestsPerSecond,
		LeaderboardRequestsPerSecond:           file.Leaderboards.RequestsPerSecond,
		LeaderboardIntervalSeconds:             file.Leaderboards.IntervalSeconds,
		LeaderboardLimit:                       file.Leaderboards.Limit,
		LeaderboardNullAssetURL:                file.Leaderboards.NullAssetURL,
		ScheduledIntervalSeconds:               file.Scheduled.IntervalSeconds,
		GiveawayScanSeconds:                    file.Giveaways.ScanSeconds,
		RedditPollSeconds:                      file.Reddit.PollSeconds,
		MobilePushScanSeconds:                  file.MobilePush.ScanSeconds,
		RosterAutomationScanSeconds:            file.RosterAutomations.ScanSeconds,
		RosterAutomationBatchSize:              file.RosterAutomations.BatchSize,
	}, nil
}

func applyEnvironment(cfg *Config) {
	cfg.ProxyURL = appendOriginPath(os.Getenv("CLASHKING_PROXY_INTERNAL_ORIGIN"), "/v1")
	cfg.TimescaleURL = buildTimescaleURL(os.Getenv)
	cfg.ValkeyAddr = buildValkeyAddress(os.Getenv)
	cfg.ValkeyPassword = os.Getenv("VALKEY_PASSWORD")
	cfg.RedditClientID = strings.TrimSpace(os.Getenv("REDDIT_CLIENT_ID"))
	cfg.RedditSecret = os.Getenv("REDDIT_CLIENT_SECRET")
	cfg.RedditUsername = strings.TrimSpace(os.Getenv("REDDIT_USERNAME"))
	cfg.RedditPassword = os.Getenv("REDDIT_PASSWORD")
	cfg.MobilePushFCMServiceAccountJSON = os.Getenv("MOBILE_PUSH_FCM_SERVICE_ACCOUNT_JSON")
	cfg.MobilePushFCMProjectID = strings.TrimSpace(os.Getenv("MOBILE_PUSH_FCM_PROJECT_ID"))
	cfg.MobilePushTokenKey = os.Getenv("DATA_ENCRYPTION_KEY")
}

func deriveConfig(cfg *Config) {
	if cfg.GlobalClanWriteWorkers == 0 {
		cfg.GlobalClanWriteWorkers = 1
	}
	cfg.WarMaxInFlight = RequestConcurrency(cfg.WarRequestsPerSecond)
	if cfg.WarDormantRequestsPerSecond == 0 {
		cfg.WarDormantRequestsPerSecond = 50
	}
	if cfg.BattlelogPriorityRequestsPerSecond == 0 {
		cfg.BattlelogPriorityRequestsPerSecond = 100
	}
	if cfg.TrackedClanRequestsPerSecond == 0 {
		cfg.TrackedClanRequestsPerSecond = 950
	}
	if cfg.TrackedClanTargetRefreshSeconds == 0 {
		cfg.TrackedClanTargetRefreshSeconds = 3600
	}
	if cfg.TrackedClanSnapshotPrefix == "" {
		cfg.TrackedClanSnapshotPrefix = "trackedclans:snapshot:"
	}
	if cfg.TrackedClanCWLStateSnapshot == "" {
		cfg.TrackedClanCWLStateSnapshot = "cwlstate"
	}
	if cfg.CapitalRequestsPerSecond == 0 {
		cfg.CapitalRequestsPerSecond = 250
	}
	if cfg.CapitalTargetRefreshSeconds == 0 {
		cfg.CapitalTargetRefreshSeconds = 300
	}
	if cfg.CapitalSnapshotPrefix == "" {
		cfg.CapitalSnapshotPrefix = "capital:raid:"
	}
	if cfg.TrackedPlayerTargetRefreshSeconds == 0 {
		cfg.TrackedPlayerTargetRefreshSeconds = 3600
	}
	if cfg.BasicPlayerRequestsPerSecond == 0 {
		cfg.BasicPlayerRequestsPerSecond = 30
	}
	if cfg.LeaderboardRequestsPerSecond == 0 {
		cfg.LeaderboardRequestsPerSecond = 100
	}
	if cfg.LeaderboardIntervalSeconds == 0 {
		cfg.LeaderboardIntervalSeconds = 600
	}
	if cfg.LeaderboardLimit == 0 {
		cfg.LeaderboardLimit = 500
	}
	if cfg.LeaderboardNullAssetURL == "" {
		cfg.LeaderboardNullAssetURL = "https://api-assets.clashofclans.com/null"
	}
	if cfg.StatsTimescaleFlushSeconds == 0 {
		cfg.StatsTimescaleFlushSeconds = 60
	}
	if cfg.RosterAutomationScanSeconds == 0 {
		cfg.RosterAutomationScanSeconds = 15
	}
	if cfg.RosterAutomationBatchSize == 0 {
		cfg.RosterAutomationBatchSize = 100
	}
}

func buildTimescaleURL(getenv func(string) string) string {
	host := strings.TrimSpace(getenv("TIMESCALE_HOST"))
	username := strings.TrimSpace(getenv("TIMESCALE_USERNAME"))
	password := getenv("TIMESCALE_PASSWORD")
	database := strings.TrimSpace(getenv("TIMESCALE_DATABASE"))
	if host == "" || username == "" || password == "" || database == "" {
		return ""
	}
	connection := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(username, password),
		Host:   net.JoinHostPort(host, firstNonEmpty(getenv("TIMESCALE_PORT"), "5432")),
		Path:   database,
	}
	query := connection.Query()
	query.Set("sslmode", firstNonEmpty(getenv("TIMESCALE_SSLMODE"), "disable"))
	connection.RawQuery = query.Encode()
	return connection.String()
}

func buildValkeyAddress(getenv func(string) string) string {
	host := strings.TrimSpace(getenv("VALKEY_HOST"))
	if host == "" {
		return ""
	}
	return net.JoinHostPort(host, firstNonEmpty(getenv("VALKEY_PORT"), "6379"))
}

func normalizeOrigin(value string) string {
	return strings.TrimRight(strings.TrimSpace(value), "/")
}

func appendOriginPath(origin, path string) string {
	origin = normalizeOrigin(origin)
	if origin == "" {
		return ""
	}
	return origin + path
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
