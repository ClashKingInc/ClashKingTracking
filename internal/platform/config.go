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
	ProxyURL                               string
	ClashKingAPIURL                        string
	ClashKingAPIToken                      string
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
	WarDiscoveryActiveRequestsPerSecond    int
	WarDiscoveryDormantRequestsPerSecond   int
	WarDiscoveryMaxInFlight                int
	CWLRequestsPerSecond                   int
	CWLWarRequestsPerSecond                int
	CWLSyncSeconds                         int
	CWLResolveLeagueFromClanProfile        bool
	TrackedClanRequestsPerSecond           int
	TrackedClanTargetRefreshSeconds        int
	TrackedClanSnapshotPrefix              string
	TrackedClanCWLStateSnapshot            string
	CapitalRequestsPerSecond               int
	CapitalTargetRefreshSeconds            int
	CapitalSnapshotPrefix                  string
	StatsTimescaleFlushSeconds             int
	EventStreamName                        string
	EventStreamConsumer                    string
	EventStreamRetentionSeconds            int
	EventStreamReclaimIdleSeconds          int
	TrackedPlayerRequestsPerSecond         int
	TrackedPlayerTargetRefreshSeconds      int
	BasicPlayerRequestsPerSecond           int
	LeaderboardIntervalSeconds             int
	LeaderboardLimit                       int
	LeaderboardNullAssetURL                string
	ScheduledRequestsPerSecond             int
	ScheduledIntervalSeconds               int
	CloudflareAccountID                    string
	CloudflareAIGatewayID                  string
	CloudflareAIAPIToken                   string
	CloudflareAIAPIOrigin                  string
	ReminderRequestsPerSecond              int
	GiveawayScanSeconds                    int
	RedditPollSeconds                      int
	RedditClientID                         string
	RedditSecret                           string
	RedditUsername                         string
	RedditPassword                         string
	MobilePushFCMServiceAccountJSON        string
	MobilePushFCMProjectID                 string
	MobilePushFCMAPIOrigin                 string
	MobilePushTokenKey                     string
	MobilePushScanSeconds                  int
	RosterAutomationScanSeconds            int
	RosterAutomationBatchSize              int
	DiscordBotToken                        string
	DiscordAPIURL                          string
	DiscordGatewayQueueSize                int
	DiscordGatewayMemberChunkConcurrency   int
	DiscordMessageCreateEnabled            bool
	DiscordDeliveryBatchSize               int
	WarArchiveEndpoint                     string
	WarArchiveOrigin                       string
	WarArchiveBucket                       string
	WarArchiveAccessKeyID                  string
	WarArchiveSecretAccessKey              string
	WarArchiveRequestsPerSecond            int
	WarArchiveScanSeconds                  int
	WarArchivePackSize                     int
	RunOnce                                bool
	DryRun                                 bool
	MockDB                                 bool
	SentryDSN                              string
	SentryEnvironment                      string
	SentryRelease                          string
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

type jsonConfig struct {
	RunOnce              bool                       `json:"run_once"`
	DryRun               bool                       `json:"dry_run"`
	MockDB               bool                       `json:"mock_db"`
	TargetPageMultiplier int                        `json:"target_page_multiplier"`
	Stats                jsonStatsConfig            `json:"stats"`
	Events               jsonEventsConfig           `json:"events"`
	GlobalClans          jsonGlobalClansConfig      `json:"globalclans"`
	Battlelogs           jsonBattlelogsConfig       `json:"battlelogs"`
	WarDiscovery         jsonWarDiscoveryConfig     `json:"war_discovery"`
	CWL                  jsonCWLConfig              `json:"cwl"`
	WarArchiver          jsonWarArchiverConfig      `json:"war_archiver"`
	TrackedClans         jsonTrackedClansConfig     `json:"trackedclans"`
	Capital              jsonCapitalConfig          `json:"capital"`
	TrackedPlayers       jsonTrackedPlayersConfig   `json:"trackedplayers"`
	BasicPlayers         jsonBasicPlayersConfig     `json:"basicplayers"`
	Leaderboards         jsonLeaderboardsConfig     `json:"leaderboards"`
	Scheduled            jsonScheduledConfig        `json:"scheduled"`
	Reminders            jsonRemindersConfig        `json:"reminders"`
	Giveaways            jsonGiveawaysConfig        `json:"giveaways"`
	Reddit               jsonRedditConfig           `json:"reddit"`
	MobilePush           jsonMobilePushConfig       `json:"mobile_push"`
	RosterAutomations    jsonRosterAutomationConfig `json:"roster_automations"`
	DiscordGateway       jsonDiscordGatewayConfig   `json:"discord_gateway"`
	DiscordDelivery      jsonDiscordDeliveryConfig  `json:"discord_delivery"`
}

type jsonStatsConfig struct {
	TimescaleFlushSeconds int `json:"timescale_flush_seconds"`
}

type jsonEventsConfig struct {
	Stream             string `json:"stream"`
	Consumer           string `json:"consumer"`
	RetentionSeconds   int    `json:"retention_seconds"`
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

type jsonWarDiscoveryConfig struct {
	ActiveRequestsPerSecond  int `json:"active_requests_per_second"`
	DormantRequestsPerSecond int `json:"dormant_requests_per_second"`
}

type jsonCWLConfig struct {
	RequestsPerSecond            int  `json:"requests_per_second"`
	WarRequestsPerSecond         int  `json:"war_requests_per_second"`
	SyncSeconds                  int  `json:"sync_seconds"`
	ResolveLeagueFromClanProfile bool `json:"resolve_league_from_clan_profile"`
}

type jsonWarArchiverConfig struct {
	RequestsPerSecond int `json:"requests_per_second"`
	ScanSeconds       int `json:"scan_seconds"`
	PackSize          int `json:"pack_size"`
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
	IntervalSeconds int    `json:"interval_seconds"`
	Limit           int    `json:"limit"`
	NullAssetURL    string `json:"null_asset_url"`
}

type jsonScheduledConfig struct {
	RequestsPerSecond int `json:"requests_per_second"`
	IntervalSeconds   int `json:"interval_seconds"`
}

type jsonRemindersConfig struct {
	RequestsPerSecond int `json:"requests_per_second"`
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

type jsonDiscordGatewayConfig struct {
	QueueSize              int  `json:"queue_size"`
	MemberChunkConcurrency int  `json:"member_chunk_concurrency"`
	MessageCreateEnabled   bool `json:"message_create_enabled"`
}

type jsonDiscordDeliveryConfig struct {
	BatchSize int `json:"batch_size"`
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
		RunOnce:                                file.RunOnce,
		DryRun:                                 file.DryRun,
		MockDB:                                 file.MockDB,
		TargetPageMultiplier:                   file.TargetPageMultiplier,
		StatsTimescaleFlushSeconds:             file.Stats.TimescaleFlushSeconds,
		EventStreamName:                        file.Events.Stream,
		EventStreamConsumer:                    file.Events.Consumer,
		EventStreamRetentionSeconds:            file.Events.RetentionSeconds,
		EventStreamReclaimIdleSeconds:          file.Events.ReclaimIdleSeconds,
		GlobalClanPriorityRequestsPerSecond:    file.GlobalClans.PriorityRequestsPerSecond,
		GlobalClanNonPriorityRequestsPerSecond: file.GlobalClans.NonPriorityRequestsPerSecond,
		GlobalClanWriteWorkers:                 file.GlobalClans.WriteWorkers,
		BattlelogRequestsPerSecond:             file.Battlelogs.RequestsPerSecond,
		BattlelogPriorityRequestsPerSecond:     file.Battlelogs.PriorityRequestsPerSecond,
		BattlelogCheckpointTTLDays:             file.Battlelogs.CheckpointTTLDays,
		BattlelogFirstSeenLookbackDays:         file.Battlelogs.FirstSeenLookbackDays,
		WarDiscoveryActiveRequestsPerSecond:    file.WarDiscovery.ActiveRequestsPerSecond,
		WarDiscoveryDormantRequestsPerSecond:   file.WarDiscovery.DormantRequestsPerSecond,
		CWLRequestsPerSecond:                   file.CWL.RequestsPerSecond,
		CWLWarRequestsPerSecond:                file.CWL.WarRequestsPerSecond,
		CWLSyncSeconds:                         file.CWL.SyncSeconds,
		CWLResolveLeagueFromClanProfile:        file.CWL.ResolveLeagueFromClanProfile,
		WarArchiveRequestsPerSecond:            file.WarArchiver.RequestsPerSecond,
		WarArchiveScanSeconds:                  file.WarArchiver.ScanSeconds,
		WarArchivePackSize:                     file.WarArchiver.PackSize,
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
		LeaderboardIntervalSeconds:             file.Leaderboards.IntervalSeconds,
		LeaderboardLimit:                       file.Leaderboards.Limit,
		LeaderboardNullAssetURL:                file.Leaderboards.NullAssetURL,
		ScheduledRequestsPerSecond:             file.Scheduled.RequestsPerSecond,
		ScheduledIntervalSeconds:               file.Scheduled.IntervalSeconds,
		ReminderRequestsPerSecond:              file.Reminders.RequestsPerSecond,
		GiveawayScanSeconds:                    file.Giveaways.ScanSeconds,
		RedditPollSeconds:                      file.Reddit.PollSeconds,
		MobilePushScanSeconds:                  file.MobilePush.ScanSeconds,
		RosterAutomationScanSeconds:            file.RosterAutomations.ScanSeconds,
		RosterAutomationBatchSize:              file.RosterAutomations.BatchSize,
		DiscordGatewayQueueSize:                file.DiscordGateway.QueueSize,
		DiscordGatewayMemberChunkConcurrency:   file.DiscordGateway.MemberChunkConcurrency,
		DiscordMessageCreateEnabled:            file.DiscordGateway.MessageCreateEnabled,
		DiscordDeliveryBatchSize:               file.DiscordDelivery.BatchSize,
	}, nil
}

func applyEnvironment(cfg *Config) {
	cfg.ProxyURL = appendOriginPath(os.Getenv("CLASHKING_PROXY_INTERNAL_ORIGIN"), "/v1")
	cfg.ClashKingAPIURL = normalizeOrigin(firstNonEmpty(os.Getenv("CLASHKING_API_ORIGIN"), os.Getenv("CLASHKING_API_URL")))
	cfg.ClashKingAPIToken = os.Getenv("CLASHKING_API_TOKEN")
	cfg.TimescaleURL = buildTimescaleURL(os.Getenv)
	cfg.ValkeyAddr = buildValkeyAddress(os.Getenv)
	cfg.ValkeyPassword = os.Getenv("VALKEY_PASSWORD")
	cfg.RedditClientID = strings.TrimSpace(os.Getenv("REDDIT_CLIENT_ID"))
	cfg.RedditSecret = os.Getenv("REDDIT_CLIENT_SECRET")
	cfg.RedditUsername = strings.TrimSpace(os.Getenv("REDDIT_USERNAME"))
	cfg.RedditPassword = os.Getenv("REDDIT_PASSWORD")
	cfg.MobilePushFCMServiceAccountJSON = os.Getenv("MOBILE_PUSH_FCM_SERVICE_ACCOUNT_JSON")
	cfg.MobilePushFCMProjectID = strings.TrimSpace(os.Getenv("MOBILE_PUSH_FCM_PROJECT_ID"))
	cfg.MobilePushFCMAPIOrigin = normalizeOrigin(os.Getenv("CLASHKING_LOCAL_FCM_API_ORIGIN"))
	cfg.MobilePushTokenKey = os.Getenv("DATA_ENCRYPTION_KEY")
	cfg.DiscordBotToken = firstNonEmpty(os.Getenv("DISCORD_BOT_TOKEN"), os.Getenv("BOT_TOKEN"))
	cfg.DiscordAPIURL = normalizeOrigin(os.Getenv("CLASHKING_LOCAL_DISCORD_API_URL"))
	cfg.SentryDSN = strings.TrimSpace(os.Getenv("SENTRY_DSN"))
	cfg.SentryEnvironment = strings.TrimSpace(os.Getenv("SENTRY_ENVIRONMENT"))
	cfg.SentryRelease = strings.TrimSpace(os.Getenv("SENTRY_RELEASE"))
	cfg.CloudflareAccountID = strings.TrimSpace(os.Getenv("CLOUDFLARE_ACCOUNT_ID"))
	cfg.CloudflareAIGatewayID = firstNonEmpty(strings.TrimSpace(os.Getenv("CLOUDFLARE_AI_GATEWAY_ID")), "clashking")
	cfg.CloudflareAIAPIToken = os.Getenv("CLOUDFLARE_AI_API_TOKEN")
	cfg.CloudflareAIAPIOrigin = normalizeOrigin(os.Getenv("CLASHKING_LOCAL_CLOUDFLARE_AI_API_ORIGIN"))
	if value := strings.TrimSpace(os.Getenv("DISCORD_MESSAGE_CREATE_ENABLED")); value != "" {
		cfg.DiscordMessageCreateEnabled = strings.EqualFold(value, "true") || value == "1"
	}
	cfg.WarArchiveEndpoint = normalizeOrigin(firstNonEmpty(os.Getenv("WAR_ARCHIVE_S3_ENDPOINT"), os.Getenv("R2_ENDPOINT"), os.Getenv("R2_ENDPOINT_URL")))
	if cfg.WarArchiveEndpoint == "" {
		accountID := strings.TrimSpace(os.Getenv("R2_ACCOUNT_ID"))
		if accountID != "" {
			cfg.WarArchiveEndpoint = "https://" + accountID + ".r2.cloudflarestorage.com"
		}
	}
	cfg.WarArchiveOrigin = normalizeOrigin(firstNonEmpty(os.Getenv("WAR_ARCHIVE_ORIGIN"), "https://wars.clashk.ing"))
	cfg.WarArchiveBucket = firstNonEmpty(os.Getenv("WAR_ARCHIVE_BUCKET"), os.Getenv("R2_WARS_BUCKET"), "clashking-wars")
	cfg.WarArchiveAccessKeyID = os.Getenv("R2_ACCESS_KEY_ID")
	cfg.WarArchiveSecretAccessKey = os.Getenv("R2_SECRET_ACCESS_KEY")
}

func deriveConfig(cfg *Config) {
	if cfg.GlobalClanWriteWorkers == 0 {
		cfg.GlobalClanWriteWorkers = 1
	}
	cfg.WarDiscoveryMaxInFlight = RequestConcurrency(cfg.WarDiscoveryActiveRequestsPerSecond)
	if cfg.WarDiscoveryDormantRequestsPerSecond == 0 {
		cfg.WarDiscoveryDormantRequestsPerSecond = 50
	}
	if cfg.CWLRequestsPerSecond == 0 {
		cfg.CWLRequestsPerSecond = 250
	}
	if cfg.CWLWarRequestsPerSecond == 0 {
		cfg.CWLWarRequestsPerSecond = cfg.CWLRequestsPerSecond
	}
	if cfg.CWLSyncSeconds == 0 {
		cfg.CWLSyncSeconds = 180
	}
	if cfg.WarArchiveScanSeconds == 0 {
		cfg.WarArchiveScanSeconds = 30
	}
	if cfg.WarArchiveRequestsPerSecond == 0 {
		cfg.WarArchiveRequestsPerSecond = 1_000
	}
	if cfg.WarArchivePackSize == 0 {
		cfg.WarArchivePackSize = 10_000
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
	if cfg.ScheduledRequestsPerSecond == 0 {
		cfg.ScheduledRequestsPerSecond = 100
	}
	if cfg.ReminderRequestsPerSecond == 0 {
		cfg.ReminderRequestsPerSecond = 50
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
	if cfg.DiscordGatewayQueueSize == 0 {
		cfg.DiscordGatewayQueueSize = 4096
	}
	if cfg.DiscordGatewayMemberChunkConcurrency == 0 {
		cfg.DiscordGatewayMemberChunkConcurrency = 2
	}
	if cfg.DiscordDeliveryBatchSize == 0 {
		cfg.DiscordDeliveryBatchSize = 50
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

// ValidateLoopbackProviderURL keeps test-provider overrides from forwarding
// Discord or push credentials to a remote host.
func ValidateLoopbackProviderURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("local provider URL must be an HTTP loopback URL without credentials, query, or fragment")
	}
	host := net.ParseIP(parsed.Hostname())
	if host == nil || !host.IsLoopback() || parsed.Port() == "" {
		return errors.New("local provider URL must use a loopback IP and explicit port")
	}
	return nil
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
