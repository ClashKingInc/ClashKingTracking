package platform

import (
	"flag"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPAddr          string
	GRPCAddr          string
	ProxyURL          string
	StatsMongoURI     string
	StaticMongoURI    string
	RedisAddr         string
	RedisPassword     string
	RedditClientID    string
	RedditSecret      string
	RedditUsername    string
	RedditPassword    string
	RunOnce           bool
	DryRun            bool
	MockDB            bool
	Domains           map[string]bool
	EventBufferSize   int
	RecentEventBuffer int
}

func Load() Config {
	// Local .env files are optional; deployed environments can rely entirely on real env vars.
	_ = godotenv.Load()

	domainList := flag.String("domains", os.Getenv("DOMAINS"), "comma-separated domains to run")
	runOnce := flag.Bool("run-once", envBool("RUN_ONCE"), "run a single cycle where supported")
	dryRun := flag.Bool("dry-run", envBool("DRY_RUN"), "disable persistent writes")
	mockDB := flag.Bool("mock-db", envBool("MOCK_DB"), "use in-memory repositories")
	httpAddr := flag.String("http-addr", envString("HTTP_ADDR", ":8080"), "http listen address")
	grpcAddr := flag.String("grpc-addr", envString("GRPC_ADDR", ":9090"), "grpc listen address")
	flag.Parse()

	return Config{
		HTTPAddr:          *httpAddr,
		GRPCAddr:          *grpcAddr,
		ProxyURL:          envString("PROXY_URL", ""),
		StatsMongoURI:     envString("STATS_MONGODB_URI", ""),
		StaticMongoURI:    envString("STATIC_MONGODB_URI", ""),
		RedisAddr:         envString("REDIS_ADDR", ""),
		RedisPassword:     envString("REDIS_PASSWORD", ""),
		RedditClientID:    envString("REDDIT_CLIENT_ID", ""),
		RedditSecret:      envString("REDDIT_CLIENT_SECRET", ""),
		RedditUsername:    envString("REDDIT_USERNAME", ""),
		RedditPassword:    envString("REDDIT_PASSWORD", ""),
		RunOnce:           *runOnce,
		DryRun:            *dryRun,
		MockDB:            *mockDB,
		Domains:           parseDomains(*domainList),
		EventBufferSize:   envInt("EVENT_BUFFER_SIZE", 1024),
		RecentEventBuffer: envInt("RECENT_EVENT_BUFFER", 2048),
	}
}

func (c Config) Enabled(name string) bool {
	if len(c.Domains) == 0 {
		return true
	}
	return c.Domains[name]
}

func envString(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	return value == "1" || value == "true" || value == "yes"
}

func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func parseDomains(raw string) map[string]bool {
	// An empty domain filter means "run everything"; otherwise only listed domains are enabled.
	result := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		result[part] = true
	}
	return result
}
