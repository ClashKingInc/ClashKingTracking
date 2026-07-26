package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
	valkey "github.com/valkey-io/valkey-go"
)

type Domain interface {
	Name() string
	Run(context.Context, *App) error
}

type App struct {
	Config      Config
	Logger      *slog.Logger
	Valkey      valkey.Client
	Clash       *clashy.Client
	R2          ObjectStore
	Stats       *Tracker
	StatsWriter *TimescaleStatsWriter
	Scheduler   *Scheduler
}

func New(ctx context.Context, cfg Config) (*App, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if needsClashClient(cfg) && cfg.ProxyURL == "" {
		return nil, errors.New("proxy_url is required when Clash-backed domains are enabled")
	}
	var valkeyClient valkey.Client
	var err error
	if cfg.ValkeyAddr != "" {
		valkeyClient, err = valkey.NewClient(valkey.ClientOption{
			InitAddress:  []string{cfg.ValkeyAddr},
			Password:     cfg.ValkeyPassword,
			DisableCache: true,
		})
		if err != nil {
			return nil, err
		}
	}
	stats := NewTracker()
	var statsWriter *TimescaleStatsWriter
	if shouldPersistStats(cfg) {
		statsWriter, err = NewTimescaleStatsWriter(ctx, cfg.TimescaleURL, stats, cfg.Script, time.Duration(cfg.StatsTimescaleFlushSeconds)*time.Second)
		if err != nil {
			if valkeyClient != nil {
				valkeyClient.Close()
			}
			return nil, err
		}
	}
	var clashClient *clashy.Client
	if needsClashClient(cfg) {
		proxyLimit := proxyConnectionLimit(cfg)
		clashConfig := clashy.DefaultClientConfig()
		clashConfig.BaseURL = cfg.ProxyURL
		clashConfig.ThrottleLimit = proxyLimit
		clashConfig.LookupCache = false
		clashConfig.UpdateCache = false
		clashConfig.MaxBaseURLConns = proxyLimit
		clashClient, err = clashy.NewClient(clashConfig)
		if err != nil {
			if valkeyClient != nil {
				valkeyClient.Close()
			}
			if statsWriter != nil {
				statsWriter.Close()
			}
			return nil, err
		}
	}
	var objectStore ObjectStore
	hasR2Config := cfg.R2Endpoint != "" || cfg.R2Bucket != "" || cfg.R2AccessKeyID != "" || cfg.R2SecretAccessKey != ""
	if hasR2Config {
		objectStore, err = NewR2ObjectStore(cfg)
		if err != nil {
			if clashClient != nil {
				_ = clashClient.Close()
			}
			if valkeyClient != nil {
				valkeyClient.Close()
			}
			if statsWriter != nil {
				statsWriter.Close()
			}
			return nil, err
		}
	} else if cfg.R2MockUpload {
		objectStore = MockObjectStore{}
	}
	app := &App{
		Config:      cfg,
		Logger:      logger,
		Valkey:      valkeyClient,
		Clash:       clashClient,
		R2:          objectStore,
		Stats:       stats,
		StatsWriter: statsWriter,
		Scheduler:   NewScheduler(),
	}
	return app, nil
}

func (a *App) Close(ctx context.Context) error {
	if a.Valkey != nil {
		a.Valkey.Close()
	}
	if a.Clash != nil {
		_ = a.Clash.Close()
	}
	if a.StatsWriter != nil {
		a.StatsWriter.Close()
	}
	return nil
}

func Run(ctx context.Context, app *App, domains []Domain) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if app.StatsWriter != nil {
		go app.StatsWriter.Run(runCtx, app.Logger)
	}
	go func() {
		// Keep delayed jobs alive in the same process as the domains that schedule them.
		_ = app.Scheduler.Run(runCtx)
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, len(domains))
	for _, domain := range domains {
		domain := domain
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := domain.Run(runCtx, app); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("%s: %w", domain.Name(), err)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	var firstErr error
	for err := range errCh {
		if err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	return firstErr
}

func shouldPersistStats(cfg Config) bool {
	return !cfg.MockDB && !cfg.DryRun && cfg.TimescaleURL != "" && cfg.StatsTimescaleFlushSeconds > 0
}

func proxyConnectionLimit(cfg Config) int {
	rate := max(
		cfg.GlobalClanPriorityRequestsPerSecond+cfg.GlobalClanNonPriorityRequestsPerSecond,
		cfg.BattlelogRequestsPerSecond,
		cfg.BattlelogPriorityRequestsPerSecond,
		cfg.WarRequestsPerSecond,
		cfg.BotClanRequestsPerSecond,
		cfg.BotPlayerRequestsPerSecond,
		cfg.BasicPlayerRequestsPerSecond,
		cfg.LeaderboardRequestsPerSecond,
	)
	if rate <= 0 {
		return 100
	}
	return rate * 3
}

func needsClashClient(cfg Config) bool {
	switch cfg.Script {
	case "globalclans", "botplayers", "basicplayers", "botclans", "wars", "scheduled", "battlelogs", "leaderboards":
		return true
	default:
		return false
	}
}
