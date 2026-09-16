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
	Config       Config
	Logger       *slog.Logger
	Valkey       valkey.Client
	Clash        *clashy.Client
	Stats        *Tracker
	StatsWriter  *TimescaleStatsWriter
	Availability *AvailabilityGate
	Errors       ErrorReporter
}

func New(ctx context.Context, cfg Config) (*App, error) {
	statsDSN := cfg.TimescaleURL
	if cfg.TimescaleURL != "" {
		var err error
		statsDSN, err = cfg.DatabasePools.connectionString(cfg.TimescaleURL, cfg.Script, true)
		if err != nil {
			return nil, err
		}
		cfg.TimescaleURL, err = cfg.DatabasePools.connectionString(cfg.TimescaleURL, cfg.Script, false)
		if err != nil {
			return nil, err
		}
	}
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
		statsWriter, err = NewTimescaleStatsWriter(ctx, statsDSN, stats, cfg.Script, time.Duration(cfg.StatsTimescaleFlushSeconds)*time.Second)
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
	reporter, err := newErrorReporter(cfg)
	if err != nil {
		if valkeyClient != nil {
			valkeyClient.Close()
		}
		if clashClient != nil {
			_ = clashClient.Close()
		}
		if statsWriter != nil {
			statsWriter.Close()
		}
		return nil, err
	}
	app := &App{
		Config:       cfg,
		Logger:       logger,
		Valkey:       valkeyClient,
		Clash:        clashClient,
		Stats:        stats,
		StatsWriter:  statsWriter,
		Availability: NewAvailabilityGate(valkeyClient),
		Errors:       reporter,
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
	if a.Errors != nil {
		return a.Errors.Close(ctx)
	}
	return nil
}

func Run(ctx context.Context, app *App, domains []Domain) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if app.StatsWriter != nil {
		go app.StatsWriter.Run(runCtx, app.Logger)
	}
	if app.Availability != nil {
		go app.Availability.Run(runCtx)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, len(domains))
	for _, domain := range domains {
		domain := domain
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := domain.Run(runCtx, app); err != nil && !errors.Is(err, context.Canceled) {
				wrapped := fmt.Errorf("%s: %w", domain.Name(), err)
				if app.Errors != nil {
					app.Errors.Capture(wrapped, map[string]string{"script": app.Config.Script, "domain": domain.Name()})
				}
				errCh <- wrapped
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
		cfg.WarDiscoveryActiveRequestsPerSecond+cfg.WarDiscoveryDormantRequestsPerSecond,
		cfg.CWLRequestsPerSecond+cfg.CWLWarRequestsPerSecond,
		cfg.WarArchiveRequestsPerSecond,
		cfg.TrackedClanRequestsPerSecond,
		cfg.TrackedPlayerRequestsPerSecond,
		cfg.BasicPlayerRequestsPerSecond,
		cfg.ScheduledRequestsPerSecond,
		cfg.ReminderRequestsPerSecond,
	)
	if rate <= 0 {
		return 100
	}
	return rate * 3
}

func needsClashClient(cfg Config) bool {
	switch cfg.Script {
	case "globalclans", "trackedplayers", "basicplayers", "trackedclans", "war-discovery", "cwl", "war-archiver", "capital", "reminders", "availability", "scheduled", "battlelogs", "leaderboards", "notifications":
		return true
	default:
		return false
	}
}
