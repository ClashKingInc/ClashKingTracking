package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/redis/go-redis/v9"
)

type Domain interface {
	Name() string
	Run(context.Context, *App) error
}

type App struct {
	Config    Config
	Logger    *slog.Logger
	Store     Store
	Redis     *redis.Client
	Clash     *clashy.Client
	Bus       *Bus
	Stats     *Tracker
	Scheduler *Scheduler
	httpSrv   *http.Server
}

func New(ctx context.Context, cfg Config) (*App, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	store, err := newStore(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if needsClashClient(cfg) && cfg.ProxyURL == "" {
		return nil, errors.New("PROXY_URL is required when Clash-backed domains are enabled")
	}
	var redisClient *redis.Client
	if cfg.RedisAddr != "" {
		redisClient = redis.NewClient(&redis.Options{
			Addr:         cfg.RedisAddr,
			Password:     cfg.RedisPassword,
			DB:           0,
			PoolSize:     50,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
		})
	}
	bus := NewBus(cfg.EventBufferSize, cfg.RecentEventBuffer)
	stats := NewTracker(bus)
	var clashClient *clashy.Client
	if needsClashClient(cfg) {
		clashClient, err = clashy.NewClient(clashy.ClientConfig{
			BaseURL:     cfg.ProxyURL,
			LookupCache: false,
			UpdateCache: false,
		})
		if err != nil {
			return nil, err
		}
	}
	app := &App{
		Config:    cfg,
		Logger:    logger,
		Store:     store,
		Redis:     redisClient,
		Clash:     clashClient,
		Bus:       bus,
		Stats:     stats,
		Scheduler: NewScheduler(),
		httpSrv: &http.Server{
			Addr:    cfg.HTTPAddr,
			Handler: stats.HTTPMux(),
		},
	}
	return app, nil
}

func (a *App) StartHTTP() {
	go func() {
		if err := a.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.Logger.Error("http server exited", "err", err)
		}
	}()
}

func (a *App) Close(ctx context.Context) error {
	if a.Redis != nil {
		_ = a.Redis.Close()
	}
	if a.Clash != nil {
		_ = a.Clash.Close()
	}
	if err := a.httpSrv.Shutdown(ctx); err != nil {
		return err
	}
	return a.Store.Close(ctx)
}

func Run(ctx context.Context, app *App, domains []Domain) error {
	app.StartHTTP()
	schedulerCtx, cancelScheduler := context.WithCancel(ctx)
	defer cancelScheduler()
	go func() {
		// Keep delayed jobs alive in the same process as the domains that schedule them.
		_ = app.Scheduler.Run(schedulerCtx)
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, len(domains))
	for _, domain := range domains {
		domain := domain
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := domain.Run(ctx, app); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("%s: %w", domain.Name(), err)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func newStore(ctx context.Context, cfg Config) (Store, error) {
	// Dry-run and mock-db both use the in-memory store so writes can be exercised safely.
	if cfg.MockDB || cfg.DryRun || cfg.StatsMongoURI == "" || cfg.StaticMongoURI == "" {
		return NewMockStore(), nil
	}
	return NewMongoStore(ctx, cfg.StatsMongoURI, cfg.StaticMongoURI)
}

func needsClashClient(cfg Config) bool {
	for _, domain := range []string{"globalclans", "botplayers", "botclans", "wars", "scheduled"} {
		if cfg.Enabled(domain) {
			return true
		}
	}
	return false
}
