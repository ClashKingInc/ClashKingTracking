package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	clashy "github.com/clashkinginc/clashy.go"
	valkey "github.com/valkey-io/valkey-go"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type Domain interface {
	Name() string
	Run(context.Context, *App) error
}

type App struct {
	Config         Config
	Logger         *slog.Logger
	Store          Store
	Valkey         valkey.Client
	Clash          *clashy.Client
	R2             ObjectStore
	Stats          *Tracker
	Scheduler      *Scheduler
	tracerProvider *sdktrace.TracerProvider
	httpSrv        *http.Server
}

func New(ctx context.Context, cfg Config) (*App, error) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	tracerProvider, err := newTracerProvider(ctx, cfg)
	if err != nil {
		return nil, err
	}
	store, err := newStore(ctx, cfg)
	if err != nil {
		if tracerProvider != nil {
			_ = tracerProvider.Shutdown(ctx)
		}
		return nil, err
	}
	if needsClashClient(cfg) && cfg.ProxyURL == "" {
		return nil, errors.New("proxy_url is required when Clash-backed domains are enabled")
	}
	var valkeyClient valkey.Client
	if cfg.ValkeyAddr != "" {
		valkeyClient, err = valkey.NewClient(valkey.ClientOption{
			InitAddress:  []string{cfg.ValkeyAddr},
			Password:     cfg.ValkeyPassword,
			DisableCache: true,
		})
		if err != nil {
			_ = store.Close(ctx)
			if tracerProvider != nil {
				_ = tracerProvider.Shutdown(ctx)
			}
			return nil, err
		}
	}
	stats := NewTracker()
	var clashClient *clashy.Client
	if needsClashClient(cfg) {
		clashClient, err = clashy.NewClient(clashy.ClientConfig{
			BaseURL:     cfg.ProxyURL,
			LookupCache: false,
			UpdateCache: false,
		})
		if err != nil {
			if valkeyClient != nil {
				valkeyClient.Close()
			}
			_ = store.Close(ctx)
			if tracerProvider != nil {
				_ = tracerProvider.Shutdown(ctx)
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
			_ = store.Close(ctx)
			if tracerProvider != nil {
				_ = tracerProvider.Shutdown(ctx)
			}
			return nil, err
		}
	} else if cfg.R2MockUpload {
		objectStore = MockObjectStore{}
	}
	app := &App{
		Config:         cfg,
		Logger:         logger,
		Store:          store,
		Valkey:         valkeyClient,
		Clash:          clashClient,
		R2:             objectStore,
		Stats:          stats,
		Scheduler:      NewScheduler(),
		tracerProvider: tracerProvider,
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
	if a.Valkey != nil {
		a.Valkey.Close()
	}
	if a.Clash != nil {
		_ = a.Clash.Close()
	}
	if err := a.httpSrv.Shutdown(ctx); err != nil {
		return err
	}
	if err := a.Store.Close(ctx); err != nil {
		return err
	}
	if a.tracerProvider != nil {
		return a.tracerProvider.Shutdown(ctx)
	}
	return nil
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
	switch cfg.Script {
	case "globalclans", "botplayers", "botclans", "wars", "scheduled", "battlelogs":
		return true
	default:
		return false
	}
}
