package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"clashking_tracking/internal/platform"
	scriptdomains "clashking_tracking/scripts"
)

func main() {
	// Trap SIGINT/SIGTERM so all domains and background jobs can stop cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := platform.Load()
	app, err := platform.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		// Give the HTTP server, scheduler, and stores a bounded window to shut down.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = app.Close(shutdownCtx)
	}()

	// Keep the domain registry explicit so startup order and enabled domains are obvious.
	domains := []platform.Domain{
		scriptdomains.NewGlobalClansDomain(),
		scriptdomains.NewBotPlayersDomain(),
		scriptdomains.NewBotClansDomain(),
		scriptdomains.NewWarsDomain(),
		scriptdomains.NewScheduledDomain(),
		scriptdomains.NewRedditDomain(),
		scriptdomains.NewGiveawaysDomain(),
		scriptdomains.NewEventsDomain(),
	}

	var enabled []platform.Domain
	// `--domains` narrows this list; otherwise all registered domains run.
	for _, domain := range domains {
		if cfg.Enabled(domain.Name()) {
			enabled = append(enabled, domain)
		}
	}
	if err := platform.Run(ctx, app, enabled); err != nil {
		log.Fatal(err)
	}
}
