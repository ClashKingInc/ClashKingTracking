package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"strings"
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
	// Keep the script registry explicit so valid entrypoints are easy to audit.
	domains := []platform.Domain{
		scriptdomains.NewGlobalClansDomain(),
		scriptdomains.NewBattlelogsDomain(),
		scriptdomains.NewBotPlayersDomain(),
		scriptdomains.NewBotClansDomain(),
		scriptdomains.NewWarsDomain(),
		scriptdomains.NewScheduledDomain(),
		scriptdomains.NewRedditDomain(),
		scriptdomains.NewGiveawaysDomain(),
		scriptdomains.NewEventsDomain(),
		scriptdomains.NewMobilePushDomain(),
	}

	selected, err := selectedDomain(cfg.Script, domains)
	if err != nil {
		log.Fatal(err)
	}
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
	if err := platform.Run(ctx, app, []platform.Domain{selected}); err != nil {
		log.Fatal(err)
	}
}

func selectedDomain(script string, domains []platform.Domain) (platform.Domain, error) {
	names := make(map[string]bool, len(domains))
	for _, domain := range domains {
		names[domain.Name()] = true
	}
	if script == "" {
		return nil, fmt.Errorf("--script is required; valid scripts: %s", domainNames(domains))
	}
	if !names[script] {
		return nil, fmt.Errorf("unknown script %q; valid scripts: %s", script, domainNames(domains))
	}
	for _, domain := range domains {
		if domain.Name() == script {
			return domain, nil
		}
	}
	return nil, fmt.Errorf("unknown script %q; valid scripts: %s", script, domainNames(domains))
}

func domainNames(domains []platform.Domain) string {
	names := make([]string, 0, len(domains))
	for _, domain := range domains {
		names = append(names, domain.Name())
	}
	return strings.Join(names, ", ")
}
