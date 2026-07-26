//go:build platform_internal_tests

package platform

import (
	"context"
	"errors"
	"testing"
)

type runTestDomain struct {
	name string
	run  func(context.Context) error
}

func (d runTestDomain) Name() string { return d.name }

func (d runTestDomain) Run(ctx context.Context, _ *App) error { return d.run(ctx) }

func TestRunCancelsSiblingDomainsAfterFailure(t *testing.T) {
	stopped := make(chan struct{})
	app := &App{Scheduler: NewScheduler()}
	err := Run(t.Context(), app, []Domain{
		runTestDomain{name: "failed", run: func(context.Context) error {
			return errors.New("failed")
		}},
		runTestDomain{name: "blocking", run: func(ctx context.Context) error {
			<-ctx.Done()
			close(stopped)
			return ctx.Err()
		}},
	})
	if err == nil {
		t.Fatal("Run should return the domain failure")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("sibling domain was not stopped before Run returned")
	}
}

func TestNewRequiresProxyForClashDomains(t *testing.T) {
	_, err := New(context.Background(), Config{
		MockDB: true,
		Script: "globalclans",
	})
	if err == nil {
		t.Fatal("expected missing proxy_url error for Clash-backed domains")
	}
}

func TestNewSkipsClashClientForNonClashDomains(t *testing.T) {
	app, err := New(context.Background(), Config{
		MockDB: true,
		Script: "events",
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if app.Clash != nil {
		t.Fatal("expected Clash client to be nil when no Clash-backed domains are enabled")
	}
}

func TestProxyConnectionLimitUsesLargestRequestRate(t *testing.T) {
	cfg := Config{
		GlobalClanPriorityRequestsPerSecond:    950,
		GlobalClanNonPriorityRequestsPerSecond: 50,
		BattlelogRequestsPerSecond:             10,
		WarRequestsPerSecond:                   950,
		BotClanRequestsPerSecond:               950,
		BotPlayerRequestsPerSecond:             950,
	}

	if got, want := proxyConnectionLimit(cfg), 3000; got != want {
		t.Fatalf("proxyConnectionLimit = %d, want %d", got, want)
	}
}

func TestProxyConnectionLimitFallsBackWhenRatesMissing(t *testing.T) {
	if got, want := proxyConnectionLimit(Config{}), 100; got != want {
		t.Fatalf("proxyConnectionLimit = %d, want %d", got, want)
	}
}
