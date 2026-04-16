package platform

import (
	"context"
	"testing"
)

func TestNewRequiresProxyForClashDomains(t *testing.T) {
	_, err := New(context.Background(), Config{
		MockDB:  true,
		Domains: map[string]bool{"globalclans": true},
	})
	if err == nil {
		t.Fatal("expected missing PROXY_URL error for Clash-backed domains")
	}
}

func TestNewSkipsClashClientForNonClashDomains(t *testing.T) {
	app, err := New(context.Background(), Config{
		MockDB:  true,
		Domains: map[string]bool{"events": true},
	})
	if err != nil {
		t.Fatalf("New returned error: %v", err)
	}
	if app.Clash != nil {
		t.Fatal("expected Clash client to be nil when no Clash-backed domains are enabled")
	}
}
