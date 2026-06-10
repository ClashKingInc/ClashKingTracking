//go:build platform_internal_tests

package platform

import (
	"context"
	"testing"
)

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
