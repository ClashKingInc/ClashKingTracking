package main

import (
	"context"
	"strings"
	"testing"

	"clashking_tracking/internal/platform"
)

type testDomain string

func (d testDomain) Name() string {
	return string(d)
}

func (d testDomain) Run(context.Context, *platform.App) error {
	return nil
}

func TestSelectedDomainRejectsUnknownScript(t *testing.T) {
	_, err := selectedDomain("missing", []platform.Domain{
		testDomain("wars"),
		testDomain("battlelogs"),
	})
	if err == nil || !strings.Contains(err.Error(), `unknown script "missing"`) {
		t.Fatalf("err = %v, want unknown script error", err)
	}
}

func TestSelectedDomainReturnsMatchingScript(t *testing.T) {
	selected, err := selectedDomain("wars", []platform.Domain{
		testDomain("wars"),
		testDomain("battlelogs"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if selected.Name() != "wars" {
		t.Fatalf("selected = %+v, want wars", selected)
	}
}

func TestSelectedDomainRequiresScript(t *testing.T) {
	_, err := selectedDomain("", []platform.Domain{
		testDomain("wars"),
		testDomain("battlelogs"),
	})
	if err == nil || !strings.Contains(err.Error(), "--script is required") {
		t.Fatalf("err = %v, want required script error", err)
	}
}
