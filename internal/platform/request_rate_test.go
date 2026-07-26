//go:build platform_internal_tests

package platform

import "testing"

func TestRequestConcurrencyProvidesLatencyHeadroom(t *testing.T) {
	if got, want := RequestConcurrency(1000), 3000; got != want {
		t.Fatalf("RequestConcurrency(1000) = %d, want %d", got, want)
	}
	if got := RequestConcurrency(0); got != 0 {
		t.Fatalf("RequestConcurrency(0) = %d, want 0", got)
	}
}
