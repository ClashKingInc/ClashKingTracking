//go:build utils_internal_tests

package utils

import "testing"

func TestSplit(t *testing.T) {
	// The helper should preserve order while leaving a shorter tail batch when needed.
	got := Split([]int{1, 2, 3, 4, 5}, 2)
	if len(got) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(got))
	}
	if len(got[2]) != 1 || got[2][0] != 5 {
		t.Fatalf("unexpected final batch: %#v", got[2])
	}
}
