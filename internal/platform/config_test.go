package platform

import "testing"

func TestEnvBool(t *testing.T) {
	// Keep the accepted env spellings pinned down because CLI defaults flow through these helpers.
	t.Setenv("CHECK_BOOL", "true")
	if !envBool("CHECK_BOOL") {
		t.Fatal("expected envBool to parse true")
	}
}

func TestEnvInt(t *testing.T) {
	// Invalid parsing falls back, so this test guards the successful parse path explicitly.
	t.Setenv("CHECK_INT", "42")
	if got := envInt("CHECK_INT", 1); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}
