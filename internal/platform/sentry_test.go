package platform

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

type recordingErrorReporter struct {
	err  error
	tags map[string]string
}

func (r *recordingErrorReporter) Capture(err error, tags map[string]string) bool {
	r.err, r.tags = err, tags
	return true
}

func (*recordingErrorReporter) Close(context.Context) error { return nil }

type failingTestDomain struct{}

func (failingTestDomain) Name() string                    { return "failing" }
func (failingTestDomain) Run(context.Context, *App) error { return errors.New("unexpected failure") }

func TestSentryIsOffWithoutDSN(t *testing.T) {
	reporter, err := newErrorReporter(Config{})
	if err != nil || reporter != nil {
		t.Fatalf("empty DSN reporter = %T, err = %v; want disabled", reporter, err)
	}
}

func TestSentryEnvironmentConfigurationIncludesReleaseMetadata(t *testing.T) {
	t.Setenv("SENTRY_DSN", "https://public@example.com/1")
	t.Setenv("SENTRY_ENVIRONMENT", "development")
	t.Setenv("SENTRY_RELEASE", "tracking@abc123")
	var cfg Config
	applyEnvironment(&cfg)
	if cfg.SentryDSN == "" || cfg.SentryEnvironment != "development" || cfg.SentryRelease != "tracking@abc123" {
		t.Fatalf("Sentry environment config missing: %+v", cfg)
	}
}

func TestSentryScrubsSensitiveErrorData(t *testing.T) {
	raw := "request https://user:secret@example.com/path for #2VC0Q9LV user@example.com token abcdefghijklmnopqrstuvwxyz123456 id 123456789"
	scrubbed := scrubSentryText(raw)
	for _, secret := range []string{"secret", "#2VC0Q9LV", "user@example.com", "abcdefghijklmnopqrstuvwxyz123456", "123456789"} {
		if strings.Contains(scrubbed, secret) {
			t.Fatalf("scrubbed text retained %q: %q", secret, scrubbed)
		}
	}
	event := scrubSentryEvent(&sentry.Event{
		Message:  raw,
		User:     sentry.User{ID: "123"},
		Extra:    map[string]any{"payload": raw},
		Contexts: map[string]map[string]any{"request": {"url": raw}},
	}, nil)
	if event.User.ID != "" || event.Extra != nil || event.Contexts != nil || strings.Contains(event.Message, "secret") {
		t.Fatalf("Sentry event was not privacy scrubbed: %#v", event)
	}
}

func TestSentrySuppressesRepeatedSignatureForFiveMinutes(t *testing.T) {
	now := time.Date(2026, 9, 6, 12, 0, 0, 0, time.UTC)
	reporter := &sentryReporter{now: func() time.Time { return now }, seen: map[string]time.Time{}}
	signature := sentryErrorSignature(errors.New("database 123456 failed"))
	if !reporter.accept(signature) || reporter.accept(signature) {
		t.Fatal("first signature should pass and immediate duplicate should be suppressed")
	}
	now = now.Add(sentrySignatureCooldown)
	if !reporter.accept(signature) {
		t.Fatal("signature should pass after the suppression window")
	}
}

func TestSentrySignatureIgnoresSensitiveIdentifierValues(t *testing.T) {
	left := sentryErrorSignature(errors.New("guild 123456 failed for #2VC0Q9LV"))
	right := sentryErrorSignature(errors.New("guild 987654 failed for #VY2J0LL"))
	if left != right {
		t.Fatalf("sensitive identifiers split one error signature: %s != %s", left, right)
	}
}

func TestPlatformRunReportsOnlyTerminalDomainError(t *testing.T) {
	reporter := &recordingErrorReporter{}
	app := &App{Config: Config{Script: "test-script"}, Errors: reporter}
	err := Run(t.Context(), app, []Domain{failingTestDomain{}})
	if err == nil || reporter.err == nil {
		t.Fatal("terminal domain error was not returned and reported")
	}
	if reporter.tags["script"] != "test-script" || reporter.tags["domain"] != "failing" {
		t.Fatalf("terminal error tags = %#v", reporter.tags)
	}
}
