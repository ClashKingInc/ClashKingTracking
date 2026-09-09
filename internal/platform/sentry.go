package platform

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
)

const (
	sentrySignatureCooldown = 5 * time.Minute
	sentrySignatureLimit    = 512
)

type ErrorReporter interface {
	Capture(error, map[string]string) bool
	Close(context.Context) error
}

type sentryReporter struct {
	client *sentry.Client
	now    func() time.Time
	mu     sync.Mutex
	seen   map[string]time.Time
}

var sentrySensitivePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(?:https?|postgres(?:ql)?|redis|rediss)://\S+`),
	regexp.MustCompile(`(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b`),
	regexp.MustCompile(`#[0289PYLQGRJCUV]{3,15}`),
	regexp.MustCompile(`\b[0-9]{6,}\b`),
	regexp.MustCompile(`\b[A-Za-z0-9_=-]{24,}\b`),
}

func newErrorReporter(cfg Config) (ErrorReporter, error) {
	if cfg.SentryDSN == "" {
		return nil, nil
	}
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              cfg.SentryDSN,
		Environment:      cfg.SentryEnvironment,
		Release:          cfg.SentryRelease,
		AttachStacktrace: true,
		SendDefaultPII:   false,
		EnableTracing:    false,
		TracesSampleRate: 0,
		EnableLogs:       false,
		MaxBreadcrumbs:   0,
		BeforeSend:       scrubSentryEvent,
	})
	if err != nil {
		return nil, fmt.Errorf("initialize Sentry: %w", err)
	}
	return &sentryReporter{client: client, now: time.Now, seen: map[string]time.Time{}}, nil
}

func (r *sentryReporter) Capture(err error, tags map[string]string) bool {
	if r == nil || r.client == nil || err == nil {
		return false
	}
	signature := sentryErrorSignature(err)
	if !r.accept(signature) {
		return false
	}
	scope := sentry.NewScope()
	for key, value := range tags {
		key, value = strings.TrimSpace(key), scrubSentryText(value)
		if key != "" && value != "" {
			scope.SetTag(key, value)
		}
	}
	scope.SetFingerprint([]string{signature})
	return r.client.CaptureException(err, &sentry.EventHint{OriginalException: err}, scope) != nil
}

func (r *sentryReporter) accept(signature string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now().UTC()
	if previous, ok := r.seen[signature]; ok && now.Sub(previous) < sentrySignatureCooldown {
		return false
	}
	if len(r.seen) >= sentrySignatureLimit {
		type observation struct {
			signature string
			at        time.Time
		}
		oldest := make([]observation, 0, len(r.seen))
		for key, at := range r.seen {
			oldest = append(oldest, observation{signature: key, at: at})
		}
		sort.Slice(oldest, func(i, j int) bool { return oldest[i].at.Before(oldest[j].at) })
		delete(r.seen, oldest[0].signature)
	}
	r.seen[signature] = now
	return true
}

func (r *sentryReporter) Close(ctx context.Context) error {
	if r == nil || r.client == nil {
		return nil
	}
	if !r.client.FlushWithContext(ctx) {
		return errors.New("Sentry flush deadline exceeded")
	}
	return nil
}

func sentryErrorSignature(err error) string {
	value := fmt.Sprintf("%T:%s", err, scrubSentryText(err.Error()))
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:12])
}

func scrubSentryText(value string) string {
	value = strings.TrimSpace(value)
	for _, pattern := range sentrySensitivePatterns {
		value = pattern.ReplaceAllString(value, "[redacted]")
	}
	if len(value) > 512 {
		value = value[:512]
	}
	return value
}

func scrubSentryEvent(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
	if event == nil {
		return nil
	}
	event.Message = scrubSentryText(event.Message)
	for index := range event.Exception {
		event.Exception[index].Value = scrubSentryText(event.Exception[index].Value)
	}
	event.Request = nil
	event.User = sentry.User{}
	event.Breadcrumbs = nil
	event.Extra = nil
	event.Contexts = nil
	return event
}
