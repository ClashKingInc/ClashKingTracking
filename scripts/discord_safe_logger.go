package scripts

import (
	"context"
	"log/slog"
	"regexp"
)

// disgo logs rate-limit endpoints, which include incoming-webhook secrets.
// Redact them at the SDK boundary, including attributes bound with WithAttrs.
var discordWebhookURLPattern = regexp.MustCompile(`(/webhooks/[0-9]+/)[^/?\s]+`)

type discordSafeLogHandler struct{ inner slog.Handler }

func (h discordSafeLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}
func discordSafeAttr(attr slog.Attr) slog.Attr {
	attr.Value = attr.Value.Resolve()
	switch attr.Value.Kind() {
	case slog.KindString:
		attr.Value = slog.StringValue(discordWebhookURLPattern.ReplaceAllString(attr.Value.String(), "${1}[REDACTED]"))
	case slog.KindGroup:
		group := attr.Value.Group()
		clean := make([]slog.Attr, len(group))
		for i, a := range group {
			clean[i] = discordSafeAttr(a)
		}
		attr.Value = slog.GroupValue(clean...)
	}
	return attr
}
func (h discordSafeLogHandler) Handle(ctx context.Context, r slog.Record) error {
	safe := slog.NewRecord(r.Time, r.Level, discordWebhookURLPattern.ReplaceAllString(r.Message, "${1}[REDACTED]"), r.PC)
	r.Attrs(func(a slog.Attr) bool { safe.AddAttrs(discordSafeAttr(a)); return true })
	return h.inner.Handle(ctx, safe)
}
func (h discordSafeLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		clean[i] = discordSafeAttr(a)
	}
	return discordSafeLogHandler{h.inner.WithAttrs(clean)}
}
func (h discordSafeLogHandler) WithGroup(name string) slog.Handler {
	return discordSafeLogHandler{h.inner.WithGroup(name)}
}
func discordSafeLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return slog.New(discordSafeLogHandler{logger.Handler()})
}
