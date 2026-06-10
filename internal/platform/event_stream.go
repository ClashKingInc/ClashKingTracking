package platform

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	valkey "github.com/valkey-io/valkey-go"
	"go.opentelemetry.io/otel/attribute"
)

// PublishEvent appends domain events to Valkey Streams for the independent events script.
func (a *App) PublishEvent(ctx context.Context, event Event) error {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if a.Valkey == nil || a.Config.EventStreamName == "" {
		return nil
	}
	return AppendEvent(ctx, a.Valkey, a.Config, event)
}

func AppendEvent(ctx context.Context, client valkey.Client, cfg Config, event Event) error {
	raw, err := json.Marshal(event.Value)
	if err != nil {
		return err
	}
	ctx, span := StartSpan(ctx, "valkey.events.xadd",
		attribute.String("operation", "xadd"),
		attribute.String("domain", "events"),
	)
	defer span.End()

	builder := client.B().Xadd().Key(cfg.EventStreamName)
	var cmd valkey.Completed
	if cfg.EventStreamRetentionSeconds > 0 {
		cmd = builder.Minid().Almost().Threshold(
			eventStreamMinID(time.Now().UTC(), cfg.EventStreamRetentionSeconds),
		).
			Id("*").FieldValue().
			FieldValue("topic", event.Topic).
			FieldValue("clan_tag", event.ClanTag).
			FieldValue("timestamp", event.Timestamp.UTC().Format(time.RFC3339Nano)).
			FieldValue("value", string(raw)).
			Build()
	} else {
		cmd = builder.Id("*").FieldValue().
			FieldValue("topic", event.Topic).
			FieldValue("clan_tag", event.ClanTag).
			FieldValue("timestamp", event.Timestamp.UTC().Format(time.RFC3339Nano)).
			FieldValue("value", string(raw)).
			Build()
	}
	err = client.Do(ctx, cmd).Error()
	RecordSpanError(span, err)
	span.SetAttributes(SpanErrorStatus(err))
	return err
}

func eventStreamMinID(now time.Time, retentionSeconds int) string {
	cutoff := now.UTC().Add(-time.Duration(retentionSeconds) * time.Second)
	return strconv.FormatInt(cutoff.UnixMilli(), 10) + "-0"
}
