package platform

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

var appendEventOnceScript = valkey.NewLuaScript(`
	if redis.call('EXISTS', KEYS[2]) == 1 then
		return 0
	end
	if ARGV[1] ~= '' then
		redis.call('XADD', KEYS[1], 'MINID', '~', ARGV[1], '*',
			'topic', ARGV[2], 'clan_tag', ARGV[3], 'timestamp', ARGV[4], 'value', ARGV[5])
	else
		redis.call('XADD', KEYS[1], '*',
			'topic', ARGV[2], 'clan_tag', ARGV[3], 'timestamp', ARGV[4], 'value', ARGV[5])
	end
	redis.call('SET', KEYS[2], '1', 'EX', ARGV[6])
	return 1
`)

// PublishEvent appends domain events to Valkey Streams for the independent events script.
func (a *App) PublishEvent(ctx context.Context, event Event) error {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if a.Valkey == nil || a.Config.EventStreamName == "" {
		if a.Config.DryRun || a.Config.MockDB {
			return nil
		}
		return errors.New("Valkey and events.stream are required to publish events")
	}
	return AppendEvent(ctx, a.Valkey, a.Config, event)
}

func AppendEvent(ctx context.Context, client valkey.Client, cfg Config, event Event) error {
	raw, err := json.Marshal(event.Value)
	if err != nil {
		return err
	}
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
	return client.Do(ctx, cmd).Error()
}

// AppendEventOnce atomically appends an event and reserves its dedupe identity.
// A retry with the same identity succeeds without adding a second stream item.
func AppendEventOnce(ctx context.Context, client valkey.Client, cfg Config, dedupeID string, ttl time.Duration, event Event) (bool, error) {
	if client == nil {
		return false, errors.New("Valkey is required to publish a deduplicated event")
	}
	if cfg.EventStreamName == "" {
		return false, errors.New("events.stream is required to publish a deduplicated event")
	}
	if dedupeID == "" {
		return false, errors.New("event dedupe identity is required")
	}
	if ttl <= 0 {
		return false, errors.New("event dedupe TTL must be greater than zero")
	}
	raw, err := json.Marshal(event.Value)
	if err != nil {
		return false, err
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	minID := ""
	if cfg.EventStreamRetentionSeconds > 0 {
		minID = eventStreamMinID(time.Now().UTC(), cfg.EventStreamRetentionSeconds)
	}
	seconds := int64(math.Ceil(ttl.Seconds()))
	result, err := appendEventOnceScript.Exec(ctx, client,
		[]string{cfg.EventStreamName, cfg.EventStreamName + ":dedupe:" + dedupeID},
		[]string{minID, event.Topic, event.ClanTag, event.Timestamp.UTC().Format(time.RFC3339Nano), string(raw), strconv.FormatInt(seconds, 10)},
	).AsInt64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func eventStreamMinID(now time.Time, retentionSeconds int) string {
	cutoff := now.UTC().Add(-time.Duration(retentionSeconds) * time.Second)
	return strconv.FormatInt(cutoff.UnixMilli(), 10) + "-0"
}
