package scripts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"

	"github.com/disgoorg/disgo/rest"
	"github.com/disgoorg/snowflake/v2"
	valkey "github.com/valkey-io/valkey-go"
)

type deliveryErrorRecorder struct {
	seen     map[string]bool
	captured []string
}

func (r *deliveryErrorRecorder) Capture(err error, tags map[string]string) bool {
	key := err.Error() + ":" + tags["category"]
	if r.seen[key] {
		return false
	}
	r.seen[key] = true
	r.captured = append(r.captured, key)
	return true
}

func (*deliveryErrorRecorder) Close(context.Context) error { return nil }

func TestDiscordEventLogTypes(t *testing.T) {
	tests := []struct {
		event mobileWarEvent
		want  string
	}{
		{event: mobileWarEvent{Topic: "clan", Value: map[string]any{"type": "member_join"}}, want: "join_log"},
		{event: mobileWarEvent{Topic: "clan", Value: map[string]any{"type": "member_leave"}}, want: "leave_log"},
		{event: mobileWarEvent{Topic: "capital", Value: map[string]any{"type": "raid_attacks"}}, want: "capital_attacks"},
		{event: mobileWarEvent{Topic: "reddit", Value: map[string]any{}}, want: "reddit_feed"},
	}
	for _, test := range tests {
		got := discordEventLogTypes(test.event)
		if len(got) != 1 || got[0] != test.want {
			t.Fatalf("discordEventLogTypes(%+v) = %v, want %s", test.event, got, test.want)
		}
	}
}

func TestDiscordDeliveryAcknowledgesFailedAttemptWithoutRetry(t *testing.T) {
	deliveryErr := errors.New("temporary Discord failure")
	processed, acknowledged := 0, 0
	worker := &discordDeliveryWorker{
		app: &platform.App{
			Config: platform.Config{EventStreamRetentionSeconds: 300},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		processEventFn: func(context.Context, mobileWarEvent) (bool, error) {
			processed++
			return true, deliveryErr
		},
		ackFn: func(_ context.Context, id string) error {
			if id != "1-0" {
				t.Fatalf("acknowledged stream ID = %q", id)
			}
			acknowledged++
			return nil
		},
	}
	entry := valkey.XRangeEntry{ID: "1-0", FieldValues: map[string]string{
		"topic": "war", "clan_tag": "#AAA", "timestamp": time.Now().UTC().Format(time.RFC3339Nano), "value": `{"type":"new_war"}`,
	}}

	if err := worker.processEntries(t.Context(), []valkey.XRangeEntry{entry}); err != nil {
		t.Fatalf("process entries: %v", err)
	}
	if processed != 1 || acknowledged != 1 {
		t.Fatalf("processed = %d, acknowledged = %d; want one best-effort attempt and one acknowledgement", processed, acknowledged)
	}
}

func TestDiscordDeliveryDropsExpiredEvent(t *testing.T) {
	processed, acknowledged := 0, 0
	worker := &discordDeliveryWorker{
		app: &platform.App{
			Config: platform.Config{EventStreamRetentionSeconds: 300},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		processEventFn: func(context.Context, mobileWarEvent) (bool, error) {
			processed++
			return true, nil
		},
		ackFn: func(context.Context, string) error { acknowledged++; return nil },
	}
	entry := valkey.XRangeEntry{ID: "2-0", FieldValues: map[string]string{
		"topic": "reminder", "timestamp": time.Now().UTC().Add(-10 * time.Minute).Format(time.RFC3339Nano), "value": `{"type":"war"}`,
	}}

	if err := worker.processEntries(t.Context(), []valkey.XRangeEntry{entry}); err != nil {
		t.Fatalf("process entries: %v", err)
	}
	if processed != 0 || acknowledged != 1 {
		t.Fatalf("processed = %d, acknowledged = %d; expired delivery must be dropped and acknowledged", processed, acknowledged)
	}
}

func TestDiscordDeliveryReportsMalformedPayloadOnceAndAcknowledgesEachEntry(t *testing.T) {
	reporter := &deliveryErrorRecorder{seen: map[string]bool{}}
	acknowledged := 0
	worker := &discordDeliveryWorker{
		app: &platform.App{
			Config: platform.Config{EventStreamRetentionSeconds: 300},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			Errors: reporter,
		},
		ackFn: func(context.Context, string) error { acknowledged++; return nil },
	}
	entries := []valkey.XRangeEntry{
		{ID: "1-0", FieldValues: map[string]string{"topic": "war", "value": "{"}},
		{ID: "2-0", FieldValues: map[string]string{"topic": "war", "value": "{"}},
	}
	if err := worker.processEntries(t.Context(), entries); err != nil {
		t.Fatal(err)
	}
	if acknowledged != 2 || len(reporter.captured) != 1 {
		t.Fatalf("acknowledged=%d captures=%v, want two ACKs and one suppressed signature", acknowledged, reporter.captured)
	}
}

func TestDiscordDeliveryReportsOnlyInvalidProviderRequests(t *testing.T) {
	reporter := &deliveryErrorRecorder{seen: map[string]bool{}}
	processed := 0
	worker := &discordDeliveryWorker{
		app: &platform.App{
			Config: platform.Config{EventStreamRetentionSeconds: 300},
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
			Errors: reporter,
		},
		processEventFn: func(context.Context, mobileWarEvent) (bool, error) {
			processed++
			if processed == 1 {
				return true, &rest.Error{Code: rest.JSONErrorCodeAPIResourceOverloaded}
			}
			return true, &rest.Error{Code: rest.JSONErrorCodeInvalidFormBody}
		},
		ackFn: func(context.Context, string) error { return nil },
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	entries := []valkey.XRangeEntry{
		{ID: "1-0", FieldValues: map[string]string{"topic": "war", "timestamp": now, "value": `{}`}},
		{ID: "2-0", FieldValues: map[string]string{"topic": "war", "timestamp": now, "value": `{}`}},
	}
	if err := worker.processEntries(t.Context(), entries); err != nil {
		t.Fatal(err)
	}
	if len(reporter.captured) != 1 || !strings.Contains(reporter.captured[0], "invalid_provider_request") {
		t.Fatalf("unexpected captures: %v", reporter.captured)
	}
}

func TestDiscordDeliveryAttemptsDestinationsIndependently(t *testing.T) {
	attempted := []string{}
	err := attemptDiscordDestinations([]string{"first", "second", "third"}, func(destination string) error {
		attempted = append(attempted, destination)
		if destination == "first" || destination == "third" {
			return errors.New(destination + " failed")
		}
		return nil
	})
	if strings.Join(attempted, ",") != "first,second,third" {
		t.Fatalf("attempted destinations = %v", attempted)
	}
	if err == nil || !strings.Contains(err.Error(), "first failed") || !strings.Contains(err.Error(), "third failed") {
		t.Fatalf("combined delivery error = %v", err)
	}
}

func TestReminderMemberTagsAndTextUseOnlyExplicitMentions(t *testing.T) {
	event := mobileWarEvent{Topic: "reminder", Value: map[string]any{
		"type": "clan_games",
		"members": []any{
			map[string]any{"tag": "#AAA", "name": "A"},
			map[string]any{"tag": "#BBB", "name": "B"},
		},
	}}
	if got := reminderMemberTags(event); len(got) != 2 || got[0] != "#AAA" || got[1] != "#BBB" {
		t.Fatalf("reminderMemberTags() = %v", got)
	}
	text := reminderText(event, 60, "Finish your games", []snowflake.ID{123})
	for _, fragment := range []string{"Clan Games", "60 minutes remaining", "Finish your games", "<@123>"} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("reminderText() = %q, missing %q", text, fragment)
		}
	}
}

func TestValidateDiscordDeliveryConfig(t *testing.T) {
	cfg := platform.Config{}
	if err := validateDiscordDeliveryConfig(cfg, nil); err == nil {
		t.Fatal("expected incomplete configuration to fail")
	}
}

func TestPermanentDiscordDestinationFailureUsesSanitizedClassification(t *testing.T) {
	tests := []struct {
		code      rest.JSONErrorCode
		want      string
		permanent bool
	}{
		{rest.JSONErrorCodeUnknownChannel, "discord_unknown_channel", true},
		{rest.JSONErrorCodeUnknownWebhook, "discord_invalid_webhook", true},
		{rest.JSONErrorCodeMissingAccess, "discord_missing_access", true},
		{rest.JSONErrorCode(50013), "discord_missing_permissions", true},
		{rest.JSONErrorCodeAPIResourceOverloaded, "", false},
	}
	for _, test := range tests {
		reason, permanent := permanentDiscordDestinationFailure(fmt.Errorf("provider request failed: %w", &rest.Error{Code: test.code, Message: "raw provider detail must not be stored"}))
		if reason != test.want || permanent != test.permanent {
			t.Fatalf("code %d classified as (%q, %v), want (%q, %v)", test.code, reason, permanent, test.want, test.permanent)
		}
		if strings.Contains(reason, "raw provider") {
			t.Fatalf("classification leaked provider text: %q", reason)
		}
	}
	if reason, permanent := permanentDiscordDestinationFailure(errors.New("network timeout")); reason != "" || permanent {
		t.Fatalf("temporary network failure classified as permanent: (%q, %v)", reason, permanent)
	}
}

func TestPermanentDestinationDisableIsFencedByObservedRevision(t *testing.T) {
	current := time.Date(2026, 9, 6, 12, 0, 0, 123000000, time.UTC)
	affected := []int64{}
	worker := &discordDeliveryWorker{disableFn: func(_ context.Context, target discordDeliveryTarget, reason string) (int64, error) {
		if reason != "discord_unknown_channel" {
			t.Fatalf("disable reason = %q", reason)
		}
		rows := int64(0)
		if target.Revision.Equal(current) {
			rows = 1
		}
		affected = append(affected, rows)
		return rows, nil
	}}
	permanent := &rest.Error{Code: rest.JSONErrorCodeUnknownChannel}
	old := discordDeliveryTarget{Config: "reminder", ConfigID: "one", Revision: current.Add(-time.Second)}
	unchanged := discordDeliveryTarget{Config: "reminder", ConfigID: "one", Revision: current}
	_ = worker.handleDestinationFailure(t.Context(), old, permanent)
	_ = worker.handleDestinationFailure(t.Context(), unchanged, permanent)
	if len(affected) != 2 || affected[0] != 0 || affected[1] != 1 {
		t.Fatalf("revision-fenced affected rows = %v, want [0 1]", affected)
	}
}
