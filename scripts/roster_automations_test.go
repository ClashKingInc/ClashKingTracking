//go:build script_internal_tests

package scripts

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"
)

func TestRosterAutomationCyclePublishesExactTimeExecution(t *testing.T) {
	scheduledAt := time.Date(2026, 9, 8, 18, 0, 0, 0, time.UTC)
	store := &memoryRosterAutomationStore{due: []models.RosterAutomationExecution{{
		ExecutionID:      rosterAutomationExecutionID("auto", "roster", scheduledAt),
		AutomationID:     "auto",
		ServerID:         "123",
		RosterID:         "roster",
		ActionType:       "roster_post",
		ScheduledAt:      scheduledAt,
		DiscordChannelID: "456",
		WebhookID:        "789",
		MessageID:        "1011",
		Attempt:          1,
	}}}
	domain := &rosterAutomationsDomain{store: store}
	var event platform.Event
	domain.publish = func(_ context.Context, got platform.Event) error {
		event = got
		return nil
	}
	app := &platform.App{
		Config: platform.Config{RosterAutomationBatchSize: 10},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Stats:  platform.NewTracker(),
	}
	if err := domain.runCycle(t.Context(), app, scheduledAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if event.Topic != "roster_automation" || event.Value["scheduled_at"] != scheduledAt.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected event: %+v", event)
	}
	if event.Value["webhook_id"] != "789" || event.Value["message_id"] != "1011" {
		t.Fatalf("canonical webhook binding missing: %+v", event.Value)
	}
	if len(store.dispatched) != 1 || store.dispatched[0] != "auto:roster:1788890400" {
		t.Fatalf("dispatched = %v", store.dispatched)
	}
}

func TestRosterAutomationCycleRetriesPublishFailure(t *testing.T) {
	store := &memoryRosterAutomationStore{due: []models.RosterAutomationExecution{{ExecutionID: "execution", Attempt: 2}}}
	domain := &rosterAutomationsDomain{store: store, publish: func(context.Context, platform.Event) error {
		return errors.New("stream unavailable")
	}}
	app := &platform.App{
		Config: platform.Config{RosterAutomationBatchSize: 10},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Stats:  platform.NewTracker(),
	}
	if err := domain.runCycle(t.Context(), app, time.Now().UTC()); err == nil {
		t.Fatal("publish failure should be returned")
	}
	if len(store.retried) != 1 || len(store.dispatched) != 0 {
		t.Fatalf("retried=%v dispatched=%v", store.retried, store.dispatched)
	}
}

func TestRosterAutomationRetryDelayIsBounded(t *testing.T) {
	if got := rosterAutomationRetryDelay(1); got != 15*time.Second {
		t.Fatalf("first retry = %s", got)
	}
	if got := rosterAutomationRetryDelay(99); got != 15*time.Minute {
		t.Fatalf("bounded retry = %s", got)
	}
}
