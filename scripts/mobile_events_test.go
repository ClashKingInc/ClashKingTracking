package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	valkey "github.com/valkey-io/valkey-go"
	"golang.org/x/oauth2"
)

type memoryLegendDeliveryMarkers struct{ delivered map[string]bool }

func (m *memoryLegendDeliveryMarkers) Delivered(_ context.Context, eventID, deviceID string) (bool, error) {
	return m.delivered[eventID+"\x00"+deviceID], nil
}

func (m *memoryLegendDeliveryMarkers) MarkDelivered(_ context.Context, eventID, deviceID string) error {
	m.delivered[eventID+"\x00"+deviceID] = true
	return nil
}

func TestLegendDefenseRetrySkipsDevicesAlreadyDelivered(t *testing.T) {
	fcmADC.Lock()
	previousSource := fcmADC.source
	fcmADC.source = oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "access-token"})
	fcmADC.Unlock()
	previousClient := pushHTTPClient
	t.Cleanup(func() {
		fcmADC.Lock()
		fcmADC.source = previousSource
		fcmADC.Unlock()
		pushHTTPClient = previousClient
	})

	requests := map[string]int{}
	pushHTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		var payload struct {
			Message struct {
				Token string `json:"token"`
			} `json:"message"`
		}
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		requests[payload.Message.Token]++
		status := http.StatusOK
		if payload.Message.Token == "token-b" && requests[payload.Message.Token] == 1 {
			status = http.StatusServiceUnavailable
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
	})}

	const key = "push-test-key"
	markers := &memoryLegendDeliveryMarkers{delivered: make(map[string]bool)}
	app := &platform.App{
		Config: platform.Config{MobilePushFCMProjectID: "test-project", MobilePushTokenKey: key},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	worker := &mobileEventsWorker{app: app, cfg: app.Config, logger: app.Logger, legendMarkers: markers}
	devices := []models.PushDevice{
		{UserID: "user-a", DeviceID: "shared", Environment: "production", Provider: "fcm", TokenCiphertext: encryptPushTestSecret(t, "token-a", key)},
		{UserID: "user-b", DeviceID: "shared", Environment: "production", Provider: "fcm", TokenCiphertext: encryptPushTestSecret(t, "token-b", key)},
	}
	message := pushMessage{Title: "Legend defense"}
	if err := worker.deliverLegendDefenseDevices(t.Context(), "event-1", "#P0Y", devices, message); err == nil {
		t.Fatal("first partial delivery succeeded")
	}
	if err := worker.deliverLegendDefenseDevices(t.Context(), "event-1", "#P0Y", devices, message); err != nil {
		t.Fatal(err)
	}
	if requests["token-a"] != 1 || requests["token-b"] != 2 {
		t.Fatalf("requests = %#v, want successful device once and failed device retried", requests)
	}
}

func TestMobileEventBatchIsolatesFailedEntryAndAcknowledgesLaterSuccess(t *testing.T) {
	failed := errors.New("temporary provider failure")
	var processed, acknowledged []string
	worker := &mobileEventsWorker{
		processEntry: func(_ context.Context, id string, _ mobileWarEvent) error {
			processed = append(processed, id)
			if id == "1-0" {
				return failed
			}
			return nil
		},
		ackEntry: func(_ context.Context, id string) error {
			acknowledged = append(acknowledged, id)
			return nil
		},
	}
	entries := []valkey.XRangeEntry{
		{ID: "1-0", FieldValues: map[string]string{"topic": "legend", "value": `{"type":"legend_defense"}`}},
		{ID: "2-0", FieldValues: map[string]string{"topic": "legend", "value": `{"type":"legend_defense"}`}},
	}
	err := worker.processEntries(t.Context(), entries)
	if !errors.Is(err, failed) {
		t.Fatalf("batch error = %v, want failed first entry", err)
	}
	if strings.Join(processed, ",") != "1-0,2-0" {
		t.Fatalf("processed = %#v, want both entries", processed)
	}
	if strings.Join(acknowledged, ",") != "2-0" {
		t.Fatalf("acknowledged = %#v, want only healthy second entry", acknowledged)
	}
}

func TestMobilePushConsumesWarAndRaidReminders(t *testing.T) {
	for _, eventType := range []string{"war", "raid_mobile"} {
		if !mobilePushEventType(mobileWarEvent{Topic: "reminder", Value: map[string]any{"type": eventType}}) {
			t.Fatalf("%s reminder was not accepted as a mobile event", eventType)
		}
	}
}

func TestMobilePushConsumesOnlyLegendDefenseEvents(t *testing.T) {
	if !mobilePushEventType(mobileWarEvent{Topic: "legend", Value: map[string]any{"type": "legend_defense"}}) {
		t.Fatal("Legend defense event was not accepted")
	}
	if mobilePushEventType(mobileWarEvent{Topic: "legend", Value: map[string]any{"type": "legend_battle"}}) {
		t.Fatal("retired Legend attack/combined event was accepted")
	}
	for _, fragment := range []string{
		"account.enabled=true",
		"preference.legend_defenses_enabled=true",
		"account.player_tag=$1",
	} {
		if !strings.Contains(legendDefenseSubscriptionsSQL, fragment) {
			t.Fatalf("Legend defense subscription query is missing %q: %s", fragment, legendDefenseSubscriptionsSQL)
		}
	}
}

func TestWarReminderDecoderAcceptsOnlyV2NestedData(t *testing.T) {
	event := mobileWarEvent{Value: map[string]any{
		"minutes_remaining": json.Number("45"),
		"data": map[string]any{
			"state": "inWar",
			"clan":  map[string]any{"tag": "#AAA"},
		},
	}}
	war, minutes, err := decodeWarReminderEvent(event)
	if err != nil {
		t.Fatalf("decode v2 reminder: %v", err)
	}
	if minutes != 45 || war.State != "inWar" || war.Clan == nil || war.Clan.Tag != "#AAA" {
		t.Fatalf("decoded reminder = minutes %d, war %#v", minutes, war)
	}

	event.Value["data"] = `{"state":"inWar"}`
	if _, _, err := decodeWarReminderEvent(event); err == nil {
		t.Fatal("stringified legacy reminder data was accepted")
	}
	delete(event.Value, "minutes_remaining")
	event.Value["data"] = map[string]any{"state": "inWar"}
	if _, _, err := decodeWarReminderEvent(event); err == nil {
		t.Fatal("reminder without numeric minutes_remaining was accepted")
	}
}

func TestMobileSubscriptionsUseOnlyOrdinaryNotificationColumns(t *testing.T) {
	for _, column := range []string{
		"d.user_id",
		"d.device_id",
		"preference.war_state_enabled",
		"preference.war_attacks_enabled",
		"account.enabled = true",
		"mobile_notification_accounts",
		"mobile_notification_preferences",
		"mobile_push_devices",
	} {
		if !strings.Contains(mobileSubscriptionsSQL, column) {
			t.Fatalf("subscription query missing %q", column)
		}
	}
	for _, retired := range []string{"mobile_war_subscriptions", "live_activity_enabled", "provider = 'apns'", "league_battles_enabled", "ranked_battlelog", "account.active", "account.source"} {
		if strings.Contains(mobileSubscriptionsSQL, retired) {
			t.Fatalf("subscription query still reads retired %q", retired)
		}
	}
}

func TestSubscriptionWantsWarAndCWLEvents(t *testing.T) {
	tests := []struct {
		name  string
		sub   mobileSubscription
		event mobileWarEvent
		want  bool
	}{
		{
			name:  "war start",
			sub:   mobileSubscription{WarStartEnabled: true},
			event: mobileWarEvent{Topic: "war", Value: map[string]any{"type": "new_war"}},
			want:  true,
		},
		{
			name:  "score change",
			sub:   mobileSubscription{ScoreChangeEnabled: true},
			event: mobileWarEvent{Topic: "war", Value: map[string]any{"type": "new_attacks"}},
			want:  true,
		},
		{
			name:  "war end",
			sub:   mobileSubscription{WarEndEnabled: true},
			event: mobileWarEvent{Topic: "war", Value: map[string]any{"type": "war_state"}},
			want:  true,
		},
		{
			name: "cwl preparation does not look like a battle start",
			sub:  mobileSubscription{WarStartEnabled: true},
			event: mobileWarEvent{Topic: "war", Value: map[string]any{
				"type": "new_war", "war_type": "cwl", "war_role": "preparation",
			}},
			want: false,
		},
		{
			name:  "disabled preference",
			sub:   mobileSubscription{},
			event: mobileWarEvent{Topic: "war", Value: map[string]any{"type": "new_war"}},
			want:  false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := subscriptionWantsEvent(test.sub, test.event); got != test.want {
				t.Fatalf("subscriptionWantsEvent() = %v, want %v", got, test.want)
			}
		})
	}
}
