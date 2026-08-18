package scripts

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMobilePushConsumesWarAndRaidReminders(t *testing.T) {
	for _, eventType := range []string{"war", "raid_mobile"} {
		if !mobilePushEventType(mobileWarEvent{Topic: "reminder", Value: map[string]any{"type": eventType}}) {
			t.Fatalf("%s reminder was not accepted as a mobile event", eventType)
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
		"war_state_enabled",
		"war_attacks_enabled",
		"mobile_notification_accounts",
		"mobile_push_devices",
	} {
		if !strings.Contains(mobileSubscriptionsSQL, column) {
			t.Fatalf("subscription query missing %q", column)
		}
	}
	for _, retired := range []string{"mobile_war_subscriptions", "live_activity_enabled", "provider = 'apns'", "league_battles_enabled", "ranked_battlelog"} {
		if strings.Contains(mobileSubscriptionsSQL, retired) {
			t.Fatalf("subscription query still reads retired %q", retired)
		}
	}
}

func TestMobileLiveEventDeliveryKeyIsPerDevice(t *testing.T) {
	first := mobileLiveEventDeliveryKey("123-0", mobileSubscription{DeviceID: "phone", Environment: "production"})
	second := mobileLiveEventDeliveryKey("123-0", mobileSubscription{DeviceID: "tablet", Environment: "production"})
	if first == second {
		t.Fatalf("delivery keys must differ by device: %q", first)
	}
	if got := mobileLiveEventDeliveryKey("123-0", mobileSubscription{DeviceID: "phone", Environment: "production"}); got != first {
		t.Fatalf("delivery key is not stable: %q != %q", got, first)
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
