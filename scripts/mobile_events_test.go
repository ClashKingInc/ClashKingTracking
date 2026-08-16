package scripts

import (
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

func TestMobileSubscriptionsUseOnlyOrdinaryNotificationColumns(t *testing.T) {
	for _, column := range []string{
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
			name:  "cwl update",
			sub:   mobileSubscription{WarStartEnabled: true},
			event: mobileWarEvent{Topic: "cwl", Value: map[string]any{"type": "cwl_war_update"}},
			want:  true,
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
