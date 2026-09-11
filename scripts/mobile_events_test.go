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
