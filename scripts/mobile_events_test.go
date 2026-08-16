package scripts

import (
	"strings"
	"testing"
)

func TestLegendActivityKeyIsScopedToPlayer(t *testing.T) {
	if got := legendNotificationActivityKey("#PLAYER"); got != "mobile:legend:active:#PLAYER" {
		t.Fatalf("legend activity key = %q", got)
	}
}

func TestMobilePushConsumesIndividualLegendBattles(t *testing.T) {
	event := mobileWarEvent{Topic: "legend", Value: map[string]any{
		"type": "legend_battle", "battle_id": "battle-1", "player_tag": "#PLAYER", "opponent_name": "Opponent",
		"attack": true, "stars": float64(3), "destruction_percentage": float64(100),
	}}
	if !mobilePushEventType(event) {
		t.Fatal("legend batch was not accepted as a mobile event")
	}
	battle := mobileLegendBattleFromEvent(event)
	if battle.PlayerTag != "#PLAYER" || !battle.Attack || battle.Stars != 3 {
		t.Fatalf("unexpected parsed battle: %#v", battle)
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

func TestLegendBattleRecipientsRestrictAttackNotificationsToBookmarks(t *testing.T) {
	for _, required := range []string{
		"player.league_id = 105000036",
		"account.player_tag = ANY($1)",
		"account.source = 'bookmarked'",
		"device.legend_attacks_enabled = true",
		"device.legend_defenses_enabled = true",
		"device.authorization_status IN ('authorized', 'provisional')",
		"device.last_seen_at >= now() - interval '7 days'",
	} {
		if !strings.Contains(legendBattleRecipientsSQL, required) {
			t.Fatalf("legend recipient query missing %q", required)
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
