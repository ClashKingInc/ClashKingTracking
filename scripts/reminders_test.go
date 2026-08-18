//go:build script_internal_tests

package scripts

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	clashy "github.com/clashkinginc/clashy.go"
)

func TestInactivityReminderTimestampParameterIsTyped(t *testing.T) {
	if occurrences := strings.Count(inactivityReminderRowsSQL, "$1::timestamptz"); occurrences != 2 {
		t.Fatalf("inactivity timestamp casts = %d, want 2: %s", occurrences, inactivityReminderRowsSQL)
	}
}

func TestDueReminderQueryHandlesRegularWarWithoutWarTag(t *testing.T) {
	source, err := os.ReadFile("reminders.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "COALESCE(schedule.war_tag, '')") {
		t.Fatal("due reminder query must scan a nullable regular-war tag safely")
	}
}

func TestReminderReconciliationEventsRequireV2Identity(t *testing.T) {
	domain := &remindersDomain{}
	if err := domain.handleReconciliationEvent(context.Background(), "war_schedule", "#AAA", map[string]any{}); err == nil {
		t.Fatal("war_schedule event without schedule_key was accepted")
	}
	if err := domain.handleReconciliationEvent(context.Background(), "reminder_config", "", map[string]any{}); err == nil {
		t.Fatal("reminder_config event without clan_tag was accepted")
	}
	if err := domain.handleReconciliationEvent(context.Background(), "unrelated", "", nil); err != nil {
		t.Fatalf("unrelated event should be ignored: %v", err)
	}
}

func TestReminderRemovalKeepsWarTypeFilter(t *testing.T) {
	source, err := os.ReadFile("reminders.go")
	if err != nil {
		t.Fatal(err)
	}
	needle := "cardinality(reminder.war_type_names) = 0 OR schedule.war_type = ANY(reminder.war_type_names)"
	if strings.Count(string(source), needle) < 2 {
		t.Fatal("war reminder insertion and removal must apply the same war-type filter")
	}
}

func TestDiscordReminderEventsUseOnlyV2FieldNames(t *testing.T) {
	source, err := os.ReadFile("reminders.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	for _, retired := range []string{`"clan_data"`, `"raid_data"`, `"reminder_data"`} {
		if strings.Contains(text, retired) {
			t.Fatalf("Discord reminder publisher still uses retired field %s", retired)
		}
	}
	for _, current := range []string{`"clan": clan`, `"reminder": reminderData`, `"raid": &raid`, `"members": missing`} {
		if !strings.Contains(text, current) {
			t.Fatalf("Discord reminder publisher is missing v2 field %s", current)
		}
	}
}

func TestFixedDiscordReminderPayloadComesFromTypedColumns(t *testing.T) {
	if strings.Contains(discordReminderPayloadSQL, "reminder.data") {
		t.Fatal("fixed reminder event still forwards an opaque compatibility blob")
	}
	for _, field := range []string{
		"id", "server_id", "type_name", "clan_tag", "channel_id", "trigger_time",
		"minutes_remaining", "custom_text", "town_halls", "roles", "war_types", "trigger_threshold",
	} {
		if !strings.Contains(discordReminderPayloadSQL, "'"+field+"'") {
			t.Fatalf("typed reminder payload is missing %q: %s", field, discordReminderPayloadSQL)
		}
	}
}

func TestRemainingRaidAttacksTotalsVerifiedAccounts(t *testing.T) {
	members := map[string]clashy.RaidMember{
		"#USED":  {Tag: "#USED", AttackCount: 4, AttackLimit: 5},
		"#BONUS": {Tag: "#BONUS", AttackCount: 5, AttackLimit: 5, BonusAttackLimit: 1},
		"#DONE":  {Tag: "#DONE", AttackCount: 5, AttackLimit: 5},
	}
	if got, want := remainingRaidAttacks([]string{"#USED", "#BONUS", "#DONE", "#NO_ATTACKS"}, members), 7; got != want {
		t.Fatalf("remaining attacks = %d, want %d", got, want)
	}
}

func TestRaidReminderClockUsesSharedMondayEnd(t *testing.T) {
	friday := time.Date(2026, 8, 21, 7, 0, 0, 0, time.UTC)
	if got, want := raidWeekendEnd(friday), time.Date(2026, 8, 24, 7, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("raid end = %s, want %s", got, want)
	}
	afterEnd := time.Date(2026, 8, 24, 7, 1, 0, 0, time.UTC)
	if got, want := raidWeekendEnd(afterEnd), time.Date(2026, 8, 31, 7, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("next raid end = %s, want %s", got, want)
	}
}
