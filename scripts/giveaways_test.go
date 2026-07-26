//go:build script_internal_tests

package scripts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"clashking_tracking/internal/platform"
)

func TestGiveawayTransitionEventShape(t *testing.T) {
	now := time.Now().UTC()
	raw, _ := json.Marshal(map[string]any{"id": "giveaway-1", "status": "ongoing"})
	transition, err := scanGiveawayTransition(fakeGiveawayScanner{
		values: []any{
			"giveaway_start", "scheduled", "ongoing", "giveaway-1", "server-1",
			"ongoing", false, now, now.Add(time.Hour), raw, true,
		},
	})

	if err != nil {
		t.Fatal(err)
	}
	if transition.Kind != "giveaway_start" || transition.Event.Type != "giveaway_start" {
		t.Fatalf("transition = %+v", transition)
	}
	if transition.Event.Value["type"] != "giveaway_start" {
		t.Fatalf("event value = %+v", transition.Event.Value)
	}
}

func TestGiveawayEventPayloadUsesEveryRetainedTypedColumn(t *testing.T) {
	implicitRowSerializer := "to_json" + "b("
	if strings.Contains(dueGiveawaysSQL, implicitRowSerializer) || strings.Contains(markGiveawayTransitionSQL, implicitRowSerializer) {
		t.Fatal("giveaway events must not serialize the database row implicitly")
	}
	for _, field := range []string{
		"id", "server_id", "prize", "channel_id", "status", "start_time", "end_time", "winners",
		"mentions", "text_above_embed", "text_in_embed", "text_on_end", "image_url",
		"profile_picture_required", "coc_account_required", "roles_mode", "roles", "boosters",
		"entries", "winners_list", "updated", "message_id", "event_pending", "event_pending_at",
		"created_at", "updated_at",
	} {
		key := "'" + field + "', " + field
		if !strings.Contains(giveawayEventPayloadSQL, key) {
			t.Fatalf("typed event payload omits %q: %s", field, giveawayEventPayloadSQL)
		}
	}
}

func TestValidateGiveawaysConfig(t *testing.T) {
	cfg := platform.Config{MockDB: true}
	cfg.GiveawayScanSeconds = 0
	if err := validateGiveawaysConfig(cfg); err == nil {
		t.Fatal("expected invalid scan seconds error")
	}
	cfg.GiveawayScanSeconds = 30
	if err := validateGiveawaysConfig(cfg); err != nil {
		t.Fatalf("valid mock config rejected: %v", err)
	}
}

type fakeGiveawayScanner struct {
	values []any
}

func (s fakeGiveawayScanner) Scan(dest ...any) error {
	for i := range dest {
		switch target := dest[i].(type) {
		case *string:
			*target = s.values[i].(string)
		case *bool:
			*target = s.values[i].(bool)
		case *time.Time:
			*target = s.values[i].(time.Time)
		case *[]byte:
			*target = s.values[i].([]byte)
		}
	}
	return nil
}
