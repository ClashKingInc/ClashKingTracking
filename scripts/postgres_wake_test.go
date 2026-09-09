//go:build script_internal_tests

package scripts

import "testing"

func TestParseTrackingWakeEventAcceptsVersionedAPIShapes(t *testing.T) {
	for _, payload := range []string{
		`{"v":1,"kind":"reminder_config","clanTag":"#P0Y","reminderType":"War"}`,
		`{"v":1,"kind":"mobile_reminder_config","userId":"18446744073709551615"}`,
		`{"v":1,"kind":"guild_reactivated","serverId":"18446744073709551615"}`,
	} {
		if _, err := parseTrackingWakeEvent(payload); err != nil {
			t.Fatalf("valid payload rejected: %s: %v", payload, err)
		}
	}
}

func TestParseTrackingWakeEventRejectsMalformedOrUnboundedPayloads(t *testing.T) {
	for _, payload := range []string{
		``,
		`{"v":2,"kind":"guild_reactivated","serverId":"1"}`,
		`{"v":1,"kind":"unknown","serverId":"1"}`,
		`{"v":1,"kind":"reminder_config","clanTag":"p0y","reminderType":"War"}`,
		`{"v":1,"kind":"mobile_reminder_config","userId":"12x"}`,
		`{"v":1,"kind":"guild_reactivated","serverId":"1","extra":true}`,
		`{"v":1,"kind":"guild_reactivated","serverId":"1"} {}`,
	} {
		if _, err := parseTrackingWakeEvent(payload); err == nil {
			t.Fatalf("invalid payload accepted: %q", payload)
		}
	}
}
