package scripts

import (
	"encoding/json"
	"slices"

	"clashking_tracking/internal/platform"
	clashy "github.com/clashkinginc/clashy.go"
)

// Compare adjacent successful polls. Initial snapshots and counter resets must
// never be presented as donations or achievements.
func trackedClanLogChanges(previous, current clashy.Clan) []platform.Event {
	var events []platform.Event
	add := func(kind string, details map[string]any) {
		value := map[string]any{"type": kind, "clan": json.RawMessage(jsonBytes(current)), "previous_clan": json.RawMessage(jsonBytes(previous))}
		for k, v := range details {
			value[k] = v
		}
		events = append(events, platform.Event{Topic: "clan", ClanTag: current.Tag, Value: value})
	}
	// One event per count change, not one per joining/leaving member.
	if previous.MemberCount != current.MemberCount {
		add("member_count", map[string]any{"previous_count": previous.MemberCount, "current_count": current.MemberCount})
	}
	oldMembers := make(map[string]clashy.ClanMember, len(previous.Members))
	for _, member := range previous.Members {
		oldMembers[member.Tag] = member
	}
	var donations []map[string]any
	for _, member := range current.Members {
		old, exists := oldMembers[member.Tag]
		if !exists {
			continue
		}
		donated, received := max(0, member.Donations-old.Donations), max(0, member.Received-old.Received)
		if donated > 0 || received > 0 {
			donations = append(donations, map[string]any{"member": member, "donated": donated, "received": received})
		}
		if old.Role != member.Role {
			add("role_change", map[string]any{"member": member, "previous_role": old.Role, "current_role": member.Role})
		}
	}
	if len(donations) > 0 {
		add("all_member_donations", map[string]any{"donations": donations})
	}
	if previous.Description != current.Description {
		add("clan_description", nil)
	}
	if previous.RequiredTrophies != current.RequiredTrophies || previous.RequiredTownhall != current.RequiredTownhall || previous.RequiredBuilderBaseTrophies != current.RequiredBuilderBaseTrophies || previous.Type != current.Type {
		add("clan_requirements", nil)
	}
	if current.Level > previous.Level {
		add("clan_achievement", map[string]any{"achievement": "clan_level", "previous": previous.Level, "current": current.Level})
	}
	if current.WarWinStreak > previous.WarWinStreak && current.WarWinStreak > 0 && current.WarWinStreak%5 == 0 {
		add("clan_achievement", map[string]any{"achievement": "war_win_streak", "previous": previous.WarWinStreak, "current": current.WarWinStreak})
	}
	// Names are the stable Clash API CWL tiers; do not infer rank from asset IDs
	// or treat initial placement from Unranked as a promotion.
	leagues := []string{"Bronze League III", "Bronze League II", "Bronze League I", "Silver League III", "Silver League II", "Silver League I", "Gold League III", "Gold League II", "Gold League I", "Crystal League III", "Crystal League II", "Crystal League I", "Master League III", "Master League II", "Master League I", "Champion League III", "Champion League II", "Champion League I"}
	oldRank, newRank := slices.Index(leagues, previous.WarLeague.Name), slices.Index(leagues, current.WarLeague.Name)
	if oldRank >= 0 && newRank > oldRank {
		add("clan_achievement", map[string]any{"achievement": "cwl_promotion", "previous": previous.WarLeague, "current": current.WarLeague})
	}
	return events
}
