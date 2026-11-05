package scripts

import clashy "github.com/clashkinginc/clashy.go"

const personalWarReminderRecipientsSQL = `
SELECT account.user_id, link.tag, device.device_id,
       device.platform, device.provider, device.environment,
       device.token_ciphertext, device.locale
FROM user_bookmarks account
JOIN clan_notification_tracking interest ON interest.tag=account.tag AND interest.war_tracked
JOIN player_links link ON link.user_id=account.user_id AND link.is_verified AND link.tag=ANY($3::text[])
JOIN mobile_notification_preferences preference ON preference.user_id=account.user_id
JOIN mobile_push_devices device ON device.user_id=account.user_id AND device.enabled AND device.provider='fcm'
WHERE account.entity_type='clan' AND account.war_notifications_enabled AND account.tag=$1
  AND preference.war_reminders_enabled AND $2=ANY(preference.reminder_timings)
`

type personalRaidReminder struct {
	Accounts, Unstarted, Remaining int
	Name                           string
}

type personalWarAccount struct {
	name      string
	remaining int
}

func unfinishedWarMembers(war clashy.ClanWar, clanTag string) map[string]personalWarAccount {
	limit := 2
	if war.WarTag != "" {
		limit = 1
	}
	remaining := make(map[string]personalWarAccount)
	for _, side := range []*clashy.WarClan{war.Clan, war.Opponent} {
		if side == nil || side.Tag != clanTag {
			continue
		}
		for _, member := range side.Members {
			if unused := limit - len(member.Attacks); unused > 0 {
				remaining[member.Tag] = personalWarAccount{name: member.Name, remaining: unused}
			}
		}
	}
	return remaining
}

// Only verified links supplied by the caller and the live clan roster qualify.
// A linked player absent from the raid log can still have five unused attacks.
func personalRaidRemaining(tags []string, roster []clashy.ClanMember, raidMembers []clashy.RaidMember) personalRaidReminder {
	current := make(map[string]clashy.ClanMember)
	for _, member := range roster {
		if member.TownHall >= 6 {
			current[member.Tag] = member
		}
	}
	raid := make(map[string]clashy.RaidMember)
	for _, member := range raidMembers {
		raid[member.Tag] = member
	}
	seen := make(map[string]bool)
	var result personalRaidReminder
	for _, tag := range tags {
		member, eligible := current[tag]
		if !eligible || seen[tag] {
			continue
		}
		seen[tag] = true
		remaining := remainingRaidAttacks([]string{tag}, raid)
		if remaining <= 0 {
			continue
		}
		result.Accounts++
		result.Name = member.Name
		result.Remaining += remaining
		if raid[tag].AttackCount == 0 {
			result.Unstarted++
		}
	}
	return result
}
