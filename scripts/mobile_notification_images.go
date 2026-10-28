package scripts

import "strings"

// These are the same assets used by the app's Home event timers.
const legendNotificationImage = "https://assets.clashk.ing/leagues/league-tier/legend_league_1.png"

func campaignNotificationImage(key string) string {
	for _, event := range []struct{ slug, image string }{
		{"cwl", "icons/Icon_DC_CWL_No_Border.png"},
		{"clan-games", "icons/Icon_HV_Clan_Games_Medal.png"},
		{"raid-weekend", "icons/Icon_HV_Raid_Attack.png"},
	} {
		if strings.HasPrefix(key, "game-event-"+event.slug+"-") || strings.HasPrefix(key, event.slug+"-") {
			return "https://assets.clashk.ing/" + event.image
		}
	}
	return ""
}

func notificationClanBadgeURL(token string) string {
	if token == "" {
		return ""
	}
	return "https://api-assets.clashofclans.com/badges/200/" + token + ".png"
}
