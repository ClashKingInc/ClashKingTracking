package scripts

import "fmt"

func clanReminderNotificationText(raid bool, clan string, remaining, minutes int) (string, string) {
	if raid {
		return clan + ": Raid reminder", "Raid Weekend ends in " + formatReminderTime(minutes) + "."
	}
	return clan + ": War reminder", fmt.Sprintf("War ends in %s. %d clan attacks remaining.", formatReminderTime(minutes), remaining)
}
