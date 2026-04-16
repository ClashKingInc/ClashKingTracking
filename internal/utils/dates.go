package utils

import (
	"fmt"
	"time"
)

func CurrentSeason(now time.Time) string {
	return now.UTC().Format("2006-01")
}

func CurrentRaidWeek(now time.Time) string {
	now = now.UTC()
	start := now
	// Raid weekends roll over on Friday 07:00 UTC, so midweek lookups map back to the prior start.
	if now.Weekday() > time.Monday && (now.Weekday() < time.Friday || (now.Weekday() == time.Friday && now.Hour() < 7)) {
		start = now.AddDate(0, 0, -7)
	}
	daysToFriday := (int(time.Friday) - int(start.Weekday()) + 7) % 7
	weekend := time.Date(start.Year(), start.Month(), start.Day(), 7, 0, 0, 0, time.UTC).AddDate(0, 0, daysToFriday)
	return weekend.Format("2006-01-02")
}

func CurrentGamesSeason(now time.Time) string {
	return fmt.Sprintf("%04d-%02d", now.UTC().Year(), int(now.UTC().Month()))
}

func IsCWL(now time.Time) bool {
	now = now.UTC()
	// CWL tracking intentionally mirrors the game's visible window rather than calendar month boundaries.
	if now.Day() < 1 || now.Day() > 10 {
		return false
	}
	if now.Day() == 1 && now.Hour() < 8 {
		return false
	}
	return !(now.Day() == 11 && now.Hour() >= 8)
}
