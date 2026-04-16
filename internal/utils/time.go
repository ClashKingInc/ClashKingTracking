package utils

import "time"

const cocLayout = "20060102T150405.000Z"

func ParseCocTime(raw string) (time.Time, error) {
	return time.Parse(cocLayout, raw)
}

func RaidWeekendStart(now time.Time) time.Time {
	now = now.UTC()
	start := now
	// The API reports raid activity against the weekend start, which flips at Friday 07:00 UTC.
	if now.Weekday() > time.Monday && (now.Weekday() < time.Friday || (now.Weekday() == time.Friday && now.Hour() < 7)) {
		start = now.AddDate(0, 0, -7)
	}
	daysToFriday := (int(time.Friday) - int(start.Weekday()) + 7) % 7
	return time.Date(start.Year(), start.Month(), start.Day(), 7, 0, 0, 0, time.UTC).AddDate(0, 0, daysToFriday)
}
