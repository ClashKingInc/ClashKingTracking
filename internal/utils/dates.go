package utils

import "time"

func CurrentSeason(now time.Time) string {
	return now.UTC().Format("2006-01")
}

func IsCWL(now time.Time) bool {
	now = now.UTC()
	// Global CWL discovery is useful through the fifteenth. Live tracked clans
	// can continue beyond this window when their persisted group is not ended.
	return now.Day() >= 1 && now.Day() <= 15
}
