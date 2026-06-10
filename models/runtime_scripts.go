package models

import "time"

type GiveawayRow struct {
	ID        string
	ServerID  string
	Status    string
	Updated   bool
	StartTime time.Time
	EndTime   time.Time
	Data      any
}

type GiveawayTransition struct {
	Kind     string
	From     string
	To       string
	Row      GiveawayRow
	Event    Event
	EventDue bool
}

type LeaderboardSnapshotItemRow struct {
	Kind       string
	LocationID string
	Date       time.Time
	Tag        string
	Name       string
	Rank       int
	Data       any
}
