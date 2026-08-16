package models

import "time"

type TrackedPlayerTarget struct {
	Tag string
}

type PlayerProfileChangeRow struct {
	EventTime     time.Time
	PlayerTag     string
	ClanTag       string
	TownHallLevel int
	ChangeType    string
	PreviousValue any
	CurrentValue  any
}

type PlayerStatChangeRow struct {
	EventTime     time.Time
	PlayerTag     string
	ClanTag       *string
	StatType      string
	PreviousValue int64
	CurrentValue  int64
	Delta         int64
}

type TrackedPlayerIngest struct {
	Players        []BasicPlayerRow
	ProfileChanges []PlayerProfileChangeRow
	StatChanges    []PlayerStatChangeRow
	LastOnlineAt   *time.Time
	Event          Event
	SnapshotTag    string
	SnapshotRaw    []byte
}
