package models

import "time"

type BotPlayerTarget struct {
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

type PlayerSeasonStatRow struct {
	PlayerTag        string
	Season           string
	ClanTag          string
	Donated          int
	Received         int
	CapitalGoldDonos int
	ActivityScore    int
	LastOnlineAt     *time.Time
}

type BotPlayerIngest struct {
	Players        []BasicPlayerRow
	ProfileChanges []PlayerProfileChangeRow
	SeasonStats    []PlayerSeasonStatRow
	LastOnlineAt   *time.Time
	Event          Event
	SnapshotTag    string
	SnapshotRaw    []byte
}
