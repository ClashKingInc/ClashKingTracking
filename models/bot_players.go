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

// PlayerCurrentStatRow is the current-state snapshot of a player, upserted into
// player_current_stats (PK player_tag). Only the columns this writer owns are set;
// the legends/donations/activity jsonb breakdowns are populated by other domains
// (legends → league rework) and left at their DB default here. The data jsonb
// column is not carried here — the writer reuses the ingest's SnapshotRaw (same
// full CoC player object) to avoid duplicating the payload in memory.
type PlayerCurrentStatRow struct {
	PlayerTag     string
	ClanTag       string
	Name          string
	TownHallLevel int
	LastOnlineAt  *time.Time
}

type BotPlayerIngest struct {
	Players        []BasicPlayerRow
	ProfileChanges []PlayerProfileChangeRow
	SeasonStats    []PlayerSeasonStatRow
	CurrentStats   *PlayerCurrentStatRow
	LastOnlineAt   *time.Time
	Event          Event
	SnapshotTag    string
	SnapshotRaw    []byte
}
