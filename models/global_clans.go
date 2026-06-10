package models

import "time"

type BasicClanRow struct {
	Tag             string
	Name            string
	Description     string
	ClanLevel       int
	LocationID      *int
	CWLLeagueID     int
	CapitalLeagueID *int
	PublicWarLog    bool
	WarWins         int
	MemberCount     int
	BadgeURL        string
	TroopsDonated   int
	TroopsReceived  int
	MemberTags      []string
	LastActive      *time.Time
}

type ClanChangeRow struct {
	EventTime     time.Time
	ClanTag       string
	ChangeType    string
	PreviousValue any
	CurrentValue  any
}

type JoinLeaveRow struct {
	EventTime     time.Time
	EventType     string
	ClanTag       string
	PlayerTag     string
	TownHallLevel int
}

type GlobalClanIngest struct {
	Clans           []BasicClanRow
	Players         []BasicPlayerRow
	ActiveClanTags  []string
	DeletedClanTags []string
	ClanChanges     []ClanChangeRow
	JoinLeaves      []JoinLeaveRow
}
