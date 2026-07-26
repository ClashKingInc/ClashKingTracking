package models

import "time"

type BasicClanRow struct {
	Tag                  string
	Name                 string
	Description          string
	ClanLevel            int
	LocationID           *int
	CWLLeagueID          int
	CapitalLeagueID      *int
	PublicWarLog         bool
	WarWins              int
	WarWinStreak         int
	ClanPoints           int
	BuilderBasePoints    int
	CapitalPoints        int
	MemberCount          int
	BadgeURL             string
	TroopsDonated        int
	TroopsReceived       int
	Members              []BasicClanMember
	LastActive           *time.Time
	RecordClanPoints     int
	RecordClanPointsAt   *time.Time
	RecordWarWinStreak   int
	RecordWarWinStreakAt *time.Time
}

type BasicClanMember struct {
	Tag      string `json:"tag"`
	Name     string `json:"name"`
	TownHall int    `json:"town_hall"`
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
	PlayerName    string
	TownHallLevel int
}

type ClanRecordRow struct {
	Tag            string
	ClanPoints     int
	ClanPointsAt   *time.Time
	WarWinStreak   int
	WarWinStreakAt *time.Time
}

type GlobalClanIngest struct {
	Clans           []BasicClanRow
	Players         []BasicPlayerRow
	ClanRecords     []ClanRecordRow
	ActiveClanTags  []string
	DeletedClanTags []string
	ClanChanges     []ClanChangeRow
	JoinLeaves      []JoinLeaveRow
}
