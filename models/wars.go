package models

import "time"

type WarLogIndexRow struct {
	WarID            string
	ClanTag          string
	OpponentTag      string
	PrepTime         time.Time
	StartTime        *time.Time
	EndTime          time.Time
	ClanBadgeURL     string
	OpponentBadgeURL string
	Size             int
	WarType          string
	State            string
	BattleModifier   string
	CWLWarTag        string
	R2Key            string
	StoredAt         *time.Time
}

type WarAttackRow struct {
	WarID                 string
	WarEndTime            time.Time
	WarType               string
	WarSize               int
	AttackingClanTag      string
	DefendingClanTag      string
	AttackerTag           string
	DefenderTag           string
	AttackerTownHall      int
	DefenderTownHall      int
	AttackerMapPosition   int
	DefenderMapPosition   int
	Stars                 int
	DestructionPercentage int
	Duration              int
	AttackOrder           int
	BattleModifier        string
}

type WarScheduleRow struct {
	WarID         string
	SourceClanTag string
	OpponentTag   string
	PrepTime      time.Time
	EndTime       time.Time
	NextRunAt     time.Time
	CWLWarTag     string
	Status        string
	Attempts      int
	LastError     string
}

type CWLGroupRow struct {
	CWLID       string
	Season      string
	CWLLeagueID int
	ClanTags    []string
	Rounds      [][]string
	Data        any
}

type WarIngest struct {
	IndexRows     []WarLogIndexRow
	AttackRows    []WarAttackRow
	Players       []BasicPlayerRow
	Schedules     []WarScheduleRow
	CWLGroups     []CWLGroupRow
	FinishedWarID string
	R2Key         string
	RawWarJSON    []byte
}
