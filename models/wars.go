package models

import "time"

type WarLogIndexRow struct {
	WarID          string
	ClanTag        string
	OpponentTag    string
	PrepTime       time.Time
	StartTime      *time.Time
	EndTime        time.Time
	Size           int
	WarType        string
	State          string
	BattleModifier string
	WarTag         string
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
	DefenderName          string
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
	ScheduleKey   string
	WarID         string
	SourceClanTag string
	OpponentTag   string
	PrepTime      time.Time
	EndTime       time.Time
	NextRunAt     time.Time
	WarTag        string
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
	IndexRows           []WarLogIndexRow
	AttackRows          []WarAttackRow
	Players             []BasicPlayerRow
	Schedules           []WarScheduleRow
	CWLGroups           []CWLGroupRow
	FinishedScheduleKey string
	FinishedWarID       string
	RawWarJSON          []byte
}
