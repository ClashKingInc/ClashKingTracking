package models

import "time"

type WarLogIndexRow struct {
	WarID                         string
	ClanTag                       string
	OpponentTag                   string
	PrepTime                      time.Time
	StartTime                     *time.Time
	EndTime                       time.Time
	Size                          int
	AttacksPerMember              int
	WarType                       string
	State                         string
	BattleModifier                string
	WarTag                        string
	ClanName                      string
	OpponentName                  string
	ClanBadgeToken                string
	OpponentBadgeToken            string
	ClanLevel                     int
	OpponentClanLevel             int
	ClanAttacks                   int
	OpponentAttacks               int
	ClanStars                     int
	OpponentStars                 int
	ClanDestructionPercentage     float64
	OpponentDestructionPercentage float64
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
	WarType       string
	WarTag        string
}

// PlayerTimerRow records that a player is participating in a time-bounded event.
// EventKey points at war_schedule.schedule_key for wars and is the clan tag for raids.
type PlayerTimerRow struct {
	PlayerTag string
	EventType string
	EventKey  string
	ExpiresAt time.Time
}

type CWLGroupRow struct {
	CWLID       string
	Season      string
	CWLLeagueID *int
	State       string
	WarSize     *int
	Rounds      [][]string
	Clans       []CWLGroupClanRow
}

type CWLGroupClanRow struct {
	ClanTag    string
	Name       string
	ClanLevel  int
	BadgeToken string
	Members    []BasicClanMember
}

type WarIngest struct {
	IndexRows           []WarLogIndexRow
	AttackRows          []WarAttackRow
	Schedules           []WarScheduleRow
	PlayerTimers        []PlayerTimerRow
	CWLGroups           []CWLGroupRow
	FinishedScheduleKey string
	FinishedWarID       string
}
