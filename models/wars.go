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
	WarTag        string
}

// CurrentWarTimerRow is the active player-to-war lookup. It deliberately has no
// payload: the durable war record is written only after the scheduled final fetch.
type CurrentWarTimerRow struct {
	PlayerTag   string
	WarID       string
	ClanTag     string
	OpponentTag string
	EndTime     time.Time
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
}

type WarIngest struct {
	IndexRows           []WarLogIndexRow
	AttackRows          []WarAttackRow
	Players             []BasicPlayerRow
	Schedules           []WarScheduleRow
	CurrentWarTimers    []CurrentWarTimerRow
	CWLGroups           []CWLGroupRow
	FinishedScheduleKey string
	FinishedWarID       string
	RawWarJSON          []byte
}
