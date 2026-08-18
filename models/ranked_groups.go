package models

type RankedLeagueGroupMemberRow struct {
	SeasonID         int64
	GroupTag         string
	LeagueTierID     int
	PlayerTag        string
	PlayerName       string
	ClanTag          string
	ClanName         string
	Placement        int
	LeagueTrophies   int
	AttackWinCount   int
	AttackLoseCount  int
	DefenseWinCount  int
	DefenseLoseCount int
}
