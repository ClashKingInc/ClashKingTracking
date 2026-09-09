package models

type RankedLeagueGroupMemberRow struct {
	SeasonID           int64
	GroupTag           string
	LeagueTierID       int
	PlayerTag          string
	PlayerName         string
	Placement          int
	LeagueTrophies     int
	TownHall           int
	MaximumBattleCount int
	AttackWinCount     int
	AttackLossCount    int
	AttackStarCount    int
	DefenseWinCount    int
	DefenseLossCount   int
	DefenseStarCount   int
}
