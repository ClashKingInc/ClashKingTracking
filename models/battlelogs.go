package models

import (
	"time"
)

type BasicPlayerRow struct {
	Tag              string
	Name             string
	LeagueID         int
	LeagueGroupID    string
	LeagueSeasonID   int64
	LeagueGroupKnown bool
	ClanTag          string
	ClanTagKnown     bool
	TownHall         int
	Trophies         int
}

type BattlelogCheckpoint struct {
	Tag       string
	Timestamp time.Time
}

type BattlelogIngest struct {
	Rows        []BattlelogRow
	Checkpoints []BattlelogCheckpoint
}

type BattlelogRow struct {
	ArmyShareCode         string
	ArmyHash              [32]byte
	PlayerTag             string
	OpponentTag           string
	OpponentTH            uint8
	BattleType            string
	Attack                bool
	Stars                 uint8
	DestructionPercentage uint8
	Gold                  uint32
	Elixir                uint32
	DarkElixir            uint32
	Duration              uint16
	Timestamp             time.Time
	ArmyColumns           map[string]uint16
}
