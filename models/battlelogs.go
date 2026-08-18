package models

import (
	"time"

	"github.com/google/uuid"
)

type BasicPlayerRow struct {
	Tag          string
	Name         string
	LeagueID     int
	ClanTag      string
	ClanTagKnown bool
	TownHall     int
	Trophies     int
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
	BattleID              uuid.UUID
	ArmyShareCode         string
	PlayerTag             string
	OpponentTag           string
	OpponentName          string
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
