package models

import (
	"time"

	"github.com/google/uuid"
)

type BasicPlayerRow struct {
	Tag      string
	Name     string
	LeagueID int
	TownHall int
}

type BattlelogCheckpoint struct {
	Tag       string
	Timestamp time.Time
}

type BattlelogIngest struct {
	Rows        []BattlelogRow
	Players     []BasicPlayerRow
	Checkpoints []BattlelogCheckpoint
}

type BattlelogRow struct {
	BattleID              uuid.UUID
	ArmyHash              uint64
	PlayerTag             string
	PlayerTH              uint8
	OpponentTag           string
	OpponentTH            uint8
	LeagueID              uint32
	BattleType            string
	Attack                bool
	Stars                 uint8
	DestructionPercentage uint8
	Gold                  uint32
	Elixir                uint32
	DarkElixir            uint32
	Timestamp             time.Time
	ArmyColumns           map[string]uint16
}

type ItemRollupKey struct {
	DayStart   time.Time
	PlayerTH   int16
	LeagueID   int32
	BattleType string
	ItemKey    string
}

type ItemHitrateCounts struct {
	Attacks    int64
	ZeroStars  int64
	OneStars   int64
	TwoStars   int64
	ThreeStars int64
}
