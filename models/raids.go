package models

import "time"

// RaidWeekendRow is one capital raid weekend for a clan, upserted into
// raid_weekends (PK clan_tag, start_time). The jsonb columns mirror the CoC
// raid log entry: members/attack_log/defense_log are the raw arrays and data is
// the full entry.
type RaidWeekendRow struct {
	ClanTag          string
	StartTime        time.Time
	EndTime          time.Time
	State            string
	TotalAttacks     int
	CapitalTotalLoot int
	RaidsCompleted   int
	OffensiveReward  int
	DefensiveReward  int
	Members          []byte // json array
	AttackLog        []byte // json array
	DefenseLog       []byte // json array
	Data             []byte // full raid log entry json
}

// CapitalRaidMemberRow is one member's participation in a raid weekend, upserted
// into capital_raid_members (PK clan_tag, start_time, player_tag). Data holds the
// raw CoC raid member object.
type CapitalRaidMemberRow struct {
	ClanTag                string
	StartTime              time.Time
	PlayerTag              string
	PlayerName             string
	AttackCount            int
	AttackLimit            int
	BonusAttackLimit       int
	CapitalResourcesLooted int
	Data                   []byte // raw raid member object json
}
