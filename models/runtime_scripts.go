package models

import "time"

type GiveawayRow struct {
	ID        string
	ServerID  string
	Status    string
	Updated   bool
	StartTime time.Time
	EndTime   time.Time
	Data      any
}

type GiveawayTransition struct {
	Kind     string
	From     string
	To       string
	Row      GiveawayRow
	Event    Event
	EventDue bool
}

type PlayerTrophyHistoryRow struct {
	LocationID     string
	Date           time.Time
	PlayerTag      string
	PlayerName     string
	ExpLevel       int
	Trophies       int
	AttackWins     int
	DefenseWins    int
	Rank           int
	PreviousRank   *int
	ClanTag        *string
	ClanName       *string
	ClanBadgeToken *string
	LeagueID       *int
}

type PlayerBuilderBaseTrophyHistoryRow struct {
	LocationID            string
	Date                  time.Time
	PlayerTag             string
	PlayerName            string
	ExpLevel              int
	BuilderBaseTrophies   int
	BuilderBaseBattleWins *int
	Rank                  int
	PreviousRank          *int
	ClanTag               *string
	ClanName              *string
	ClanBadgeToken        *string
	LeagueID              *int
}

type ClanTrophyHistoryRow struct {
	LocationID     string
	Date           time.Time
	ClanTag        string
	ClanName       string
	ClanBadgeToken string
	ClanLevel      int
	ClanPoints     int
	Members        int
	ClanLocationID *int
	Rank           int
	PreviousRank   *int
}

type ClanBuilderBaseTrophyHistoryRow struct {
	LocationID        string
	Date              time.Time
	ClanTag           string
	ClanName          string
	ClanBadgeToken    string
	ClanLevel         int
	BuilderBasePoints int
	Members           int
	ClanLocationID    *int
	Rank              int
	PreviousRank      *int
}

type ClanCapitalHistoryRow struct {
	LocationID     string
	Date           time.Time
	ClanTag        string
	ClanName       string
	ClanBadgeToken string
	ClanLevel      int
	CapitalPoints  int
	Members        int
	ClanLocationID *int
	Rank           int
	PreviousRank   *int
}

type LegendHistoryRow struct {
	Season         string
	PlayerTag      string
	PlayerName     string
	ExpLevel       int
	Trophies       int
	AttackWins     int
	DefenseWins    int
	Rank           int
	ClanTag        *string
	ClanName       *string
	ClanBadgeToken *string
	LeagueTierID   *int
}
