package wararchive

import (
	"fmt"
	"strconv"
)

// CWLStats is the additive v1 histogram consumed by the API's stats.cwl reader.
// A nil PackStats.CWL means it has not been computed; it must not mean zero hits.
type CWLStats struct {
	Version  int                    `json:"version"`
	Coverage CWLCoverage            `json:"coverage"`
	ByDay    map[string]CWLDayStats `json:"byDay"`
}

type CWLCoverage struct {
	Complete              bool `json:"complete"`
	WarCount              int  `json:"warCount"`
	UnknownLeagueWarCount int  `json:"unknownLeagueWarCount"`
	LeagueComplete        bool `json:"leagueComplete"`
}

type CWLDayStats struct {
	ByLeague map[string]map[string]HitRateStats `json:"byLeague"`
}

func NewCWLStats() *CWLStats {
	return &CWLStats{Version: 1, Coverage: CWLCoverage{Complete: true, LeagueComplete: true}, ByDay: map[string]CWLDayStats{}}
}

// AddWar processes one CWL frame exactly once. leagueID must come from a unique
// stored group attribution, never the clan's current league. Zero means unknown.
// Invalid frames invalidate coverage without publishing a partial histogram.
func (s *CWLStats) AddWar(war War, leagueID int) error {
	s.Coverage.WarCount++
	key := "unknown"
	if leagueID > 0 {
		key = strconv.Itoa(leagueID)
	} else {
		s.Coverage.UnknownLeagueWarCount++
		s.Coverage.LeagueComplete = false
	}
	invalid := func(reason string) error {
		s.Coverage.Complete = false
		return fmt.Errorf("CWL statistics: %s", reason)
	}
	if war.EndTime.IsZero() {
		return invalid("missing end time")
	}
	histogram := map[string]HitRateStats{}
	for _, side := range [][2]Clan{{war.Clan, war.Opponent}, {war.Opponent, war.Clan}} {
		defenders := map[string]int{}
		for _, defender := range side[1].Members {
			if defender.Tag == "" || defender.TownhallLevel < 1 {
				return invalid("invalid defender")
			}
			if _, exists := defenders[defender.Tag]; exists {
				return invalid("duplicate defender")
			}
			defenders[defender.Tag] = defender.TownhallLevel
		}
		for _, attacker := range side[0].Members {
			if attacker.TownhallLevel < 1 {
				return invalid("invalid attacker town hall")
			}
			for _, attack := range attacker.Attacks {
				defenderTH, exists := defenders[attack.DefenderTag]
				if !exists {
					return invalid("missing attack defender")
				}
				if attack.Stars < 0 || attack.Stars > 3 || attack.DestructionPercentage < 0 || attack.DestructionPercentage > 100 || attack.Duration < 0 || attack.Stars == 3 && attack.DestructionPercentage != 100 {
					return invalid("invalid attack outcome")
				}
				matchup := fmt.Sprintf("%d:%d", attacker.TownhallLevel, defenderTH)
				hit := histogram[matchup]
				hit.Attacks++
				switch attack.Stars {
				case 3:
					hit.ThreeStars.Attacks++
					hit.ThreeStars.DurationSeconds += int64(attack.Duration)
				case 2:
					hit.TwoStars.Attacks++
					hit.TwoStars.DestructionPercent += int64(attack.DestructionPercentage)
					hit.TwoStars.DurationSeconds += int64(attack.Duration)
				case 1:
					hit.OneStars.Attacks++
					hit.OneStars.DestructionPercent += int64(attack.DestructionPercentage)
					hit.OneStars.DurationSeconds += int64(attack.Duration)
				case 0:
					hit.ZeroStars.Attacks++
					hit.ZeroStars.DestructionPercent += int64(attack.DestructionPercentage)
					hit.ZeroStars.DurationSeconds += int64(attack.Duration)
				}
				histogram[matchup] = hit
			}
		}
	}
	dayKey := war.EndTime.UTC().Format("2006-01-02")
	day := s.ByDay[dayKey]
	if day.ByLeague == nil {
		day.ByLeague = map[string]map[string]HitRateStats{}
	}
	if day.ByLeague[key] == nil {
		day.ByLeague[key] = map[string]HitRateStats{}
	}
	for matchup, addition := range histogram {
		hit := day.ByLeague[key][matchup]
		hit.Attacks += addition.Attacks
		addOutcome := func(target *StarOutcomeStats, value StarOutcomeStats) {
			target.Attacks += value.Attacks
			target.DestructionPercent += value.DestructionPercent
			target.DurationSeconds += value.DurationSeconds
		}
		addOutcome(&hit.ZeroStars, addition.ZeroStars)
		addOutcome(&hit.OneStars, addition.OneStars)
		addOutcome(&hit.TwoStars, addition.TwoStars)
		hit.ThreeStars.Attacks += addition.ThreeStars.Attacks
		hit.ThreeStars.DurationSeconds += addition.ThreeStars.DurationSeconds
		day.ByLeague[key][matchup] = hit
	}
	s.ByDay[dayKey] = day
	return nil
}
