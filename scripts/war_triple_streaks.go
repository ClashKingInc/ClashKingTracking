package scripts

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"clashking_tracking/internal/wararchive"
	"github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
)

// WarTripleOutcome includes non-attackers so missed hits remain reset events.
type WarTripleOutcome struct {
	Member    wararchive.Member
	Clan      string
	Qualified bool
}

// ScoreWarTripleStreaks is shared by live finalization and full-pack replay.
// Walk the roster, never just attackers: zero or one attack must reset a streak.
func ScoreWarTripleStreaks(war wararchive.War, warType string) ([]WarTripleOutcome, error) {
	if warType != "random" || !strings.EqualFold(war.State, "warEnded") {
		return nil, nil
	}
	if clashy.IsFriendlyWarWindow(war.PreparationStartTime, war.StartTime, war.EndTime) {
		return nil, nil
	}
	if war.TeamSize <= 0 || len(war.Clan.Members) != war.TeamSize || len(war.Opponent.Members) != war.TeamSize {
		return nil, nil
	}
	var outcomes []WarTripleOutcome
	seen := map[string]bool{}
	for _, side := range []struct{ own, other wararchive.Clan }{{war.Clan, war.Opponent}, {war.Opponent, war.Clan}} {
		opponents := map[string]int{}
		for _, member := range side.other.Members {
			opponents[member.Tag] = member.TownhallLevel
		}
		for _, member := range side.own.Members {
			if member.Tag == "" || member.TownhallLevel < 1 || member.TownhallLevel > 99 || seen[member.Tag] {
				return nil, fmt.Errorf("invalid regular-war roster for streak scoring")
			}
			seen[member.Tag] = true
			qualifies := war.AttacksPerMember == 2 && len(member.Attacks) == 2
			orders := map[int]bool{}
			for _, attack := range member.Attacks {
				if attack.Stars != 3 || opponents[attack.DefenderTag] != member.TownhallLevel || (attack.Order > 0 && orders[attack.Order]) {
					qualifies = false
				}
				orders[attack.Order] = true
			}
			outcomes = append(outcomes, WarTripleOutcome{member, side.own.Tag, qualifies})
		}
	}
	return outcomes, nil
}

// RecordWarTripleStreaks shares the final archive transaction. Malformed
// rosters do not reset valid streaks; missed hits on complete rosters do.
func RecordWarTripleStreaks(ctx context.Context, tx pgx.Tx, warID int32, end time.Time, war wararchive.War, warType string) error {
	outcomes, err := ScoreWarTripleStreaks(war, warType)
	if err != nil {
		return err
	}
	// Lock keys in a stable order across both rosters. The transaction lock
	// serializes concurrent wars/backfills for a player without locking others.
	sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].Member.Tag < outcomes[j].Member.Tag })
	clanTag, opponentTag := war.Clan.Tag, war.Opponent.Tag
	if clanTag > opponentTag {
		clanTag, opponentTag = opponentTag, clanTag
	}
	for _, item := range outcomes {
		member := item.Member
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 731))`, fmt.Sprintf("%s:%d", member.Tag, member.TownhallLevel)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO war_triple_streak_results
            (player_tag,townhall_level,war_id,war_end_time,qualified,player_name,clan_tag)
            SELECT $1,$2,$3,$4,$5,$6,$7 WHERE NOT EXISTS (
                SELECT 1 FROM war_triple_streak_results result
                JOIN wars prior ON prior.war_id=result.war_id AND prior.end_time=result.war_end_time
                WHERE result.player_tag=$1 AND result.townhall_level=$2 AND result.war_end_time=$4
                  AND prior.clan_tag=$8 AND prior.opponent_tag=$9 AND prior.prep_time=$10
            ) ON CONFLICT DO NOTHING`, member.Tag, member.TownhallLevel, warID, end, item.Qualified, member.Name, item.Clan,
			clanTag, opponentTag, war.PreparationStartTime); err != nil {
			return err
		}
		// Recompute by game-war identity, not allocated SQL IDs. Older data can
		// contain duplicate IDs for one war; a later real war must not restore
		// their inflated score after the derived leaderboard is repaired.
		if _, err := tx.Exec(ctx, `WITH canonical AS MATERIALIZED (
            SELECT min(result.war_id) AS war_id,result.war_end_time,bool_and(result.qualified) AS qualified
            FROM war_triple_streak_results result
            JOIN wars war ON war.war_id=result.war_id AND war.end_time=result.war_end_time
            WHERE result.player_tag=$1 AND result.townhall_level=$2
            GROUP BY war.clan_tag,war.opponent_tag,war.prep_time,result.war_end_time
        ), latest AS (
            SELECT result.player_tag,result.townhall_level,result.player_name,result.clan_tag,canonical.war_end_time
            FROM canonical JOIN war_triple_streak_results result
              ON result.player_tag=$1 AND result.townhall_level=$2 AND result.war_id=canonical.war_id
            ORDER BY canonical.war_end_time DESC,canonical.war_id DESC LIMIT 1
        ), reset AS (
            SELECT war_end_time,war_id FROM canonical WHERE NOT qualified
            ORDER BY war_end_time DESC,war_id DESC LIMIT 1
        ), total AS (
            SELECT (count(*)*2)::integer AS streak FROM canonical r WHERE qualified
            AND (NOT EXISTS(SELECT 1 FROM reset) OR (r.war_end_time,r.war_id) > (SELECT war_end_time,war_id FROM reset))
        ) INSERT INTO war_triple_streaks (player_tag,townhall_level,streak,last_attack_time,player_name,clan_tag)
        SELECT player_tag,townhall_level,total.streak,war_end_time,player_name,clan_tag FROM latest CROSS JOIN total
        ON CONFLICT (player_tag,townhall_level) DO UPDATE SET streak=EXCLUDED.streak,
        last_attack_time=EXCLUDED.last_attack_time,player_name=EXCLUDED.player_name,clan_tag=EXCLUDED.clan_tag`, member.Tag, member.TownhallLevel); err != nil {
			return err
		}
	}
	return nil
}
