package scripts

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A promotion poll has no CWL season in its payload. Only a completed group
// from the old league, in the event's month or the previous month, is eligible.
// LIMIT 2 lets us reject ambiguous group membership rather than choose one.
const discordCWLPromotionGroupsSQL = `
	SELECT g.cwl_id, g.season, g.rounds
	FROM cwl_groups g
	JOIN cwl_group_clans c ON c.cwl_id = g.cwl_id
	WHERE c.clan_tag = $1 AND g.cwl_league_id = $2 AND g.state = 'ended'
	  AND g.season >= $3 AND g.season < $4
	ORDER BY g.season DESC, g.cwl_id
	LIMIT 2`

// The group round's war tag is the provenance for each persisted war. The
// end-time bounds exclude an unrelated row if a tag were ever reused.
const discordCWLPromotionWarsSQL = `
	SELECT round.ordinality, w.war_id, w.clan_tag, w.opponent_tag,
	       w.clan_stars, w.opponent_stars,
	       w.clan_destruction_percentage, w.opponent_destruction_percentage
	FROM cwl_groups g
	CROSS JOIN LATERAL jsonb_array_elements(g.rounds) WITH ORDINALITY AS round(value, ordinality)
	CROSS JOIN LATERAL jsonb_array_elements_text(round.value) AS tag(value)
	JOIN wars w ON w.war_tag = tag.value AND w.war_type = 'cwl'
	  AND lower(w.state) IN ('warended', 'ended')
	  AND (w.clan_tag = $2 OR w.opponent_tag = $2)
	  AND w.end_time >= $3 AND w.end_time < $4 AND w.end_time <= $5
	WHERE g.cwl_id = $1
	ORDER BY round.ordinality, w.war_id`

type discordCWLPromotionWar struct {
	round         int
	warID         int
	clanTag       string
	opponentTag   string
	clanStars     int
	opponentStars int
	clanPercent   float64
	otherPercent  float64
}

// Every one of the seven standard CWL rounds must have exactly one persisted
// ended war for this clan. A partial or duplicated snapshot is not a season
// result and must not be displayed as one.
func discordCWLPromotionAggregate(clanTag, season string, rounds [][]string, wars []discordCWLPromotionWar) (map[string]any, bool) {
	if len(rounds) != 7 || len(wars) != len(rounds) {
		return nil, false
	}
	if _, err := CWLSeasonLabel(season); err != nil {
		return nil, false
	}
	wins, stars, percentSum := 0, 0, 0.0
	seenRounds := make(map[int]bool, len(rounds))
	seenWars := make(map[int]bool, len(wars))
	for _, war := range wars {
		if war.round < 1 || war.round > len(rounds) || seenRounds[war.round] || seenWars[war.warID] || war.warID <= 0 {
			return nil, false
		}
		seenRounds[war.round], seenWars[war.warID] = true, true
		var ownStars, otherStars int
		var ownPercent, otherPercent float64
		switch {
		case war.clanTag == clanTag && war.opponentTag != clanTag:
			ownStars, otherStars = war.clanStars, war.opponentStars
			ownPercent, otherPercent = war.clanPercent, war.otherPercent
		case war.opponentTag == clanTag && war.clanTag != clanTag:
			ownStars, otherStars = war.opponentStars, war.clanStars
			ownPercent, otherPercent = war.otherPercent, war.clanPercent
		default:
			return nil, false
		}
		if ownStars < 0 || otherStars < 0 || math.IsNaN(ownPercent) || math.IsInf(ownPercent, 0) || ownPercent < 0 || ownPercent > 100 || math.IsNaN(otherPercent) || math.IsInf(otherPercent, 0) || otherPercent < 0 || otherPercent > 100 {
			return nil, false
		}
		if ownStars > otherStars || (ownStars == otherStars && ownPercent > otherPercent) {
			wins++
		}
		stars += ownStars
		percentSum += ownPercent
	}
	label, _ := CWLSeasonLabel(season)
	return map[string]any{
		"season": label,
		"wars":   len(wars),
		"wins":   wins,
		"stars":  stars,
		// Mean of the seven clan-side war destruction percentages, not a
		// season-wide hit rate or a sum of percentages.
		"destruction_percentage": math.Round(percentSum/float64(len(wars))*10) / 10,
	}, true
}

func loadDiscordCWLPromotionDetails(ctx context.Context, pool *pgxpool.Pool, event mobileWarEvent) (map[string]any, error) {
	if event.Topic != "clan" || stringMapValue(event.Value, "type") != "clan_achievement" || stringMapValue(event.Value, "achievement") != "cwl_promotion" || event.Timestamp.IsZero() {
		return nil, nil
	}
	previous, _ := mapValue(event.Value["previous"])
	leagueID := intValue(previous["id"])
	if leagueID <= 48000000 {
		return nil, nil
	}
	eventAt := event.Timestamp.UTC()
	month := time.Date(eventAt.Year(), eventAt.Month(), 1, 0, 0, 0, 0, time.UTC)
	seasonFloor := month.AddDate(0, -1, 0).Format("2006-01")
	seasonCeiling := month.AddDate(0, 1, 0).Format("2006-01")
	rows, err := pool.Query(ctx, discordCWLPromotionGroupsSQL, event.ClanTag, leagueID, seasonFloor, seasonCeiling)
	if err != nil {
		return nil, fmt.Errorf("query completed CWL promotion group: %w", err)
	}
	type group struct {
		id, season string
		rounds     [][]string
	}
	var groups []group
	for rows.Next() {
		var candidate group
		if err := rows.Scan(&candidate.id, &candidate.season, &candidate.rounds); err != nil {
			rows.Close()
			return nil, err
		}
		groups = append(groups, candidate)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(groups) != 1 || len(groups[0].rounds) != 7 {
		return nil, nil
	}
	seasonLabel, err := CWLSeasonLabel(groups[0].season)
	if err != nil {
		return nil, nil
	}
	seasonStart, err := time.Parse("2006-01", seasonLabel)
	if err != nil {
		return nil, nil
	}
	warFloor := seasonStart.AddDate(0, 0, -7)
	warCeiling := seasonStart.AddDate(0, 1, 21)
	warRows, err := pool.Query(ctx, discordCWLPromotionWarsSQL, groups[0].id, event.ClanTag, warFloor, warCeiling, eventAt)
	if err != nil {
		return nil, fmt.Errorf("query completed CWL promotion wars: %w", err)
	}
	var wars []discordCWLPromotionWar
	for warRows.Next() {
		var war discordCWLPromotionWar
		if err := warRows.Scan(&war.round, &war.warID, &war.clanTag, &war.opponentTag, &war.clanStars, &war.opponentStars, &war.clanPercent, &war.otherPercent); err != nil {
			warRows.Close()
			return nil, err
		}
		wars = append(wars, war)
	}
	if err := warRows.Err(); err != nil {
		warRows.Close()
		return nil, err
	}
	warRows.Close()
	details, complete := discordCWLPromotionAggregate(event.ClanTag, groups[0].season, groups[0].rounds, wars)
	if !complete {
		return nil, nil
	}
	return details, nil
}
