package scripts

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"clashking_tracking/internal/wararchive"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CWLLevelCount struct {
	Level int   `json:"level"`
	Count int64 `json:"count"`
}

type CWLHitRate struct {
	Level      int   `json:"level"`
	Attacks    int64 `json:"attacks"`
	ThreeStars int64 `json:"three_stars"`
}

type CWLParticipationBucket struct {
	Season                string          `json:"season"`
	CWLLeagueID           int             `json:"cwl_league_id"`
	WarSize               int             `json:"war_size"`
	GroupCount            int64           `json:"group_count"`
	ClanCount             int64           `json:"clan_count"`
	RegisteredPlayerCount int64           `json:"registered_player_count"`
	TownhallCounts        []CWLLevelCount `json:"townhall_counts"`
	SameTHHitRates        []CWLHitRate    `json:"same_th_hitrates"`
	FinalizedWars         int64           `json:"finalized_wars"`
	ArchivedWars          int64           `json:"archived_wars"`
	RefreshedAt           string          `json:"refreshed_at,omitempty"`
}

type CWLParticipationReport struct {
	Season               string                   `json:"season"`
	Buckets              []CWLParticipationBucket `json:"buckets"`
	SkippedUnknownLeague int64                    `json:"skipped_unknown_league"`
	SkippedUnknownSize   int64                    `json:"skipped_unknown_size"`
	ArchiveReadFailures  int64                    `json:"archive_read_failures"`
}

type CWLArchiveLocator struct {
	PackID int64
	Offset int64
	Bytes  int
}

type CWLArchiveReader func(context.Context, CWLArchiveLocator) (wararchive.War, error)

const cwlFinalizedWarsSQL = `
    SELECT DISTINCT g.cwl_league_id,g.war_size,w.war_id,w.archive_pack_id,w.archive_offset,w.archive_compressed_bytes
    FROM cwl_groups g
    CROSS JOIN LATERAL jsonb_array_elements(g.rounds) AS round(value)
    CROSS JOIN LATERAL jsonb_array_elements_text(round.value) AS tag(value)
    JOIN wars w ON w.war_tag=tag.value AND w.war_type='cwl' AND lower(w.state) IN ('warended','ended')
    WHERE g.season >= $1 AND g.season < $2 AND g.cwl_league_id > 48000000 AND g.war_size BETWEEN 1 AND 50
    AND w.end_time >= ($1::text || '-01')::date - interval '7 days' AND w.end_time < ($2::text || '-01')::date + interval '7 days'`

const cwlDuplicateCountsSQL = `
    WITH eligible AS (
      SELECT cwl_id FROM cwl_groups
      WHERE season >= $1 AND season < $2 AND cwl_league_id > 48000000 AND war_size BETWEEN 1 AND 50
    ), duplicate_clans AS (
      SELECT clan_tag FROM eligible JOIN cwl_group_clans USING (cwl_id)
      GROUP BY clan_tag HAVING count(DISTINCT cwl_id) > 1
    ), duplicate_members AS (
      SELECT tag FROM eligible JOIN cwl_group_members USING (cwl_id)
      GROUP BY tag HAVING count(DISTINCT cwl_id) > 1
    )
    SELECT (SELECT count(*) FROM duplicate_clans), (SELECT count(*) FROM duplicate_members)`

func CWLSeasonLabel(value string) (string, error) {
	if len(value) < 7 || value[4] != '-' || value[5] < '0' || value[5] > '1' || value[6] < '0' || value[6] > '9' {
		return "", fmt.Errorf("invalid CWL season %q", value)
	}
	month := value[5:7]
	if month < "01" || month > "12" {
		return "", fmt.Errorf("invalid CWL season %q", value)
	}
	for _, ch := range value[:4] {
		if ch < '0' || ch > '9' {
			return "", fmt.Errorf("invalid CWL season %q", value)
		}
	}
	return value[:7], nil
}

func RebuildCWLParticipation(ctx context.Context, pool *pgxpool.Pool, season string, readArchive CWLArchiveReader) (CWLParticipationReport, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return CWLParticipationReport{}, err
	}
	defer tx.Rollback(ctx)
	if season == "" {
		if err := tx.QueryRow(ctx, `SELECT max(left(season,7)) FROM cwl_groups WHERE season ~ '^[0-9]{4}-(0[1-9]|1[0-2])'`).Scan(&season); err != nil {
			return CWLParticipationReport{}, err
		}
	}
	label, err := CWLSeasonLabel(season)
	if err != nil {
		return CWLParticipationReport{}, err
	}
	if season != label {
		return CWLParticipationReport{}, fmt.Errorf("season must use YYYY-MM")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('cwl_participation'), hashtext($1))`, label); err != nil {
		return CWLParticipationReport{}, err
	}
	report := CWLParticipationReport{Season: label, Buckets: []CWLParticipationBucket{}}
	if err := tx.QueryRow(ctx, `SELECT count(*) FILTER (WHERE cwl_league_id IS NULL OR cwl_league_id <= 48000000), count(*) FILTER (WHERE cwl_league_id > 48000000 AND (war_size IS NULL OR war_size NOT BETWEEN 1 AND 50)) FROM cwl_groups WHERE season >= $1 AND season < $2`, label, nextCWLSeason(label)).Scan(&report.SkippedUnknownLeague, &report.SkippedUnknownSize); err != nil {
		return report, err
	}
	var duplicatedClans, duplicatedMembers int64
	if err := tx.QueryRow(ctx, cwlDuplicateCountsSQL, label, nextCWLSeason(label)).Scan(&duplicatedClans, &duplicatedMembers); err != nil {
		return report, err
	}
	if duplicatedClans > 0 || duplicatedMembers > 0 {
		return report, fmt.Errorf("CWL %s has %d clans and %d players in multiple eligible groups; participation was not replaced", label, duplicatedClans, duplicatedMembers)
	}
	rows, err := tx.Query(ctx, `
		WITH eligible AS (
		 SELECT cwl_id,cwl_league_id,war_size FROM cwl_groups
		 WHERE season >= $1 AND season < $2 AND cwl_league_id > 48000000 AND war_size BETWEEN 1 AND 50
		), clans AS (
		 SELECT e.cwl_league_id,e.war_size,count(*) AS n FROM eligible e JOIN cwl_group_clans c USING(cwl_id) GROUP BY 1,2
		), members AS (
		 SELECT e.cwl_league_id,e.war_size,m.cwl_id,m.tag,max(m.town_hall) AS town_hall
		 FROM eligible e JOIN cwl_group_members m USING(cwl_id) GROUP BY 1,2,3,4
		), member_totals AS (
		 SELECT cwl_league_id,war_size,count(*) AS n FROM members GROUP BY 1,2
		), townhall_totals AS (
		 SELECT cwl_league_id,war_size,jsonb_agg(jsonb_build_object('level',town_hall,'count',n) ORDER BY town_hall DESC) AS counts
		 FROM (SELECT cwl_league_id,war_size,town_hall,count(*) AS n FROM members WHERE town_hall > 0 GROUP BY 1,2,3) t GROUP BY 1,2
		)
		SELECT e.cwl_league_id,e.war_size,count(*)::bigint,coalesce(c.n,0)::bigint,coalesce(m.n,0)::bigint,coalesce(t.counts,'[]'::jsonb)
		FROM eligible e LEFT JOIN clans c USING(cwl_league_id,war_size) LEFT JOIN member_totals m USING(cwl_league_id,war_size) LEFT JOIN townhall_totals t USING(cwl_league_id,war_size)
		GROUP BY e.cwl_league_id,e.war_size,c.n,m.n,t.counts ORDER BY e.cwl_league_id,e.war_size`, label, nextCWLSeason(label))
	if err != nil {
		return report, err
	}
	byBucket := map[[2]int]*CWLParticipationBucket{}
	for rows.Next() {
		var b CWLParticipationBucket
		var townhallJSON []byte
		if err := rows.Scan(&b.CWLLeagueID, &b.WarSize, &b.GroupCount, &b.ClanCount, &b.RegisteredPlayerCount, &townhallJSON); err != nil {
			rows.Close()
			return report, err
		}
		if err := json.Unmarshal(townhallJSON, &b.TownhallCounts); err != nil {
			rows.Close()
			return report, err
		}
		b.Season = label
		byBucket[[2]int{b.CWLLeagueID, b.WarSize}] = &b
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return report, err
	}
	rows.Close()
	warRows, err := tx.Query(ctx, cwlFinalizedWarsSQL, label, nextCWLSeason(label))
	if err != nil {
		return report, err
	}
	seenWars := map[[3]int64]bool{}
	for warRows.Next() {
		var league, size int
		var id int64
		var packID, offset *int64
		var compressed *int
		if err := warRows.Scan(&league, &size, &id, &packID, &offset, &compressed); err != nil {
			warRows.Close()
			return report, err
		}
		b := byBucket[[2]int{league, size}]
		if b == nil || seenWars[[3]int64{int64(league), int64(size), id}] {
			continue
		}
		seenWars[[3]int64{int64(league), int64(size), id}] = true
		b.FinalizedWars++
		if league < 48000019 || league > 48000022 || packID == nil || offset == nil || compressed == nil || readArchive == nil {
			continue
		}
		war, err := readArchive(ctx, CWLArchiveLocator{*packID, *offset, *compressed})
		if err != nil {
			report.ArchiveReadFailures++
			continue
		}
		b.ArchivedWars++
		if league >= 48000019 && league <= 48000022 {
			addCWLSameTHHits(b, war)
		}
	}
	if err := warRows.Err(); err != nil {
		warRows.Close()
		return report, err
	}
	warRows.Close()
	for _, b := range byBucket {
		if b.ArchivedWars == 0 || b.CWLLeagueID < 48000019 || b.CWLLeagueID > 48000022 {
			b.SameTHHitRates = nil
		} else if b.SameTHHitRates == nil {
			b.SameTHHitRates = []CWLHitRate{}
		}
		sort.Slice(b.SameTHHitRates, func(i, j int) bool { return b.SameTHHitRates[i].Level > b.SameTHHitRates[j].Level })
		report.Buckets = append(report.Buckets, *b)
	}
	sort.Slice(report.Buckets, func(i, j int) bool {
		if report.Buckets[i].CWLLeagueID != report.Buckets[j].CWLLeagueID {
			return report.Buckets[i].CWLLeagueID < report.Buckets[j].CWLLeagueID
		}
		return report.Buckets[i].WarSize < report.Buckets[j].WarSize
	})
	if _, err = tx.Exec(ctx, `DELETE FROM cwl_participation WHERE season=$1`, label); err != nil {
		return report, err
	}
	for _, b := range report.Buckets {
		towns, _ := json.Marshal(b.TownhallCounts)
		var hits any
		if b.SameTHHitRates != nil {
			hits, _ = json.Marshal(b.SameTHHitRates)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO cwl_participation(season,cwl_league_id,war_size,group_count,clan_count,registered_player_count,townhall_counts,same_th_hitrates,finalized_wars,archived_wars) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, b.Season, b.CWLLeagueID, b.WarSize, b.GroupCount, b.ClanCount, b.RegisteredPlayerCount, towns, hits, b.FinalizedWars, b.ArchivedWars); err != nil {
			return report, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return report, err
	}
	return report, nil
}

func addCWLSameTHHits(bucket *CWLParticipationBucket, war wararchive.War) {
	for _, pair := range [][2]wararchive.Clan{{war.Clan, war.Opponent}, {war.Opponent, war.Clan}} {
		defenders := map[string]int{}
		for _, m := range pair[1].Members {
			defenders[m.Tag] = m.TownhallLevel
		}
		for _, m := range pair[0].Members {
			if m.TownhallLevel <= 0 {
				continue
			}
			for _, a := range m.Attacks {
				if defenders[a.DefenderTag] != m.TownhallLevel {
					continue
				}
				found := false
				for i := range bucket.SameTHHitRates {
					if bucket.SameTHHitRates[i].Level == m.TownhallLevel {
						bucket.SameTHHitRates[i].Attacks++
						if a.Stars == 3 {
							bucket.SameTHHitRates[i].ThreeStars++
						}
						found = true
						break
					}
				}
				if !found {
					stars := int64(0)
					if a.Stars == 3 {
						stars = 1
					}
					bucket.SameTHHitRates = append(bucket.SameTHHitRates, CWLHitRate{m.TownhallLevel, 1, stars})
				}
			}
		}
	}
}

func nextCWLSeason(label string) string {
	if strings.HasSuffix(label, "-12") {
		return fmt.Sprintf("%04d-01", mustYear(label)+1)
	}
	return fmt.Sprintf("%04d-%02d", mustYear(label), mustMonth(label)+1)
}
func mustYear(label string) int  { var year int; fmt.Sscanf(label[:4], "%d", &year); return year }
func mustMonth(label string) int { var month int; fmt.Sscanf(label[5:7], "%d", &month); return month }
