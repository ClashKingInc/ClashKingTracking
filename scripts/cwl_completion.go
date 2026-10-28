package scripts

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CWLCompletionCandidatesSQL trusts the official source's complete round list,
// not an inferred tournament shape. Every round must contain real unique tags
// and every referenced war must be final. Partial imported snapshots cannot be
// certified by this rule; callers must retain the full source round list.
// Dates, clan parity, and assumed counts of rounds/wars cannot end a group.
const CWLCompletionCandidatesSQL = `
WITH candidates AS (
 SELECT g.*, (SELECT count(*) FROM cwl_group_clans c WHERE c.cwl_id=g.cwl_id)::integer AS clan_count
 FROM cwl_groups g WHERE g.cwl_id = ANY($1::text[]) AND g.state <> 'ended'
), rounds AS (
 SELECT g.cwl_id, g.clan_count,
   CASE WHEN jsonb_typeof(r.value)='array' THEN r.value ELSE r.value->'warTags' END AS tags
 FROM candidates g CROSS JOIN LATERAL jsonb_array_elements(
   CASE WHEN jsonb_typeof(g.rounds)='array' THEN g.rounds ELSE '[]'::jsonb END) r(value)
), complete_rounds AS (
 SELECT cwl_id FROM rounds GROUP BY cwl_id, clan_count
 HAVING clan_count >= 2
 AND bool_and(CASE WHEN jsonb_typeof(tags)='array' THEN jsonb_array_length(tags)>0 ELSE false END)
), tags AS (
 SELECT r.cwl_id, t.value AS tag FROM rounds r JOIN complete_rounds c USING(cwl_id)
 CROSS JOIN LATERAL jsonb_array_elements_text(
   CASE WHEN jsonb_typeof(r.tags)='array' THEN r.tags ELSE '[]'::jsonb END) t(value)
), complete AS (
 SELECT t.cwl_id FROM tags t
 LEFT JOIN wars w ON w.war_type='cwl' AND w.war_tag=t.tag
 GROUP BY t.cwl_id
 HAVING count(*)=count(DISTINCT t.tag)
 AND bool_and(t.tag IS NOT NULL AND t.tag<>'' AND t.tag<>'#0'
   AND w.war_id IS NOT NULL AND lower(trim(w.state)) IN ('warended','ended'))
)
SELECT cwl_id FROM complete ORDER BY cwl_id`

func completeCWLGroups(ctx context.Context, tx pgx.Tx, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	// Serialize concurrent final-war commits per group. The following statement
	// gets a fresh READ COMMITTED snapshot after the preceding writer commits.
	rows, err := tx.Query(ctx, `SELECT cwl_id FROM cwl_groups WHERE cwl_id=ANY($1::text[]) ORDER BY cwl_id FOR UPDATE`, ids)
	if err != nil {
		return err
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE cwl_groups SET state='ended' WHERE cwl_id IN (`+CWLCompletionCandidatesSQL+`)`, ids)
	return err
}

// RepairCWLStates is the explicit one-time override, separate from automatic
// completion. It changes only state, without checking wars, dates, or rounds.
func RepairCWLStates(ctx context.Context, pool *pgxpool.Pool, apply bool) ([]string, error) {
	access := pgx.ReadOnly
	if apply {
		access = pgx.ReadWrite
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{AccessMode: access})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	query := `SELECT cwl_id FROM cwl_groups WHERE state IS DISTINCT FROM 'ended' ORDER BY cwl_id`
	if apply {
		query = `WITH changed AS (UPDATE cwl_groups SET state='ended' WHERE state IS DISTINCT FROM 'ended' RETURNING cwl_id)
		 SELECT cwl_id FROM changed ORDER BY cwl_id`
	}
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if apply {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

func (s *timescaleWarStore) CompleteCWLGroupsForClan(ctx context.Context, clanTag, season string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT g.cwl_id FROM cwl_groups g JOIN cwl_group_clans c USING(cwl_id)
	 WHERE c.clan_tag=$1 AND left(g.season,7)=$2 AND g.state<>'ended' ORDER BY g.cwl_id`, clanTag, season)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if err := completeCWLGroups(ctx, tx, ids); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
