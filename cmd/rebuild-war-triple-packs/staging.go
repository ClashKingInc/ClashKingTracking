package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
)

// Temporary SQL state bounds Go memory regardless of archive size. It retains
// unique game identities and only each key's latest reset/qualified suffix.
const stagingSchema = `
CREATE TEMP TABLE replay_wars (
 identity text PRIMARY KEY,
 fingerprint text NOT NULL,
 war_id integer NOT NULL,
 war_end_time timestamptz NOT NULL
);
CREATE TEMP TABLE replay_state (
 player_tag text NOT NULL,
 townhall_level smallint NOT NULL,
 reset_end timestamptz,
 reset_identity text,
 reset_war_id integer,
 reset_player_name text,
 reset_clan_tag text,
 unknown_end timestamptz,
 unknown_identity text,
 latest_end timestamptz NOT NULL,
 latest_identity text NOT NULL,
 war_id integer NOT NULL,
 player_name text NOT NULL,
 clan_tag text NOT NULL,
 PRIMARY KEY(player_tag,townhall_level)
);
CREATE TEMP TABLE replay_wins (
 credited_triples smallint NOT NULL,
 player_tag text NOT NULL,
 townhall_level smallint NOT NULL,
 identity text NOT NULL,
 war_id integer NOT NULL,
 war_end_time timestamptz NOT NULL,
 player_name text NOT NULL,
 clan_tag text NOT NULL,
 PRIMARY KEY(player_tag,townhall_level,identity)
);
CREATE TEMP TABLE incoming_wars (LIKE replay_wars INCLUDING DEFAULTS);
CREATE TEMP TABLE new_wars (identity text PRIMARY KEY);
CREATE TEMP TABLE incoming_outcomes (
 player_tag text,
 townhall_level smallint,
 identity text,
 war_id integer,
 war_end_time timestamptz,
 qualified boolean,
 credited_triples smallint,
 player_name text,
 clan_tag text
);
`

func stage(ctx context.Context, conn *pgx.Conn, wars []stagedWar, outcomes []stagedOutcome, stats *replayStats) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `TRUNCATE incoming_wars,incoming_outcomes,new_wars`); err != nil {
		return err
	}
	rows := make([][]any, 0, len(wars))
	for _, war := range wars {
		rows = append(rows, []any{war.identity, war.fingerprint, war.id, war.end})
	}
	if _, err = tx.CopyFrom(ctx, pgx.Identifier{"incoming_wars"}, []string{"identity", "fingerprint", "war_id", "war_end_time"}, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	var conflict bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(
 SELECT 1 FROM incoming_wars GROUP BY identity HAVING count(DISTINCT fingerprint)>1
 UNION ALL
 SELECT 1 FROM incoming_wars incoming JOIN replay_wars stored USING(identity)
 WHERE incoming.fingerprint<>stored.fingerprint
 )`).Scan(&conflict); err != nil {
		return err
	}
	if conflict {
		return fmt.Errorf("canonical source versions disagree on roster or scoring; preview withheld")
	}
	tag, err := tx.Exec(ctx, `WITH inserted AS (
 INSERT INTO replay_wars SELECT DISTINCT ON(identity) identity,fingerprint,war_id,war_end_time
 FROM incoming_wars ORDER BY identity,war_id ON CONFLICT DO NOTHING RETURNING identity
 ) INSERT INTO new_wars SELECT identity FROM inserted`)
	if err != nil {
		return err
	}
	stats.CanonicalWars += tag.RowsAffected()
	stats.DuplicateRows += int64(len(wars)) - tag.RowsAffected()
	rows = rows[:0]
	for _, o := range outcomes {
		rows = append(rows, []any{o.member.Tag, o.member.TownhallLevel, o.war.identity, o.war.id, o.war.end, o.qualified, o.credits, o.member.Name, o.clan})
	}
	if _, err = tx.CopyFrom(ctx, pgx.Identifier{"incoming_outcomes"}, []string{"player_tag", "townhall_level", "identity", "war_id", "war_end_time", "qualified", "credited_triples", "player_name", "clan_tag"}, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
 DELETE FROM incoming_outcomes WHERE identity NOT IN(SELECT identity FROM new_wars);
 INSERT INTO replay_state(player_tag,townhall_level,latest_end,latest_identity,war_id,player_name,clan_tag)
 SELECT DISTINCT ON(player_tag,townhall_level) player_tag,townhall_level,war_end_time,identity,war_id,player_name,clan_tag
 FROM incoming_outcomes ORDER BY player_tag,townhall_level,war_end_time DESC,identity DESC
 ON CONFLICT(player_tag,townhall_level) DO UPDATE SET
 latest_end=EXCLUDED.latest_end,latest_identity=EXCLUDED.latest_identity,war_id=EXCLUDED.war_id,
 player_name=EXCLUDED.player_name,clan_tag=EXCLUDED.clan_tag
 WHERE (EXCLUDED.latest_end,EXCLUDED.latest_identity)>(replay_state.latest_end,replay_state.latest_identity);
 UPDATE replay_state state SET reset_end=source.war_end_time,reset_identity=source.identity,
 reset_war_id=source.war_id,reset_player_name=source.player_name,reset_clan_tag=source.clan_tag
 FROM(SELECT DISTINCT ON(player_tag,townhall_level) player_tag,townhall_level,war_end_time,identity,war_id,player_name,clan_tag
 FROM incoming_outcomes WHERE qualified=false ORDER BY player_tag,townhall_level,war_end_time DESC,identity DESC)source
 WHERE state.player_tag=source.player_tag AND state.townhall_level=source.townhall_level
 AND(state.reset_end IS NULL OR(source.war_end_time,source.identity)>(state.reset_end,state.reset_identity));
 UPDATE replay_state state SET unknown_end=source.war_end_time,unknown_identity=source.identity
 FROM(SELECT DISTINCT ON(player_tag,townhall_level) player_tag,townhall_level,war_end_time,identity
 FROM incoming_outcomes WHERE qualified IS NULL ORDER BY player_tag,townhall_level,war_end_time DESC,identity DESC)source
 WHERE state.player_tag=source.player_tag AND state.townhall_level=source.townhall_level
 AND(state.unknown_end IS NULL OR(source.war_end_time,source.identity)>(state.unknown_end,state.unknown_identity));
 DELETE FROM replay_wins win USING replay_state state,(SELECT DISTINCT player_tag,townhall_level FROM incoming_outcomes)changed
 WHERE state.player_tag=changed.player_tag AND state.townhall_level=changed.townhall_level
 AND win.player_tag=state.player_tag AND win.townhall_level=state.townhall_level
 AND(win.war_end_time,win.identity)<=(state.reset_end,state.reset_identity);
 INSERT INTO replay_wins
 SELECT DISTINCT ON(source.player_tag,source.townhall_level,source.identity)
 source.credited_triples,source.player_tag,source.townhall_level,source.identity,source.war_id,source.war_end_time,source.player_name,source.clan_tag
 FROM incoming_outcomes source JOIN replay_state state USING(player_tag,townhall_level)
 WHERE source.qualified=true AND(state.reset_end IS NULL OR(source.war_end_time,source.identity)>(state.reset_end,state.reset_identity))
 ON CONFLICT DO NOTHING;
 `)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func finish(ctx context.Context, conn *pgx.Conn, output string, stats *replayStats) error {
	var barrier any
	if stats.GlobalBarrier != nil {
		barrier = *stats.GlobalBarrier
	}
	// An unknown source is a barrier, not a made-up reset. A later actual reset
	// restores an exact suffix; a newer success alone cannot prove continuity.
	_, err := conn.Exec(ctx, `CREATE TEMP TABLE replay_preview AS
 SELECT state.*,
 (unknown_end IS NULL OR(reset_end,reset_identity)>(unknown_end,unknown_identity))
 AND($1::timestamptz IS NULL OR reset_end>$1) AS available,
 (SELECT COALESCE(sum(credited_triples),0) FROM replay_wins win WHERE win.player_tag=state.player_tag AND win.townhall_level=state.townhall_level)::integer AS streak
 FROM replay_state state`, barrier)
	if err != nil {
		return err
	}
	if err = conn.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE available),count(*) FILTER(WHERE available IS NOT TRUE),(SELECT count(*) FROM replay_wins) FROM replay_preview`).Scan(&stats.PlayerKeys, &stats.ExactKeys, &stats.UnavailableKeys, &stats.QualifiedSuffixRows); err != nil {
		return err
	}
	if stats.UnavailableKeys > 0 || stats.GlobalBarrier != nil {
		stats.ManifestScorable = false
	}
	f, err := os.OpenFile(filepath.Join(output, "preview.csv"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = conn.PgConn().CopyTo(ctx, f, `COPY(SELECT player_tag,townhall_level,CASE WHEN available THEN streak END AS streak,
 available IS TRUE AS available,latest_end,player_name,clan_tag,reset_end,unknown_end
 FROM replay_preview ORDER BY player_tag,townhall_level)TO STDOUT WITH CSV HEADER`)
	if err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	// Preserve actual event times and identities for reviewed application and
	// future finalization; a derived score alone cannot preserve reset boundaries.
	artifacts := []struct{ name, query string }{
		{"qualified-suffix.csv", `COPY(SELECT player_tag,townhall_level,war_id,war_end_time,identity,player_name,clan_tag,credited_triples
         FROM replay_wins ORDER BY player_tag,townhall_level,war_end_time,identity)TO STDOUT WITH CSV HEADER`},
		{"latest-resets.csv", `COPY(SELECT player_tag,townhall_level,reset_war_id AS war_id,reset_end AS war_end_time,
         reset_identity AS identity,reset_player_name AS player_name,reset_clan_tag AS clan_tag
         FROM replay_state WHERE reset_end IS NOT NULL ORDER BY player_tag,townhall_level)TO STDOUT WITH CSV HEADER`},
		{"coverage-barriers.csv", `COPY(SELECT player_tag,townhall_level,unknown_end,unknown_identity
         FROM replay_state WHERE unknown_end IS NOT NULL ORDER BY player_tag,townhall_level)TO STDOUT WITH CSV HEADER`},
	}
	for _, artifact := range artifacts {
		file, e := os.OpenFile(filepath.Join(output, artifact.name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_, e = conn.PgConn().CopyTo(ctx, file, artifact.query)
		if e == nil {
			e = file.Sync()
		}
		closeErr := file.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
	}
	fmt.Printf("preview physical=%d canonical=%d duplicates=%d keys=%d exact=%d unavailable=%d packs=%d bytes=%d at=%s\n", stats.PhysicalRows, stats.CanonicalWars, stats.DuplicateRows, stats.PlayerKeys, stats.ExactKeys, stats.UnavailableKeys, stats.Packs, stats.DownloadBytes, time.Now().UTC().Format(time.RFC3339))
	return nil
}
