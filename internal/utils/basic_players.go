package utils

import (
	"context"

	"clashking_tracking/models"

	"github.com/jackc/pgx/v5"
)

const UpsertBasicPlayerSQL = `
	INSERT INTO basic_player (
		tag, name, league_id, clan_tag, townhall_level, trophies
	)
	VALUES ($1, $2, NULLIF($3, 0), $4, $5, $6)
	ON CONFLICT (tag) DO UPDATE SET
		name = EXCLUDED.name,
		league_id = COALESCE(EXCLUDED.league_id, basic_player.league_id),
		clan_tag = CASE WHEN $7 THEN EXCLUDED.clan_tag ELSE basic_player.clan_tag END,
		townhall_level = EXCLUDED.townhall_level,
		trophies = CASE WHEN EXCLUDED.trophies > 0 THEN EXCLUDED.trophies ELSE basic_player.trophies END
	WHERE
		basic_player.name IS DISTINCT FROM EXCLUDED.name OR
		basic_player.league_id IS DISTINCT FROM COALESCE(EXCLUDED.league_id, basic_player.league_id) OR
		($7 AND basic_player.clan_tag IS DISTINCT FROM EXCLUDED.clan_tag) OR
		basic_player.townhall_level IS DISTINCT FROM EXCLUDED.townhall_level OR
		(EXCLUDED.trophies > 0 AND basic_player.trophies IS DISTINCT FROM EXCLUDED.trophies)
`

// UpsertBasicPlayers is shared by ingesters that learn basic player facts
// while processing player, clan, battlelog, leaderboard, or scheduled data.
func UpsertBasicPlayers(ctx context.Context, tx pgx.Tx, players []models.BasicPlayerRow, domain string) error {
	_, err := UpsertBasicPlayersCount(ctx, tx, players, domain)
	return err
}

// UpsertBasicPlayersCount returns the number of rows PostgreSQL actually inserted
// or updated. Callers that only need best-effort profile persistence should use
// UpsertBasicPlayers.
func UpsertBasicPlayersCount(ctx context.Context, tx pgx.Tx, players []models.BasicPlayerRow, domain string) (int, error) {
	if len(players) == 0 {
		return 0, nil
	}
	batch := &pgx.Batch{}
	for _, player := range players {
		if player.Tag == "" || player.Name == "" || player.TownHall <= 0 {
			continue
		}
		clanTagKnown := player.ClanTagKnown || player.ClanTag != ""
		batch.Queue(UpsertBasicPlayerSQL,
			player.Tag,
			player.Name,
			player.LeagueID,
			clanTagValue(player.ClanTag, clanTagKnown),
			player.TownHall,
			player.Trophies,
			clanTagKnown,
		)
	}
	return SendBatchCount(ctx, tx, batch)
}

func clanTagValue(tag string, known bool) any {
	if !known || tag == "" {
		return nil
	}
	return tag
}

func SendBatch(ctx context.Context, tx pgx.Tx, batch *pgx.Batch) error {
	_, err := SendBatchCount(ctx, tx, batch)
	return err
}

func SendBatchCount(ctx context.Context, tx pgx.Tx, batch *pgx.Batch) (int, error) {
	results := tx.SendBatch(ctx, batch)
	defer results.Close()
	affected := 0
	for i := 0; i < batch.Len(); i++ {
		tag, err := results.Exec()
		if err != nil {
			return affected, err
		}
		affected += int(tag.RowsAffected())
	}
	return affected, nil
}
