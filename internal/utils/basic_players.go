package utils

import (
	"context"

	"clashking_tracking/internal/platform"
	"clashking_tracking/models"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
)

const UpsertBasicPlayerSQL = `
	INSERT INTO basic_player (
		tag, name, league_id, townhall_level
	)
	VALUES ($1, $2, NULLIF($3, 0), $4)
	ON CONFLICT (tag) DO UPDATE SET
		name = EXCLUDED.name,
		league_id = EXCLUDED.league_id,
		townhall_level = EXCLUDED.townhall_level
	WHERE
		basic_player.name IS DISTINCT FROM EXCLUDED.name OR
		basic_player.league_id IS DISTINCT FROM EXCLUDED.league_id OR
		basic_player.townhall_level IS DISTINCT FROM EXCLUDED.townhall_level
`

// UpsertBasicPlayers is shared by ingesters that learn basic player facts
// incidentally while processing clan, battlelog, bot-player, or war data.
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
	ctx, span := platform.StartSpan(ctx, "timescale.basic_player.upsert",
		attribute.String("domain", domain),
		attribute.String("operation", "upsert_basic_players"),
		attribute.Int("write.count", len(players)),
	)
	defer span.End()

	batch := &pgx.Batch{}
	for _, player := range players {
		if player.Tag == "" || player.Name == "" || player.TownHall <= 0 {
			continue
		}
		batch.Queue(UpsertBasicPlayerSQL,
			player.Tag,
			player.Name,
			player.LeagueID,
			player.TownHall,
		)
	}
	count, err := SendBatchCount(ctx, tx, batch)
	platform.RecordSpanError(span, err)
	span.SetAttributes(attribute.Int("rows.affected", count), platform.SpanErrorStatus(err))
	return count, err
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
