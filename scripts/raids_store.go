package scripts

import (
	"context"
	"encoding/json"
	"time"

	"clashking_tracking/internal/platform"
	"clashking_tracking/internal/utils"
	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
)

// timescaleRaidStore persists capital raid weekends to Timescale. Methods are
// nil-safe: when the domain runs against a mock/dry-run DB the store is nil and
// raids are only cached in Mongo + published as events (the existing behaviour).
type timescaleRaidStore struct {
	pool *pgxpool.Pool
}

func newRaidStore(ctx context.Context, app *platform.App) (*timescaleRaidStore, error) {
	if app.Config.MockDB || app.Config.DryRun || app.Config.TimescaleURL == "" {
		return nil, nil
	}
	pool, err := pgxpool.New(ctx, app.Config.TimescaleURL)
	if err != nil {
		return nil, err
	}
	return &timescaleRaidStore{pool: pool}, nil
}

func (s *timescaleRaidStore) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// StoreRaid upserts a raid weekend and its members in a single transaction. raw
// is the snapshot payload already marshalled by the caller (the full CoC raid
// object); it is reused for the data column instead of re-marshalling entry.
func (s *timescaleRaidStore) StoreRaid(ctx context.Context, clanTag string, entry clashy.RaidLogEntry, raw []byte) error {
	if s == nil || s.pool == nil {
		return nil
	}
	row, members := raidRowsFromEntry(clanTag, entry, raw)
	if row.ClanTag == "" || row.StartTime.IsZero() {
		return nil
	}
	ctx, span := platform.StartSpan(ctx, "timescale.raid_weekends.upsert",
		attribute.String("domain", botClansDomainName),
		attribute.String("clan_tag", clanTag),
		attribute.Int("write.members", len(members)),
	)
	defer span.End()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	defer tx.Rollback(ctx)
	if err := upsertRaidWeekend(ctx, tx, row); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	if err := upsertCapitalRaidMembers(ctx, tx, members); err != nil {
		platform.RecordSpanError(span, err)
		span.SetAttributes(platform.SpanErrorStatus(err))
		return err
	}
	err = tx.Commit(ctx)
	platform.RecordSpanError(span, err)
	span.SetAttributes(platform.SpanErrorStatus(err))
	return err
}

func raidRowsFromEntry(
	clanTag string,
	entry clashy.RaidLogEntry,
	raw []byte,
) (models.RaidWeekendRow, []models.CapitalRaidMemberRow) {
	var start, end time.Time
	if entry.StartTime != nil {
		start = entry.StartTime.Time
	}
	if entry.EndTime != nil {
		end = entry.EndTime.Time
	}
	data := raw
	if len(data) == 0 {
		data, _ = json.Marshal(entry)
	}
	row := models.RaidWeekendRow{
		ClanTag:          clanTag,
		StartTime:        start,
		EndTime:          end,
		State:            entry.State,
		TotalAttacks:     entry.AttackCount,
		CapitalTotalLoot: entry.TotalLoot,
		RaidsCompleted:   entry.CompletedRaidCount,
		OffensiveReward:  entry.OffensiveReward,
		DefensiveReward:  entry.DefensiveReward,
		Members:          jsonArray(entry.Members),
		AttackLog:        jsonArray(entry.AttackLog),
		DefenseLog:       jsonArray(entry.DefenseLog),
		Data:             data,
	}
	memberRows := make([]models.CapitalRaidMemberRow, 0, len(entry.Members))
	for _, m := range entry.Members {
		md, _ := json.Marshal(m)
		memberRows = append(memberRows, models.CapitalRaidMemberRow{
			ClanTag:                clanTag,
			StartTime:              start,
			PlayerTag:              m.Tag,
			PlayerName:             m.Name,
			AttackCount:            m.AttackCount,
			AttackLimit:            m.AttackLimit,
			BonusAttackLimit:       m.BonusAttackLimit,
			CapitalResourcesLooted: m.CapitalResourcesLooted,
			Data:                   md,
		})
	}
	return row, memberRows
}

// jsonArray marshals a slice for a jsonb column, normalising nil/empty to '[]'.
func jsonArray(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 || string(b) == "null" {
		return []byte("[]")
	}
	return b
}

func upsertRaidWeekend(ctx context.Context, tx pgx.Tx, row models.RaidWeekendRow) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO raid_weekends (
			clan_tag, start_time, end_time, state, total_attacks, capital_total_loot,
			raids_completed, offensive_reward, defensive_reward,
			members, attack_log, defense_log, data, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb, now())
		ON CONFLICT (clan_tag, start_time) DO UPDATE SET
			end_time = EXCLUDED.end_time,
			state = EXCLUDED.state,
			total_attacks = EXCLUDED.total_attacks,
			capital_total_loot = EXCLUDED.capital_total_loot,
			raids_completed = EXCLUDED.raids_completed,
			offensive_reward = EXCLUDED.offensive_reward,
			defensive_reward = EXCLUDED.defensive_reward,
			members = EXCLUDED.members,
			attack_log = EXCLUDED.attack_log,
			defense_log = EXCLUDED.defense_log,
			data = EXCLUDED.data,
			updated_at = now()
	`, row.ClanTag, row.StartTime, row.EndTime, row.State, row.TotalAttacks, row.CapitalTotalLoot,
		row.RaidsCompleted, row.OffensiveReward, row.DefensiveReward,
		string(row.Members), string(row.AttackLog), string(row.DefenseLog), string(row.Data))
	return err
}

func upsertCapitalRaidMembers(ctx context.Context, tx pgx.Tx, rows []models.CapitalRaidMemberRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		if row.PlayerTag == "" {
			continue
		}
		batch.Queue(`
			INSERT INTO capital_raid_members (
				clan_tag, start_time, player_tag, player_name, attack_count,
				attack_limit, bonus_attack_limit, capital_resources_looted, data
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
			ON CONFLICT (clan_tag, start_time, player_tag) DO UPDATE SET
				player_name = EXCLUDED.player_name,
				attack_count = EXCLUDED.attack_count,
				attack_limit = EXCLUDED.attack_limit,
				bonus_attack_limit = EXCLUDED.bonus_attack_limit,
				capital_resources_looted = EXCLUDED.capital_resources_looted,
				data = EXCLUDED.data
		`, row.ClanTag, row.StartTime, row.PlayerTag, row.PlayerName, row.AttackCount,
			row.AttackLimit, row.BonusAttackLimit, row.CapitalResourcesLooted, string(row.Data))
	}
	return utils.SendBatch(ctx, tx, batch)
}
