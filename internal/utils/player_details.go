package utils

import (
	"context"
	"time"

	"clashking_tracking/models"

	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
)

func PlayerProfileFromClashy(player clashy.Player) models.PlayerProfileIngest {
	clanTag := ""
	clanTagKnown := true
	if player.Clan != nil {
		clanTag = player.Clan.Tag
	}
	ingest := models.PlayerProfileIngest{
		Player: models.BasicPlayerRow{
			Tag:          player.Tag,
			Name:         player.Name,
			LeagueID:     player.LeagueTier.ID,
			ClanTag:      clanTag,
			ClanTagKnown: clanTagKnown,
			TownHall:     player.TownHall,
			Trophies:     player.Trophies,
		},
		Troops:    make([]models.PlayerTroopRow, 0, len(player.Troops)),
		Spells:    make([]models.PlayerSpellRow, 0, len(player.Spells)),
		Heroes:    make([]models.PlayerHeroRow, 0, len(player.Heroes)),
		Equipment: make([]models.PlayerEquipmentRow, 0, len(player.HeroEquipment)),
	}
	for _, troop := range player.Troops {
		ingest.Troops = append(ingest.Troops, models.PlayerTroopRow{
			PlayerTag:          player.Tag,
			Name:               troop.Name,
			Level:              troop.Level,
			MaxLevel:           troop.MaxLevel,
			Village:            troop.Village,
			SuperTroopIsActive: troop.SuperTroopIsActive,
		})
	}
	for _, spell := range player.Spells {
		ingest.Spells = append(ingest.Spells, models.PlayerSpellRow{
			PlayerTag: player.Tag,
			Name:      spell.Name,
			Level:     spell.Level,
			MaxLevel:  spell.MaxLevel,
			Village:   spell.Village,
		})
	}
	for _, hero := range player.Heroes {
		ingest.Heroes = append(ingest.Heroes, models.PlayerHeroRow{
			PlayerTag: player.Tag,
			Name:      hero.Name,
			Level:     hero.Level,
			MaxLevel:  hero.MaxLevel,
			Village:   hero.Village,
		})
	}
	for _, equipment := range player.HeroEquipment {
		ingest.Equipment = append(ingest.Equipment, models.PlayerEquipmentRow{
			PlayerTag: player.Tag,
			Name:      equipment.Name,
			Level:     equipment.Level,
			MaxLevel:  equipment.MaxLevel,
			Village:   equipment.Village,
			Rarity:    equipment.Rarity,
		})
	}
	return ingest
}

func UpsertPlayerProfiles(
	ctx context.Context,
	tx pgx.Tx,
	ingests []models.PlayerProfileIngest,
	domain string,
	activityAt *time.Time,
) (int, error) {
	if len(ingests) == 0 {
		return 0, nil
	}
	players := make([]models.BasicPlayerRow, 0, len(ingests))
	tags := make([]string, 0, len(ingests))
	for _, ingest := range ingests {
		if ingest.Player.Tag == "" {
			continue
		}
		players = append(players, ingest.Player)
		tags = append(tags, ingest.Player.Tag)
	}
	affected, err := UpsertBasicPlayersCount(ctx, tx, players, domain)
	if err != nil {
		return affected, err
	}
	if activityAt != nil && len(tags) > 0 {
		tag, err := tx.Exec(ctx, `
			UPDATE basic_player
			SET battlelogs_tracking_ttl = $1
			WHERE tag = ANY($2::text[])
			  AND (battlelogs_tracking_ttl IS NULL OR battlelogs_tracking_ttl < $1)
		`, *activityAt, tags)
		if err != nil {
			return affected, err
		}
		affected += int(tag.RowsAffected())
	}
	if err := replacePlayerDetailRows(ctx, tx, ingests, tags); err != nil {
		return affected, err
	}
	return affected, nil
}

func DeletePlayers(ctx context.Context, tx pgx.Tx, tags []string) error {
	tags = nonEmptyTags(tags)
	if len(tags) == 0 {
		return nil
	}
	for _, table := range []string{"player_troops", "player_spells", "player_heroes", "player_equipment"} {
		if _, err := tx.Exec(ctx, "DELETE FROM "+table+" WHERE player_tag = ANY($1::text[])", tags); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `DELETE FROM basic_player WHERE tag = ANY($1::text[])`, tags)
	return err
}

func replacePlayerDetailRows(ctx context.Context, tx pgx.Tx, ingests []models.PlayerProfileIngest, tags []string) error {
	tags = nonEmptyTags(tags)
	if len(tags) == 0 {
		return nil
	}
	for _, table := range []string{"player_troops", "player_spells", "player_heroes", "player_equipment"} {
		if _, err := tx.Exec(ctx, "DELETE FROM "+table+" WHERE player_tag = ANY($1::text[])", tags); err != nil {
			return err
		}
	}
	batch := &pgx.Batch{}
	for _, ingest := range ingests {
		for _, row := range ingest.Troops {
			batch.Queue(`
				INSERT INTO player_troops (
					player_tag, name, level, max_level, village, super_troop_is_active
				)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (player_tag, name, village) DO UPDATE SET
					level = EXCLUDED.level,
					max_level = EXCLUDED.max_level,
					super_troop_is_active = EXCLUDED.super_troop_is_active
			`, row.PlayerTag, row.Name, row.Level, row.MaxLevel, row.Village, row.SuperTroopIsActive)
		}
		for _, row := range ingest.Spells {
			batch.Queue(`
				INSERT INTO player_spells (
					player_tag, name, level, max_level, village
				)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (player_tag, name, village) DO UPDATE SET
					level = EXCLUDED.level,
					max_level = EXCLUDED.max_level
			`, row.PlayerTag, row.Name, row.Level, row.MaxLevel, row.Village)
		}
		for _, row := range ingest.Heroes {
			batch.Queue(`
				INSERT INTO player_heroes (
					player_tag, name, level, max_level, village
				)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (player_tag, name, village) DO UPDATE SET
					level = EXCLUDED.level,
					max_level = EXCLUDED.max_level
			`, row.PlayerTag, row.Name, row.Level, row.MaxLevel, row.Village)
		}
		for _, row := range ingest.Equipment {
			batch.Queue(`
				INSERT INTO player_equipment (
					player_tag, name, level, max_level, village, rarity
				)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (player_tag, name, village) DO UPDATE SET
					level = EXCLUDED.level,
					max_level = EXCLUDED.max_level,
					rarity = EXCLUDED.rarity
			`, row.PlayerTag, row.Name, row.Level, row.MaxLevel, row.Village, row.Rarity)
		}
	}
	if batch.Len() == 0 {
		return nil
	}
	return SendBatch(ctx, tx, batch)
}

func nonEmptyTags(tags []string) []string {
	out := tags[:0]
	for _, tag := range tags {
		if tag != "" {
			out = append(out, tag)
		}
	}
	return out
}
