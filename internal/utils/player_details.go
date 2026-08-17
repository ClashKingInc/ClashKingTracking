package utils

import (
	"context"
	"encoding/json"
	"sort"

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
		Heroes:       make([]models.PlayerHeroRow, 0, len(player.Heroes)),
		Equipment:    make([]models.PlayerEquipmentRow, 0, len(player.HeroEquipment)),
		Achievements: make([]models.PlayerAchievementRow, 0, len(player.Achievements)),
	}
	for _, hero := range player.Heroes {
		ingest.Heroes = append(ingest.Heroes, models.PlayerHeroRow{
			Name:     hero.Name,
			Level:    hero.Level,
			MaxLevel: hero.MaxLevel,
			Village:  hero.Village,
		})
	}
	for _, equipment := range player.HeroEquipment {
		ingest.Equipment = append(ingest.Equipment, models.PlayerEquipmentRow{
			Name:     equipment.Name,
			Level:    equipment.Level,
			MaxLevel: equipment.MaxLevel,
			Village:  equipment.Village,
			Rarity:   equipment.Rarity,
		})
	}
	for _, achievement := range player.Achievements {
		ingest.Achievements = append(ingest.Achievements, models.PlayerAchievementRow{
			Name:    achievement.Name,
			Stars:   achievement.Stars,
			Value:   achievement.Value,
			Target:  achievement.Target,
			Village: achievement.Village,
		})
	}

	// Canonical ordering makes stored JSON deterministic if the upstream order
	// ever changes, which keeps exports and weekly rollups reproducible.
	sort.Slice(ingest.Heroes, func(i, j int) bool {
		return detailKey(ingest.Heroes[i].Village, ingest.Heroes[i].Name) <
			detailKey(ingest.Heroes[j].Village, ingest.Heroes[j].Name)
	})
	sort.Slice(ingest.Equipment, func(i, j int) bool {
		return detailKey(ingest.Equipment[i].Village, ingest.Equipment[i].Name) <
			detailKey(ingest.Equipment[j].Village, ingest.Equipment[j].Name)
	})
	sort.Slice(ingest.Achievements, func(i, j int) bool {
		return detailKey(ingest.Achievements[i].Village, ingest.Achievements[i].Name) <
			detailKey(ingest.Achievements[j].Village, ingest.Achievements[j].Name)
	})
	return ingest
}

func detailKey(village, name string) string {
	return village + "\x00" + name
}

func UpsertPlayerProfiles(
	ctx context.Context,
	tx pgx.Tx,
	ingests []models.PlayerProfileIngest,
	domain string,
) (int, error) {
	if len(ingests) == 0 {
		return 0, nil
	}
	players := make([]models.BasicPlayerRow, 0, len(ingests))
	profiles := make([]models.PlayerProfileIngest, 0, len(ingests))
	for _, ingest := range ingests {
		if ingest.Player.Tag == "" {
			continue
		}
		players = append(players, ingest.Player)
		profiles = append(profiles, ingest)
	}
	affected, err := UpsertBasicPlayersCount(ctx, tx, players, domain)
	if err != nil {
		return affected, err
	}
	if err := upsertPlayerDetails(ctx, tx, profiles); err != nil {
		return affected, err
	}
	return affected, nil
}

func upsertPlayerDetails(ctx context.Context, tx pgx.Tx, ingests []models.PlayerProfileIngest) error {
	batch := &pgx.Batch{}
	for _, ingest := range ingests {
		if ingest.Player.Tag == "" || ingest.Player.TownHall <= 0 {
			continue
		}
		heroes, equipment, achievements, err := marshalPlayerDetails(ingest)
		if err != nil {
			return err
		}
		batch.Queue(`
			INSERT INTO player_profile_details (
				player_tag, townhall_level, heroes, equipment, achievements, observed_at
			)
			VALUES ($1, $2, $3::jsonb, $4::jsonb, $5::jsonb, now())
			ON CONFLICT (player_tag) DO UPDATE SET
				townhall_level = EXCLUDED.townhall_level,
				heroes = EXCLUDED.heroes,
				equipment = EXCLUDED.equipment,
				achievements = EXCLUDED.achievements,
				observed_at = EXCLUDED.observed_at
		`, ingest.Player.Tag, ingest.Player.TownHall, heroes, equipment, achievements)
	}
	if batch.Len() == 0 {
		return nil
	}
	return SendBatch(ctx, tx, batch)
}

func DeletePlayers(ctx context.Context, tx pgx.Tx, tags []string) error {
	tags = nonEmptyTags(tags)
	if len(tags) == 0 {
		return nil
	}
	// player_profile_details is removed by its ON DELETE CASCADE constraint.
	_, err := tx.Exec(ctx, `DELETE FROM basic_player WHERE tag = ANY($1::text[])`, tags)
	return err
}

func marshalPlayerDetails(ingest models.PlayerProfileIngest) ([]byte, []byte, []byte, error) {
	heroes := ingest.Heroes
	if heroes == nil {
		heroes = []models.PlayerHeroRow{}
	}
	equipment := ingest.Equipment
	if equipment == nil {
		equipment = []models.PlayerEquipmentRow{}
	}
	achievements := ingest.Achievements
	if achievements == nil {
		achievements = []models.PlayerAchievementRow{}
	}

	heroesJSON, err := json.Marshal(heroes)
	if err != nil {
		return nil, nil, nil, err
	}
	equipmentJSON, err := json.Marshal(equipment)
	if err != nil {
		return nil, nil, nil, err
	}
	achievementsJSON, err := json.Marshal(achievements)
	if err != nil {
		return nil, nil, nil, err
	}
	return heroesJSON, equipmentJSON, achievementsJSON, nil
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
