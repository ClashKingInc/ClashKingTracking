package scripts

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"time"

	"clashking_tracking/internal/armyfamily"
	"clashking_tracking/internal/platform"

	"github.com/clashkinginc/clashy.go"
)

const armyFamilyNamingPromptVersion = "army-family-name-v1"

type storedArmyComposition struct {
	Hash             armyfamily.Hash
	ShareCode        string
	MainTroops       []armyQuantity
	ClanCastleTroops []armyQuantity
	Spells           []armySpellQuantity
	Heroes           []int32
	Equipment        []armyHeroEquipment
	PetAssignments   []armyPetAssignment
	SiegeMachineID   *int32
	Usage            int64
}

func (s *timescaleScheduledStore) FinalizeArmyFamilies(ctx context.Context, day time.Time, cfg platform.Config) (int, error) {
	static, err := clashy.LoadStaticData()
	if err != nil {
		return 0, err
	}
	anchors, names, err := s.loadArmyFamilyAnchors(ctx, static)
	if err != nil {
		return 0, err
	}
	candidates, err := s.loadUnassignedArmyCompositions(ctx, day)
	if err != nil {
		return 0, err
	}
	namer := armyfamily.CloudflareNamer{
		APIOrigin: cfg.CloudflareAIAPIOrigin, AccountID: cfg.CloudflareAccountID,
		GatewayID: cfg.CloudflareAIGatewayID, APIToken: cfg.CloudflareAIAPIToken,
		Model: armyfamily.DefaultNamingModel,
	}
	writes := 0
	for _, candidate := range candidates {
		composition := familyComposition(static, candidate)
		if match, ok := armyfamily.FindDirectAnchor(composition, anchors); ok {
			tag, err := s.pool.Exec(ctx, `
				INSERT INTO army_family_members (army_hash,anchor_army_hash,troop_housing_similarity,spell_capacity_similarity,heroes_exact,equipment_similarity,equipment_difference_count,matching_version)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (army_hash) DO NOTHING
			`, candidate.Hash[:], match.AnchorHash[:], match.TroopHousingSimilarity, match.SpellCapacitySimilarity,
				match.HeroesExact, match.EquipmentSimilarity, match.EquipmentDifferenceCount, match.MatchingVersion)
			if err != nil {
				return writes, err
			}
			writes += int(tag.RowsAffected())
			continue
		}
		input := armyfamily.NamingInput{Hash: candidate.Hash, TroopNames: compositionNames(static, candidate.MainTroops), SpellNames: spellNames(static, candidate.Spells), ExistingNames: names}
		name, source := armyfamily.NameWithFallback(ctx, namer, input)
		created, err := s.createArmyFamily(ctx, candidate, name, source)
		if err != nil {
			return writes, err
		}
		if created {
			writes += 2
			names = append(names, name)
			anchors = append(anchors, armyfamily.Anchor{Hash: candidate.Hash, Composition: composition, CreatedAt: time.Now().UTC()})
		}
	}
	statsWrites, err := s.replaceArmyFamilyDailyStats(ctx, day)
	return writes + statsWrites, err
}

func (s *timescaleScheduledStore) loadArmyFamilyAnchors(ctx context.Context, static *clashy.StaticData) ([]armyfamily.Anchor, []string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT family.anchor_army_hash,family.family_name,family.created_at,composition.normalized_share_code,
			composition.main_troops,composition.clan_castle_troops,composition.spells,composition.heroes,
			composition.equipment,composition.pet_assignments,composition.siege_machine_id
		FROM army_families family JOIN army_compositions composition ON composition.army_hash=family.anchor_army_hash
		ORDER BY family.created_at,family.anchor_army_hash
	`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	anchors, names := []armyfamily.Anchor{}, []string{}
	for rows.Next() {
		var raw []byte
		var name string
		var created time.Time
		var composition storedArmyComposition
		if err := rows.Scan(&raw, &name, &created, &composition.ShareCode, &composition.MainTroops, &composition.ClanCastleTroops, &composition.Spells, &composition.Heroes, &composition.Equipment, &composition.PetAssignments, &composition.SiegeMachineID); err != nil {
			return nil, nil, err
		}
		if len(raw) != sha256.Size {
			return nil, nil, fmt.Errorf("army family anchor hash has %d bytes", len(raw))
		}
		copy(composition.Hash[:], raw)
		anchors = append(anchors, armyfamily.Anchor{Hash: composition.Hash, Composition: familyComposition(static, composition), CreatedAt: created})
		names = append(names, name)
	}
	return anchors, names, rows.Err()
}

func (s *timescaleScheduledStore) loadUnassignedArmyCompositions(ctx context.Context, day time.Time) ([]storedArmyComposition, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT composition.army_hash,composition.normalized_share_code,composition.main_troops,composition.clan_castle_troops,
			composition.spells,composition.heroes,composition.equipment,composition.pet_assignments,composition.siege_machine_id,count(*) AS usage
		FROM battles_ranked battle JOIN army_compositions composition USING (army_hash)
		WHERE battle.direction='attack' AND battle.battle_mode='legend' AND battle.battle_time >= $1 AND battle.battle_time < $1 + interval '1 day'
		  AND NOT EXISTS (SELECT 1 FROM army_family_members member WHERE member.army_hash=composition.army_hash)
		GROUP BY composition.army_hash,composition.normalized_share_code,composition.main_troops,composition.clan_castle_troops,
			composition.spells,composition.heroes,composition.equipment,composition.pet_assignments,composition.siege_machine_id
		ORDER BY usage DESC,composition.army_hash
	`, dayStart(day))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []storedArmyComposition{}
	for rows.Next() {
		var raw []byte
		var c storedArmyComposition
		if err := rows.Scan(&raw, &c.ShareCode, &c.MainTroops, &c.ClanCastleTroops, &c.Spells, &c.Heroes, &c.Equipment, &c.PetAssignments, &c.SiegeMachineID, &c.Usage); err != nil {
			return nil, err
		}
		if len(raw) != sha256.Size {
			return nil, fmt.Errorf("army composition hash has %d bytes", len(raw))
		}
		copy(c.Hash[:], raw)
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *timescaleScheduledStore) createArmyFamily(ctx context.Context, c storedArmyComposition, name, source string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	var model, prompt any
	if source == "ai" {
		model = armyfamily.DefaultNamingModel
		prompt = armyFamilyNamingPromptVersion
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO army_families(anchor_army_hash,representative_share_code,family_name,source,naming_model,naming_prompt_version)
		VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (anchor_army_hash) DO NOTHING
	`, c.Hash[:], c.ShareCode, name, source, model, prompt)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}
	if _, err = tx.Exec(ctx, `
		INSERT INTO army_family_members(army_hash,anchor_army_hash,troop_housing_similarity,spell_capacity_similarity,heroes_exact,equipment_similarity,equipment_difference_count,matching_version)
		VALUES($1,$1,1,1,true,1,0,$2)
	`, c.Hash[:], armyfamily.MatchingVersion); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func (s *timescaleScheduledStore) replaceArmyFamilyDailyStats(ctx context.Context, day time.Time) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	day = dayStart(day)
	if _, err = tx.Exec(ctx, `DELETE FROM army_family_daily_stats WHERE day=$1::date`, day); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO army_family_daily_stats(anchor_army_hash,day,attack_count,distinct_player_count,zero_star_count,one_star_count,two_star_count,three_star_count,destruction_percentage_sum,duration_seconds_sum,refreshed_at)
		SELECT member.anchor_army_hash,$1::date,count(*),count(DISTINCT battle.player_tag),count(*) FILTER(WHERE stars=0),count(*) FILTER(WHERE stars=1),
			count(*) FILTER(WHERE stars=2),count(*) FILTER(WHERE stars=3),sum(destruction_percentage),sum(coalesce(duration_seconds,0)),now()
		FROM battles_ranked battle JOIN army_family_members member USING(army_hash)
		WHERE battle.direction='attack' AND battle.battle_mode='legend' AND battle.battle_time >= $1 AND battle.battle_time < $1 + interval '1 day'
		GROUP BY member.anchor_army_hash
	`, day)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func familyComposition(static *clashy.StaticData, c storedArmyComposition) armyfamily.Composition {
	out := armyfamily.Composition{TroopHousing: map[int]int{}, SpellCapacity: map[int]int{}, Heroes: []int{}, Equipment: []int{}}
	for _, item := range c.MainTroops {
		out.TroopHousing[item.ID] += item.Quantity * staticWeight(static.LookupByID(item.ID))
	}
	for _, item := range c.Spells {
		out.SpellCapacity[item.ID] += item.Quantity * staticWeight(static.LookupByID(item.ID))
	}
	for _, id := range c.Heroes {
		out.Heroes = append(out.Heroes, int(id))
	}
	for _, item := range c.Equipment {
		out.Equipment = append(out.Equipment, item.EquipmentID)
	}
	return out
}

func staticWeight(value map[string]any) int {
	if raw, ok := value["housing_space"].(float64); ok && raw > 0 {
		return int(raw)
	}
	return 1
}
func compositionNames(static *clashy.StaticData, items []armyQuantity) []string {
	return itemNames(static, items)
}
func itemNames(static *clashy.StaticData, items []armyQuantity) []string {
	out := []string{}
	for _, item := range items {
		if name, ok := static.LookupByID(item.ID)["name"].(string); ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
func spellNames(static *clashy.StaticData, items []armySpellQuantity) []string {
	out := []string{}
	for _, item := range items {
		if name, ok := static.LookupByID(item.ID)["name"].(string); ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}
