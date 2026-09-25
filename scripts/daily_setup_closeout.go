package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"clashking_tracking/internal/armyfamily"
	clashy "github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
)

const legendISetupTierID = 105000036

type setupAttack struct {
	player, code string
	rank         *int
	siegeID      int
	stars        int
	destruction  int
}

type setupCounts struct {
	attacks, zero, one, two, three, destruction int64
	sieges                                      map[int]int64
}

func (c *setupCounts) add(a setupAttack) error {
	if a.stars < 0 || a.stars > 3 || a.destruction < 0 || a.destruction > 100 {
		return fmt.Errorf("invalid Legend attack stars=%d destruction=%d", a.stars, a.destruction)
	}
	c.attacks++
	switch a.stars {
	case 0:
		c.zero++
	case 1:
		c.one++
	case 2:
		c.two++
	case 3:
		c.three++
	}
	c.destruction += int64(a.destruction)
	if a.siegeID > 0 {
		if c.sieges == nil {
			c.sieges = map[int]int64{}
		}
		c.sieges[a.siegeID]++
	}
	return nil
}

type setupCohort struct {
	name      string
	rankLimit *int
	counts    setupCounts
	groups    map[string]*setupCounts
	variants  map[string]map[string]*setupCounts
}

func setupCohorts() []*setupCohort {
	top1000, top200 := 1000, 200
	return []*setupCohort{
		{name: "legend_i", groups: map[string]*setupCounts{}, variants: map[string]map[string]*setupCounts{}},
		{name: "top_1000", rankLimit: &top1000, groups: map[string]*setupCounts{}, variants: map[string]map[string]*setupCounts{}},
		{name: "top_200", rankLimit: &top200, groups: map[string]*setupCounts{}, variants: map[string]map[string]*setupCounts{}},
	}
}

// RebuildDailyArmySetups writes only the new setup observations for completed
// Legend days. The caller must restrict the DSN to the disposable local DB.
func RebuildDailyArmySetups(ctx context.Context, dsn string, days []time.Time) (int, error) {
	store, err := newTimescaleScheduledStore(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer store.pool.Close()
	writes := 0
	for _, day := range days {
		count, err := store.rebuildDailyArmySetups(ctx, day)
		if err != nil {
			return writes, fmt.Errorf("rebuild army setups %s: %w", day.Format("2006-01-02"), err)
		}
		writes += count
	}
	return writes, nil
}

func (s *timescaleScheduledStore) rebuildDailyArmySetups(ctx context.Context, day time.Time) (int, error) {
	static, err := clashy.LoadStaticData()
	if err != nil {
		return 0, err
	}
	day = dayStart(day)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, armyFamilyCloseoutLockID); err != nil {
		return 0, err
	}
	var snapshot bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM legend_rankings_history WHERE day=$1)`, day).Scan(&snapshot); err != nil {
		return 0, err
	}
	if !snapshot {
		return 0, fmt.Errorf("Legend ranking snapshot is unavailable for %s", day.Format("2006-01-02"))
	}
	attacks, err := readSetupAttacks(ctx, tx, day, static)
	if err != nil {
		return 0, err
	}
	cohorts, result, err := calculateDailySetups(attacks, static)
	if err != nil {
		return 0, err
	}
	if err = verifySetupPopulation(ctx, tx, day, cohorts); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM army_setup_daily_stats WHERE day=$1 AND league_tier_id=$2`, day, legendISetupTierID); err != nil {
		return 0, err
	}
	writes := 0
	for _, cohort := range cohorts {
		for _, family := range result.Families {
			counts := cohort.groups[family.Signature]
			if counts == nil || counts.attacks == 0 {
				continue
			}
			evidence, _ := json.Marshal(map[string]any{"recipeCount": family.RecipeCount})
			if err = insertSetupObservation(ctx, tx, day, cohort.rankLimit, family.Signature, "", family.CoreTroopIDs, []armyfamily.SetupCondition{}, family.RepresentativeCode, *counts, evidence); err != nil {
				return 0, err
			}
			writes++
			for _, setup := range family.Setups {
				variantCounts := cohort.variants[family.Signature][setup.Signature]
				if variantCounts == nil || variantCounts.attacks == 0 {
					continue
				}
				// Evidence is calculated for the complete day's discovery population;
				// observed counts on this row are specific to its rank cohort.
				setupEvidence, _ := json.Marshal(setup.Evidence)
				if err = insertSetupObservation(ctx, tx, day, cohort.rankLimit, family.Signature, setup.Signature, family.CoreTroopIDs, setup.Conditions, setup.RepresentativeCode, *variantCounts, setupEvidence); err != nil {
					return 0, err
				}
				writes++
			}
		}
		classified := int64(0)
		for _, counts := range cohort.groups {
			classified += counts.attacks
		}
		if classified > cohort.counts.attacks {
			return 0, errors.New("classified army attacks exceed source attacks")
		}
		_, err = tx.Exec(ctx, `UPDATE legend_daily_stats SET classified_army_attacks=$3
			WHERE day=$1 AND cohort=$2`, day, cohort.name, classified)
		if err != nil {
			return 0, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return writes, nil
}

func readSetupAttacks(ctx context.Context, tx pgx.Tx, day time.Time, static *clashy.StaticData) ([]setupAttack, error) {
	start, end := legendDayWindow(day)
	rows, err := tx.Query(ctx, `SELECT battle.player_tag,coalesce(battle.share_code,''),history.global_rank,
		battle.stars,battle.destruction_percentage
		FROM battles_ranked battle
		JOIN legend_rankings_history history ON history.day=$3 AND history.tag=battle.player_tag
		WHERE battle.direction=1 AND battle.battle_mode=2 AND battle.battle_time >= $1 AND battle.battle_time < $2
		ORDER BY battle.player_tag,battle.battle_time`, start, end, day)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attacks := []setupAttack{}
	for rows.Next() {
		var attack setupAttack
		if err := rows.Scan(&attack.player, &attack.code, &attack.rank, &attack.stars, &attack.destruction); err != nil {
			return nil, err
		}
		attack.code = normalizeSetupArmyCode(static, attack.code)
		attack.siegeID = normalizedSetupSiegeID(static, attack.code)
		attacks = append(attacks, attack)
	}
	return attacks, rows.Err()
}

// The share code has already been normalized to put the selected siege in the
// Clan Castle section. This records the siege actually used by each attack,
// including codes where it was originally exported in the main army section.
func normalizedSetupSiegeID(static *clashy.StaticData, code string) int {
	for key := range parseArmyColumns(code) {
		section, localID := splitArmyColumn(key)
		if section != "i" {
			continue
		}
		id := clashy.TroopBaseID + localID
		if data := static.LookupByID(id); data != nil && data["production_building"] == "Workshop" {
			return id
		}
	}
	return 0
}

func normalizeSetupArmyCode(static *clashy.StaticData, source string) string {
	code, err := normalizeArmyShareCodeChecked(source)
	if err != nil || code == "" {
		return ""
	}
	sections := splitArmyShareSections(code)
	items := map[byte]map[int]uint16{'i': {}, 'd': {}, 'u': {}, 's': {}}
	heroes := ""
	for _, section := range sections {
		if section[0] == 'h' {
			heroes = section
		} else {
			parseArmyItemCountsSection(section[1:], items[section[0]])
		}
	}
	selectedSiege := -1
	donatedSiege := false
	for _, section := range []byte{'i', 'u'} {
		for localID := range items[section] {
			if data := static.LookupByID(clashy.TroopBaseID + localID); data != nil && data["production_building"] == "Workshop" {
				if section == 'i' {
					if selectedSiege < 0 || localID < selectedSiege {
						selectedSiege = localID
					}
					donatedSiege = true
				} else if !donatedSiege && (selectedSiege < 0 || localID < selectedSiege) {
					selectedSiege = localID
				}
				delete(items[section], localID)
			}
		}
	}
	if selectedSiege >= 0 {
		items['i'][selectedSiege] = 1
	}
	parts := []string{}
	if heroes != "" {
		parts = append(parts, heroes)
	}
	for _, section := range []byte{'i', 'd', 'u', 's'} {
		if value := encodeArmyItemSection(section, items[section]); value != "" {
			parts = append(parts, value)
		}
	}
	return strings.Join(parts, "")
}

func recipeForSetup(static *clashy.StaticData, code string, attacks int64) armyfamily.DailyRecipe {
	recipe := armyfamily.DailyRecipe{Code: code, Attacks: attacks, TroopHousing: map[int]int{}, Spells: map[int]int{}}
	if code == "" {
		return recipe
	}
	record := armyCompositionFromColumns(static, code, parseArmyColumns(code))
	for _, troop := range record.MainTroops {
		recipe.TroopHousing[troop.ID] += troop.Quantity * staticWeight(static.LookupByID(troop.ID))
	}
	for _, spell := range record.Spells {
		recipe.Spells[spell.ID] += spell.Quantity
	}
	for _, equipment := range record.Equipment {
		recipe.Equipment = append(recipe.Equipment, armyfamily.HeroEquipment{HeroID: equipment.HeroID, EquipmentID: equipment.EquipmentID})
	}
	return recipe
}

func calculateDailySetups(attacks []setupAttack, static *clashy.StaticData) ([]*setupCohort, armyfamily.DailyResult, error) {
	usage := map[string]int64{}
	for _, attack := range attacks {
		usage[attack.code]++
	}
	codes := make([]string, 0, len(usage))
	for code := range usage {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	recipes := make(map[string]armyfamily.DailyRecipe, len(codes))
	input := make([]armyfamily.DailyRecipe, 0, len(codes))
	for _, code := range codes {
		recipe := recipeForSetup(static, code, usage[code])
		recipes[code] = recipe
		input = append(input, recipe)
	}
	supportNames := map[string]bool{"Healer": true, "Druid": true, "Apprentice Warden": true, "Headhunter": true,
		"Ice Golem": true, "Wall Breaker": true, "Super Wall Breaker": true, "Sneaky Goblin": true,
		"Goblin": true, "Archer": true, "Barbarian": true, "Minion": true}
	support := map[int]bool{}
	identitySupport := map[int]bool{}
	conditionalCore := map[int]bool{}
	for _, recipe := range input {
		for id := range recipe.TroopHousing {
			if data := static.LookupByID(id); data != nil {
				name, _ := data["name"].(string)
				support[id] = supportNames[name]
				if name == "Healer" {
					identitySupport[id] = true
				}
				if name == "Furnace" {
					conditionalCore[id] = true
				}
			}
		}
	}
	result := armyfamily.ClassifyDaily(input, armyfamily.DailyOptions{SupportTroops: support,
		IdentitySupportTroops: identitySupport, ConditionalCoreTroops: conditionalCore})
	cohorts := setupCohorts()
	families := map[string]armyfamily.DailyFamily{}
	for _, family := range result.Families {
		families[family.Signature] = family
	}
	for _, attack := range attacks {
		for _, cohort := range cohorts {
			if cohort.rankLimit != nil && (attack.rank == nil || *attack.rank > *cohort.rankLimit) {
				continue
			}
			if err := cohort.counts.add(attack); err != nil {
				return nil, armyfamily.DailyResult{}, err
			}
			groupKey := result.CodeFamily[attack.code]
			if groupKey == "" {
				continue
			}
			group := cohort.groups[groupKey]
			if group == nil {
				group = &setupCounts{}
				cohort.groups[groupKey] = group
			}
			if err := group.add(attack); err != nil {
				return nil, armyfamily.DailyResult{}, err
			}
			for _, setup := range families[groupKey].Setups {
				if !armyfamily.MatchSetup(recipes[attack.code], setup.Conditions) {
					continue
				}
				variants := cohort.variants[groupKey]
				if variants == nil {
					variants = map[string]*setupCounts{}
					cohort.variants[groupKey] = variants
				}
				variant := variants[setup.Signature]
				if variant == nil {
					variant = &setupCounts{}
					variants[setup.Signature] = variant
				}
				if err := variant.add(attack); err != nil {
					return nil, armyfamily.DailyResult{}, err
				}
			}
		}
	}
	return cohorts, result, nil
}

func verifySetupPopulation(ctx context.Context, tx pgx.Tx, day time.Time, cohorts []*setupCohort) error {
	for _, cohort := range cohorts {
		var stored int64
		if err := tx.QueryRow(ctx, `SELECT attack_count FROM legend_daily_stats WHERE day=$1 AND cohort=$2`, day, cohort.name).Scan(&stored); err != nil {
			return fmt.Errorf("missing finalized %s total for %s: %w", cohort.name, day.Format("2006-01-02"), err)
		}
		if stored != cohort.counts.attacks {
			return fmt.Errorf("%s population mismatch for %s: stored %d attacks, source %d attacks", cohort.name, day.Format("2006-01-02"), stored, cohort.counts.attacks)
		}
	}
	return nil
}

func insertSetupObservation(ctx context.Context, tx pgx.Tx, day time.Time, rankLimit *int, groupKey, variantKey string, core []int, conditions []armyfamily.SetupCondition, representative string, counts setupCounts, evidence []byte) error {
	coreIDs := make([]int32, len(core))
	for i, id := range core {
		coreIDs[i] = int32(id)
	}
	conditionsJSON, err := json.Marshal(conditions)
	if err != nil {
		return err
	}
	siegeUsage := make([]map[string]any, 0, len(counts.sieges))
	for id, attacks := range counts.sieges {
		siegeUsage = append(siegeUsage, map[string]any{"id": id, "attacks": attacks})
	}
	sort.Slice(siegeUsage, func(i, j int) bool { return siegeUsage[i]["id"].(int) < siegeUsage[j]["id"].(int) })
	siegeJSON, err := json.Marshal(siegeUsage)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO army_setup_daily_stats(
		day,league_tier_id,rank_limit,group_key,variant_key,core_troops,conditions,representative_share_code,
		attack_count,zero_star_count,one_star_count,two_star_count,three_star_count,destruction_percentage_sum,evidence,siege_usage
	) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16::jsonb)`, day, legendISetupTierID, rankLimit, groupKey, variantKey, coreIDs, string(conditionsJSON), representative,
		counts.attacks, counts.zero, counts.one, counts.two, counts.three, counts.destruction, string(evidence), string(siegeJSON))
	return err
}
