package scripts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"clashking_tracking/internal/armyfamily"
	"github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
)

const armyFamilyCloseoutLockID int64 = 636413279150006789

var legendCloseoutCohorts = [...]string{"legend_i", "top_1000", "top_200"}
var experimentalLegendCloseoutCohorts = [...]string{"legend_i", "top_1000", "top_200", "top_100"}

// RebuildLegendCloseouts reruns only the requested finalized Legend days. The
// caller owns environment safety checks; each day still requires an imported
// ranking snapshot and is rebuilt transactionally.
func RebuildLegendCloseouts(ctx context.Context, dsn string, days []time.Time) (int, error) {
	store, err := newTimescaleScheduledStore(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer store.pool.Close()
	writes := 0
	for _, day := range days {
		count, rebuildErr := store.finalizeLegendCloseout(ctx, day, true)
		if rebuildErr != nil {
			return writes, fmt.Errorf("rebuild Legend closeout %s: %w", day.Format("2006-01-02"), rebuildErr)
		}
		writes += count
	}
	return writes, nil
}

type decodedArmy struct {
	code        string
	record      armyCompositionRecord
	composition armyfamily.Composition
	usage       int64
}
type legendAttack struct {
	cohort, player, code string
	stars, destruction   int
	duration             int16
}
type dailyCounts struct {
	attacks, zero, one, two, three, destruction, duration int64
	players                                               map[string]struct{}
}
type usageCount struct{ Uses, Triples int64 }
type itemStat struct {
	ID      int   `json:"id"`
	Uses    int64 `json:"uses"`
	Triples int64 `json:"triples"`
}
type assignmentKey struct{ pet, hero int }
type assignmentStat struct {
	PetID   int   `json:"petId"`
	HeroID  int   `json:"heroId"`
	Uses    int64 `json:"uses"`
	Triples int64 `json:"triples"`
}
type equipmentPairKey struct{ hero, first, second int }
type equipmentPairStat struct {
	HeroID       int   `json:"heroId"`
	EquipmentIDs []int `json:"equipmentIds"`
	Uses         int64 `json:"uses"`
	Triples      int64 `json:"triples"`
}
type petComboCount struct {
	PetIDs []int
	usageCount
}
type petComboStat struct {
	PetIDs  []int `json:"petIds"`
	Uses    int64 `json:"uses"`
	Triples int64 `json:"triples"`
}
type legendMetadata struct {
	heroes, pets, equipment, troops, spells, sieges map[int]usageCount
	petAssignments                                  map[assignmentKey]usageCount
	equipmentPairs                                  map[equipmentPairKey]usageCount
	petCombos                                       map[string]petComboCount
}

func (s *timescaleScheduledStore) CaptureLegendSnapshot(ctx context.Context, day time.Time) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, armyFamilyCloseoutLockID); err != nil {
		return 0, err
	}
	day = dayStart(day)
	var existing int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM legend_rankings_history WHERE day=$1`, day).Scan(&existing); err != nil {
		return 0, err
	}
	if existing > 0 {
		if err = tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}
	var current int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM legend_rankings_current`).Scan(&current); err != nil {
		return 0, err
	}
	if current == 0 {
		return 0, errors.New("cannot capture Legend ranking snapshot from an empty current leaderboard")
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO legend_rankings_history(day,tag,global_rank,trophies)
		SELECT $1,current.tag,current.global_rank,current.trophies
		FROM legend_rankings_current current
	`, day)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *timescaleScheduledStore) FinalizeLegendCloseout(ctx context.Context, day time.Time) (int, error) {
	return s.finalizeLegendCloseout(ctx, day, false)
}

func (s *timescaleScheduledStore) finalizeLegendCloseout(ctx context.Context, day time.Time, experimentalMetadata bool) (int, error) {
	static, err := clashy.LoadStaticData()
	if err != nil {
		return 0, err
	}
	start, end := legendDayWindow(day)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, armyFamilyCloseoutLockID); err != nil {
		return 0, err
	}
	day = dayStart(day)
	var snapshotExists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM legend_rankings_history WHERE day=$1)`, day).Scan(&snapshotExists); err != nil {
		return 0, err
	}
	if !snapshotExists {
		return 0, fmt.Errorf("Legend ranking snapshot is unavailable for %s", day.Format("2006-01-02"))
	}
	attacks, usage, err := readLegendAttacks(ctx, tx, start, end, experimentalMetadata)
	if err != nil {
		return 0, err
	}
	decoded, err := decodeArmies(static, usage)
	if err != nil {
		return 0, err
	}
	if experimentalMetadata {
		if err = upsertDecodedArmyCompositions(ctx, tx, decoded); err != nil {
			return 0, err
		}
	}
	anchors, err := readAnchors(ctx, tx, static, decoded)
	if err != nil {
		return 0, err
	}
	members, err := readMembers(ctx, tx, mapKeys(usage))
	if err != nil {
		return 0, err
	}
	writes := 0
	for _, code := range unassignedCodes(usage, members) {
		a := decoded[code]
		match, ok := armyfamily.FindDirectAnchor(a.composition, anchors)
		if !ok {
			if err = tx.QueryRow(ctx, `INSERT INTO army_families(representative_share_code,name) VALUES($1,NULL) ON CONFLICT(representative_share_code) DO UPDATE SET representative_share_code=EXCLUDED.representative_share_code RETURNING family_id`, code).Scan(&match.FamilyID); err != nil {
				return 0, err
			}
			match.TroopHousingSimilarity, match.SpellCapacitySimilarity, match.EquipmentSimilarity = 1, 1, 1
			anchors = append(anchors, armyfamily.Anchor{FamilyID: match.FamilyID, ShareCode: code, Composition: a.composition})
			writes++
		}
		tag, e := tx.Exec(ctx, `INSERT INTO army_family_members(share_code,family_id) VALUES($1,$2) ON CONFLICT(share_code) DO NOTHING`, code, match.FamilyID)
		if e != nil {
			return 0, e
		}
		writes += int(tag.RowsAffected())
		members[code] = match.FamilyID
	}
	if _, err = tx.Exec(ctx, `DELETE FROM army_family_daily_stats WHERE day=$1`, day); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `DELETE FROM legend_daily_stats WHERE day=$1`, day); err != nil {
		return 0, err
	}
	byCohort := make(map[string][]legendAttack, len(legendCloseoutCohorts))
	for _, attack := range attacks {
		byCohort[attack.cohort] = append(byCohort[attack.cohort], attack)
	}
	cohorts := legendCloseoutCohorts[:]
	if experimentalMetadata {
		cohorts = experimentalLegendCloseoutCohorts[:]
	}
	for _, cohort := range cohorts {
		g, fams, metadata := aggregateLegend(byCohort[cohort], members, decoded)
		for _, id := range familyIDs(fams) {
			c := fams[id]
			_, err = tx.Exec(ctx, `INSERT INTO army_family_daily_stats(family_id,day,cohort,attack_count,distinct_player_count,zero_star_count,one_star_count,two_star_count,three_star_count,destruction_percentage_sum,duration_seconds_sum) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, id, day, cohort, c.attacks, len(c.players), c.zero, c.one, c.two, c.three, c.destruction, c.duration)
			if err != nil {
				return 0, err
			}
			writes++
		}
		hj, _ := json.Marshal(itemStats(metadata.heroes))
		pj, _ := json.Marshal(itemStats(metadata.pets))
		ej, _ := json.Marshal(itemStats(metadata.equipment))
		aj, _ := json.Marshal(assignmentStats(metadata.petAssignments))
		tj, _ := json.Marshal(itemStats(metadata.troops))
		spj, _ := json.Marshal(itemStats(metadata.spells))
		sj, _ := json.Marshal(itemStats(metadata.sieges))
		epj, _ := json.Marshal(equipmentPairStats(metadata.equipmentPairs))
		if experimentalMetadata {
			pcj, _ := json.Marshal(petComboStats(metadata.petCombos))
			_, err = tx.Exec(ctx, `INSERT INTO legend_daily_stats(day,cohort,attack_count,distinct_player_count,zero_star_count,one_star_count,two_star_count,three_star_count,destruction_percentage_sum,duration_seconds_sum,hero_stats,pet_stats,equipment_stats,pet_hero_assignments,troop_stats,spell_stats,siege_stats,equipment_pair_stats,pet_combo_stats) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`, day, cohort, g.attacks, len(g.players), g.zero, g.one, g.two, g.three, g.destruction, g.duration, json.RawMessage(hj), json.RawMessage(pj), json.RawMessage(ej), json.RawMessage(aj), json.RawMessage(tj), json.RawMessage(spj), json.RawMessage(sj), json.RawMessage(epj), json.RawMessage(pcj))
		} else {
			_, err = tx.Exec(ctx, `INSERT INTO legend_daily_stats(day,cohort,attack_count,distinct_player_count,zero_star_count,one_star_count,two_star_count,three_star_count,destruction_percentage_sum,duration_seconds_sum,hero_stats,pet_stats,equipment_stats,pet_hero_assignments) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, day, cohort, g.attacks, len(g.players), g.zero, g.one, g.two, g.three, g.destruction, g.duration, json.RawMessage(hj), json.RawMessage(pj), json.RawMessage(ej), json.RawMessage(aj))
		}
		if err != nil {
			return 0, err
		}
		writes++
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return writes, nil
}

func upsertDecodedArmyCompositions(ctx context.Context, tx pgx.Tx, decoded map[string]decodedArmy) error {
	codes := make([]string, 0, len(decoded))
	for code := range decoded {
		codes = append(codes, code)
	}
	sort.Strings(codes)
	for _, code := range codes {
		record := decoded[code].record
		mainTroops, err := json.Marshal(record.MainTroops)
		if err != nil {
			return err
		}
		clanCastleTroops, err := json.Marshal(record.ClanCastleTroops)
		if err != nil {
			return err
		}
		spells, err := json.Marshal(record.Spells)
		if err != nil {
			return err
		}
		equipment, err := json.Marshal(record.Equipment)
		if err != nil {
			return err
		}
		petAssignments, err := json.Marshal(record.PetAssignments)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO army_compositions
			(share_code,main_troops,clan_castle_troops,spells,heroes,equipment,pet_assignments,siege_machine_id)
			VALUES($1,$2::jsonb,$3::jsonb,$4::jsonb,$5,$6::jsonb,$7::jsonb,$8)
			ON CONFLICT(share_code) DO NOTHING`, code, mainTroops, clanCastleTroops, spells, record.Heroes, equipment, petAssignments, record.SiegeMachineID); err != nil {
			return err
		}
	}
	return nil
}
func legendDayWindow(day time.Time) (time.Time, time.Time) {
	s := dayStart(day).Add(5*time.Hour + 10*time.Minute)
	return s, s.Add(24 * time.Hour)
}
func readLegendAttacks(ctx context.Context, tx pgx.Tx, start, end time.Time, includeTop100 bool) ([]legendAttack, map[string]int64, error) {
	top100 := ""
	if includeTop100 {
		top100 = `
			UNION ALL
			SELECT tag,'top_100'::text FROM legend_rankings_history WHERE day=$3 AND global_rank <= 100`
	}
	r, e := tx.Query(ctx, `
		WITH cohort_members AS (
			SELECT tag AS player_tag,'legend_i'::text AS cohort FROM legend_rankings_history WHERE day=$3
			UNION ALL
			SELECT tag,'top_1000'::text FROM legend_rankings_history WHERE day=$3 AND global_rank <= 1000
			UNION ALL
			SELECT tag,'top_200'::text FROM legend_rankings_history WHERE day=$3 AND global_rank <= 200
			`+top100+`
		)
		SELECT cohort.cohort,battle.player_tag,battle.stars,battle.destruction_percentage,
		       battle.duration_seconds,coalesce(battle.share_code,'')
		FROM battles_ranked battle
		JOIN cohort_members cohort ON cohort.player_tag=battle.player_tag
		WHERE battle.battle_mode=2 AND battle.direction=1
		  AND battle.battle_time >= $1 AND battle.battle_time < $2
		ORDER BY cohort.cohort,battle.player_tag,battle.battle_time
	`, start, end, dayStart(start))
	if e != nil {
		return nil, nil, e
	}
	defer r.Close()
	var out []legendAttack
	u := map[string]int64{}
	for r.Next() {
		var a legendAttack
		if e = r.Scan(&a.cohort, &a.player, &a.stars, &a.destruction, &a.duration, &a.code); e != nil {
			return nil, nil, e
		}
		out = append(out, a)
		if a.code != "" {
			u[a.code]++
		}
	}
	return out, u, r.Err()
}
func decodeArmies(s *clashy.StaticData, u map[string]int64) (map[string]decodedArmy, error) {
	o := map[string]decodedArmy{}
	for c, n := range u {
		x, e := normalizeArmyShareCodeChecked(c)
		if e != nil || x != c {
			return nil, fmt.Errorf("stored Legend army %q is not canonical", c)
		}
		r := armyCompositionFromColumns(s, c, parseArmyColumns(c))
		o[c] = decodedArmy{c, r, familyComposition(s, r), n}
	}
	return o, nil
}
func readAnchors(ctx context.Context, tx pgx.Tx, s *clashy.StaticData, d map[string]decodedArmy) ([]armyfamily.Anchor, error) {
	r, e := tx.Query(ctx, `SELECT family_id,representative_share_code FROM army_families ORDER BY family_id`)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	var o []armyfamily.Anchor
	for r.Next() {
		var id int64
		var c string
		if e = r.Scan(&id, &c); e != nil {
			return nil, e
		}
		a, ok := d[c]
		if !ok {
			q, x := normalizeArmyShareCodeChecked(c)
			if x != nil || q != c {
				return nil, fmt.Errorf("invalid representative for family %d", id)
			}
			z := armyCompositionFromColumns(s, c, parseArmyColumns(c))
			a.composition = familyComposition(s, z)
		}
		o = append(o, armyfamily.Anchor{FamilyID: id, ShareCode: c, Composition: a.composition})
	}
	return o, r.Err()
}
func readMembers(ctx context.Context, tx pgx.Tx, c []string) (map[string]int64, error) {
	o := map[string]int64{}
	if len(c) == 0 {
		return o, nil
	}
	r, e := tx.Query(ctx, `SELECT share_code,family_id FROM army_family_members WHERE share_code=ANY($1::text[])`, c)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	for r.Next() {
		var s string
		var id int64
		if e = r.Scan(&s, &id); e != nil {
			return nil, e
		}
		o[s] = id
	}
	return o, r.Err()
}
func familyComposition(s *clashy.StaticData, r armyCompositionRecord) armyfamily.Composition {
	o := armyfamily.Composition{TroopHousing: map[int]int{}, SpellCapacity: map[int]int{}}
	for _, x := range r.MainTroops {
		o.TroopHousing[x.ID] += x.Quantity * staticWeight(s.LookupByID(x.ID))
	}
	for _, x := range r.Spells {
		o.SpellCapacity[x.ID] += x.Quantity * staticWeight(s.LookupByID(x.ID))
	}
	for _, x := range r.Heroes {
		o.Heroes = append(o.Heroes, int(x))
	}
	for _, x := range r.Equipment {
		o.Equipment = append(o.Equipment, x.EquipmentID)
	}
	return o
}
func staticWeight(v map[string]any) int {
	if x, ok := v["housing_space"].(float64); ok && x > 0 {
		return int(x)
	}
	return 1
}
func mapKeys(m map[string]int64) []string {
	o := make([]string, 0, len(m))
	for k := range m {
		o = append(o, k)
	}
	sort.Strings(o)
	return o
}
func unassignedCodes(u map[string]int64, m map[string]int64) []string {
	o := []string{}
	for c := range u {
		if _, ok := m[c]; !ok {
			o = append(o, c)
		}
	}
	sort.Slice(o, func(i, j int) bool {
		if u[o[i]] != u[o[j]] {
			return u[o[i]] > u[o[j]]
		}
		return o[i] < o[j]
	})
	return o
}
func newCounts() *dailyCounts { return &dailyCounts{players: map[string]struct{}{}} }
func addResult(c *dailyCounts, a legendAttack) {
	c.attacks++
	c.players[a.player] = struct{}{}
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
	c.duration += int64(a.duration)
}
func addUsage(m map[int]usageCount, id int, triple bool, seen map[int]bool) {
	if seen[id] {
		return
	}
	seen[id] = true
	v := m[id]
	v.Uses++
	if triple {
		v.Triples++
	}
	m[id] = v
}
func aggregateLegend(a []legendAttack, m map[string]int64, d map[string]decodedArmy) (*dailyCounts, map[int64]*dailyCounts, legendMetadata) {
	g := newCounts()
	fs := map[int64]*dailyCounts{}
	metadata := legendMetadata{
		heroes: map[int]usageCount{}, pets: map[int]usageCount{}, equipment: map[int]usageCount{},
		troops: map[int]usageCount{}, spells: map[int]usageCount{}, sieges: map[int]usageCount{},
		petAssignments: map[assignmentKey]usageCount{}, equipmentPairs: map[equipmentPairKey]usageCount{},
		petCombos: map[string]petComboCount{},
	}
	for _, x := range a {
		addResult(g, x)
		if id, ok := m[x.code]; ok {
			if fs[id] == nil {
				fs[id] = newCounts()
			}
			addResult(fs[id], x)
		}
		z, ok := d[x.code]
		if !ok {
			continue
		}
		t := x.stars == 3
		seen := map[int]bool{}
		for _, id := range z.record.Heroes {
			addUsage(metadata.heroes, int(id), t, seen)
		}
		seen = map[int]bool{}
		for _, v := range z.record.Equipment {
			addUsage(metadata.equipment, v.EquipmentID, t, seen)
		}
		seen = map[int]bool{}
		for _, v := range z.record.MainTroops {
			addUsage(metadata.troops, v.ID, t, seen)
		}
		seen = map[int]bool{}
		for _, v := range z.record.Spells {
			addUsage(metadata.spells, v.ID, t, seen)
		}
		if z.record.SiegeMachineID != nil {
			addUsage(metadata.sieges, int(*z.record.SiegeMachineID), t, map[int]bool{})
		}
		seen = map[int]bool{}
		for _, v := range z.record.PetAssignments {
			addUsage(metadata.pets, v.PetID, t, seen)
			k := assignmentKey{v.PetID, v.HeroID}
			q := metadata.petAssignments[k]
			q.Uses++
			if t {
				q.Triples++
			}
			metadata.petAssignments[k] = q
		}
		petIDs := make([]int, 0, len(seen))
		for petID := range seen {
			petIDs = append(petIDs, petID)
		}
		sort.Ints(petIDs)
		if len(petIDs) > 0 {
			key := intsKey(petIDs)
			combo := metadata.petCombos[key]
			combo.PetIDs = append([]int(nil), petIDs...)
			combo.Uses++
			if t {
				combo.Triples++
			}
			metadata.petCombos[key] = combo
		}
		byHero := map[int][]int{}
		for _, v := range z.record.Equipment {
			byHero[v.HeroID] = append(byHero[v.HeroID], v.EquipmentID)
		}
		for hero, ids := range byHero {
			sort.Ints(ids)
			if len(ids) != 2 || ids[0] == ids[1] {
				continue
			}
			k := equipmentPairKey{hero, ids[0], ids[1]}
			q := metadata.equipmentPairs[k]
			q.Uses++
			if t {
				q.Triples++
			}
			metadata.equipmentPairs[k] = q
		}
	}
	return g, fs, metadata
}
func familyIDs(m map[int64]*dailyCounts) []int64 {
	o := []int64{}
	for id := range m {
		o = append(o, id)
	}
	sort.Slice(o, func(i, j int) bool { return o[i] < o[j] })
	return o
}
func itemStats(m map[int]usageCount) []itemStat {
	ids := []int{}
	for id := range m {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	o := []itemStat{}
	for _, id := range ids {
		o = append(o, itemStat{id, m[id].Uses, m[id].Triples})
	}
	return o
}
func assignmentStats(m map[assignmentKey]usageCount) []assignmentStat {
	ks := []assignmentKey{}
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		if ks[i].pet != ks[j].pet {
			return ks[i].pet < ks[j].pet
		}
		return ks[i].hero < ks[j].hero
	})
	o := []assignmentStat{}
	for _, k := range ks {
		o = append(o, assignmentStat{k.pet, k.hero, m[k].Uses, m[k].Triples})
	}
	return o
}
func equipmentPairStats(m map[equipmentPairKey]usageCount) []equipmentPairStat {
	ks := make([]equipmentPairKey, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool {
		if ks[i].hero != ks[j].hero {
			return ks[i].hero < ks[j].hero
		}
		if ks[i].first != ks[j].first {
			return ks[i].first < ks[j].first
		}
		return ks[i].second < ks[j].second
	})
	o := make([]equipmentPairStat, 0, len(ks))
	for _, k := range ks {
		o = append(o, equipmentPairStat{k.hero, []int{k.first, k.second}, m[k].Uses, m[k].Triples})
	}
	return o
}
func petComboStats(m map[string]petComboCount) []petComboStat {
	o := make([]petComboStat, 0, len(m))
	for _, combo := range m {
		o = append(o, petComboStat{combo.PetIDs, combo.Uses, combo.Triples})
	}
	sort.Slice(o, func(i, j int) bool {
		for k := 0; k < len(o[i].PetIDs) && k < len(o[j].PetIDs); k++ {
			if o[i].PetIDs[k] != o[j].PetIDs[k] {
				return o[i].PetIDs[k] < o[j].PetIDs[k]
			}
		}
		return len(o[i].PetIDs) < len(o[j].PetIDs)
	})
	return o
}
