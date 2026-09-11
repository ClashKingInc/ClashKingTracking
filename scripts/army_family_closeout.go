package scripts

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"clashking_tracking/internal/armyfamily"
	"github.com/clashkinginc/clashy.go"
	"github.com/jackc/pgx/v5"
)

const armyFamilyCloseoutLockID int64 = 636413279150006789

var legendCloseoutCohorts = [...]string{"legend_i", "top_1000", "top_200"}

type decodedArmy struct {
	code        string
	record      armyCompositionRecord
	composition armyfamily.Composition
	usage       int64
}
type legendAttack struct {
	cohort, player, code string
	stars, destruction   int
	duration             *int32
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

func (s *timescaleScheduledStore) FinalizeLegendCloseout(ctx context.Context, day time.Time) (int, error) {
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
	if _, err = tx.Exec(ctx, `DELETE FROM leaderboard_history_player_home WHERE day=$1`, day); err != nil {
		return 0, err
	}
	snapshotTag, err := tx.Exec(ctx, `
		INSERT INTO leaderboard_history_player_home(day,tag,global_rank,trophies)
		SELECT $1,tag,global_rank,trophies FROM legend_rankings_current
	`, day)
	if err != nil {
		return 0, err
	}
	attacks, usage, err := readLegendAttacks(ctx, tx, start, end)
	if err != nil {
		return 0, err
	}
	decoded, err := decodeArmies(static, usage)
	if err != nil {
		return 0, err
	}
	anchors, err := readAnchors(ctx, tx, static, decoded)
	if err != nil {
		return 0, err
	}
	members, err := readMembers(ctx, tx, mapKeys(usage))
	if err != nil {
		return 0, err
	}
	writes := int(snapshotTag.RowsAffected())
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
	for _, cohort := range legendCloseoutCohorts {
		g, fams, heroes, pets, equipment, assignments := aggregateLegend(byCohort[cohort], members, decoded)
		for _, id := range familyIDs(fams) {
			c := fams[id]
			_, err = tx.Exec(ctx, `INSERT INTO army_family_daily_stats(family_id,day,cohort,attack_count,distinct_player_count,zero_star_count,one_star_count,two_star_count,three_star_count,destruction_percentage_sum,duration_seconds_sum) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, id, day, cohort, c.attacks, len(c.players), c.zero, c.one, c.two, c.three, c.destruction, c.duration)
			if err != nil {
				return 0, err
			}
			writes++
		}
		hj, _ := json.Marshal(itemStats(heroes))
		pj, _ := json.Marshal(itemStats(pets))
		ej, _ := json.Marshal(itemStats(equipment))
		aj, _ := json.Marshal(assignmentStats(assignments))
		_, err = tx.Exec(ctx, `INSERT INTO legend_daily_stats(day,cohort,attack_count,distinct_player_count,zero_star_count,one_star_count,two_star_count,three_star_count,destruction_percentage_sum,duration_seconds_sum,hero_stats,pet_stats,equipment_stats,pet_hero_assignments) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`, day, cohort, g.attacks, len(g.players), g.zero, g.one, g.two, g.three, g.destruction, g.duration, json.RawMessage(hj), json.RawMessage(pj), json.RawMessage(ej), json.RawMessage(aj))
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
func legendDayWindow(day time.Time) (time.Time, time.Time) {
	s := dayStart(day).Add(5*time.Hour + 10*time.Minute)
	return s, s.Add(24 * time.Hour)
}
func readLegendAttacks(ctx context.Context, tx pgx.Tx, start, end time.Time) ([]legendAttack, map[string]int64, error) {
	r, e := tx.Query(ctx, `
		WITH cohort_members AS (
			SELECT tag AS player_tag,'legend_i'::text AS cohort FROM leaderboard_history_player_home WHERE day=$3
			UNION ALL
			SELECT tag,'top_1000'::text FROM leaderboard_history_player_home WHERE day=$3 AND global_rank <= 1000
			UNION ALL
			SELECT tag,'top_200'::text FROM leaderboard_history_player_home WHERE day=$3 AND global_rank <= 200
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
	if a.duration != nil {
		c.duration += int64(*a.duration)
	}
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
func aggregateLegend(a []legendAttack, m map[string]int64, d map[string]decodedArmy) (*dailyCounts, map[int64]*dailyCounts, map[int]usageCount, map[int]usageCount, map[int]usageCount, map[assignmentKey]usageCount) {
	g := newCounts()
	fs := map[int64]*dailyCounts{}
	hs := map[int]usageCount{}
	ps := map[int]usageCount{}
	es := map[int]usageCount{}
	as := map[assignmentKey]usageCount{}
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
			addUsage(hs, int(id), t, seen)
		}
		seen = map[int]bool{}
		for _, v := range z.record.Equipment {
			addUsage(es, v.EquipmentID, t, seen)
		}
		seen = map[int]bool{}
		for _, v := range z.record.PetAssignments {
			addUsage(ps, v.PetID, t, seen)
			k := assignmentKey{v.PetID, v.HeroID}
			q := as[k]
			q.Uses++
			if t {
				q.Triples++
			}
			as[k] = q
		}
	}
	return g, fs, hs, ps, es, as
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
