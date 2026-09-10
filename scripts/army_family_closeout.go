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

type decodedArmy struct {
	code        string
	record      armyCompositionRecord
	composition armyfamily.Composition
	usage       int64
}
type legendAttack struct {
	player, code       string
	stars, destruction int
	duration           *int32
}
type dailyCounts struct {
	attacks, zero, one, two, three, destruction, duration, durationCount int64
	players                                                              map[string]struct{}
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
	writes := 0
	for _, code := range unassignedCodes(usage, members) {
		a := decoded[code]
		match, ok := armyfamily.FindDirectAnchor(a.composition, anchors)
		if !ok {
			h, e := representativeIDs(a.record)
			if err = tx.QueryRow(ctx, `INSERT INTO army_families(representative_share_code,name,hero_ids,equipment_ids) VALUES($1,NULL,$2,$3) ON CONFLICT(representative_share_code) DO UPDATE SET representative_share_code=EXCLUDED.representative_share_code RETURNING family_id`, code, h, e).Scan(&match.FamilyID); err != nil {
				return 0, err
			}
			match.TroopHousingSimilarity, match.SpellCapacitySimilarity, match.EquipmentSimilarity = 1, 1, 1
			anchors = append(anchors, armyfamily.Anchor{FamilyID: match.FamilyID, ShareCode: code, Composition: a.composition})
			writes++
		}
		tag, e := tx.Exec(ctx, `INSERT INTO army_family_members(share_code,family_id,troop_similarity,spell_similarity,equipment_similarity) VALUES($1,$2,$3,$4,$5) ON CONFLICT(share_code) DO NOTHING`, code, match.FamilyID, match.TroopHousingSimilarity, match.SpellCapacitySimilarity, match.EquipmentSimilarity)
		if e != nil {
			return 0, e
		}
		writes += int(tag.RowsAffected())
		members[code] = match.FamilyID
	}
	g, fams, heroes, pets, equipment, assignments := aggregateLegend(attacks, members, decoded)
	for _, anchor := range anchors {
		if fams[anchor.FamilyID] == nil {
			fams[anchor.FamilyID] = newCounts()
		}
	}
	day = dayStart(day)
	if _, err = tx.Exec(ctx, `DELETE FROM army_family_daily_stats_v2 WHERE day=$1; DELETE FROM legend_daily_stats_v2 WHERE day=$1`, day); err != nil {
		return 0, err
	}
	for _, id := range familyIDs(fams) {
		c := fams[id]
		_, err = tx.Exec(ctx, `INSERT INTO army_family_daily_stats_v2(family_id,day,attack_count,distinct_player_count,zero_star_count,one_star_count,two_star_count,three_star_count,destruction_percentage_sum,duration_seconds_sum,duration_count) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, id, day, c.attacks, len(c.players), c.zero, c.one, c.two, c.three, c.destruction, c.duration, c.durationCount)
		if err != nil {
			return 0, err
		}
		writes++
	}
	hj, _ := json.Marshal(itemStats(heroes))
	pj, _ := json.Marshal(itemStats(pets))
	ej, _ := json.Marshal(itemStats(equipment))
	aj, _ := json.Marshal(assignmentStats(assignments))
	_, err = tx.Exec(ctx, `INSERT INTO legend_daily_stats_v2(day,attack_count,distinct_player_count,perfect_320_player_count,zero_star_count,one_star_count,two_star_count,three_star_count,destruction_percentage_sum,duration_seconds_sum,duration_count,hero_stats,pet_stats,equipment_stats,pet_hero_assignments) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`, day, g.attacks, len(g.players), perfectPlayers(attacks), g.zero, g.one, g.two, g.three, g.destruction, g.duration, g.durationCount, json.RawMessage(hj), json.RawMessage(pj), json.RawMessage(ej), json.RawMessage(aj))
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return writes + 1, nil
}
func legendDayWindow(day time.Time) (time.Time, time.Time) {
	s := dayStart(day).Add(5*time.Hour + 10*time.Minute)
	return s, s.Add(24 * time.Hour)
}
func readLegendAttacks(ctx context.Context, tx pgx.Tx, start, end time.Time) ([]legendAttack, map[string]int64, error) {
	r, e := tx.Query(ctx, `SELECT player_tag,stars,destruction_percentage,duration_seconds,coalesce(share_code,'') FROM battles_ranked WHERE battle_mode='legend' AND direction='attack' AND battle_time >= $1 AND battle_time < $2 ORDER BY player_tag,battle_time`, start, end)
	if e != nil {
		return nil, nil, e
	}
	defer r.Close()
	var out []legendAttack
	u := map[string]int64{}
	for r.Next() {
		var a legendAttack
		if e = r.Scan(&a.player, &a.stars, &a.destruction, &a.duration, &a.code); e != nil {
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
func representativeIDs(r armyCompositionRecord) ([]int32, []int32) {
	h := map[int32]bool{}
	e := map[int32]bool{}
	for _, x := range r.Heroes {
		h[x] = true
	}
	for _, x := range r.Equipment {
		e[int32(x.EquipmentID)] = true
	}
	hs := []int32{}
	es := []int32{}
	for x := range h {
		hs = append(hs, x)
	}
	for x := range e {
		es = append(es, x)
	}
	sort.Slice(hs, func(i, j int) bool { return hs[i] < hs[j] })
	sort.Slice(es, func(i, j int) bool { return es[i] < es[j] })
	return hs, es
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
		c.durationCount++
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
func perfectPlayers(a []legendAttack) int64 {
	type p struct{ n, t int }
	m := map[string]p{}
	for _, x := range a {
		q := m[x.player]
		q.n++
		if x.stars == 3 {
			q.t++
		}
		m[x.player] = q
	}
	var n int64
	for _, q := range m {
		if q.n == 8 && q.t == 8 {
			n++
		}
	}
	return n
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
