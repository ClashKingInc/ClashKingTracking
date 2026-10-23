package armyfamily

import (
	"encoding/json"
	"math"
	"sort"
)

// DailyRecipe is one distinct, attack-used army on one finalized day. Code must
// be canonical; the caller must first move a main-army siege into its CC slot.
// TroopHousing excludes CC troops and siege machines. Counts are attack usage,
// never defense usage or a count of saved/shareable armies.
type DailyRecipe struct {
	Code         string
	Attacks      int64
	TroopHousing map[int]int
	Spells       map[int]int
	Equipment    []HeroEquipment
}

type HeroEquipment struct {
	HeroID, EquipmentID int
}

type DailyOptions struct {
	SupportTroops map[int]bool
	// ConditionalCoreTroops are auxiliary in small quantities, but may define
	// a separate family when at least a quarter of the full army's housing.
	ConditionalCoreTroops map[int]bool
	// IdentitySupportTroops distinguish families even when their housing share is
	// small. A healer army must not collapse into its healerless counterpart.
	IdentitySupportTroops map[int]bool
	// Gates are descriptive heuristics, not statistical significance tests.
	MinimumSetupAttacks int64
	MinimumSetupRecipes int
	MinimumSetupShare   float64
	MinimumSetupLift    float64
	MaximumOverlap      float64
	MinimumNovelShare   float64
}

type SetupCondition struct {
	Kind    string `json:"kind"` // equipment or spell
	HeroID  int    `json:"heroId,omitempty"`
	ID      int    `json:"id"`
	Minimum int    `json:"minimum"`
}

type SetupEvidence struct {
	Attacks      int64   `json:"attacks"`
	RecipeCount  int     `json:"recipeCount"`
	Share        float64 `json:"share"`
	Association  float64 `json:"association"`
	Lift         float64 `json:"lift"`
	NovelAttacks int64   `json:"novelAttacks"`
	NovelShare   float64 `json:"novelShare"`
}

type DailySetup struct {
	// Signature is a canonical JSON array of conditions, suitable as a stable key.
	Signature          string           `json:"signature"`
	Conditions         []SetupCondition `json:"conditions"`
	Evidence           SetupEvidence    `json:"evidence"`
	CoreTroopIDs       []int            `json:"coreTroopIds"`
	RepresentativeCode string           `json:"representativeCode"`
}

type DailyFamily struct {
	// Signature is a canonical JSON array of attacking troop IDs.
	Signature          string       `json:"signature"`
	CoreTroopIDs       []int        `json:"coreTroopIds"`
	Attacks            int64        `json:"attacks"`
	RecipeCount        int          `json:"recipeCount"`
	RepresentativeCode string       `json:"representativeCode"`
	Setups             []DailySetup `json:"setups"`
}

type DailyResult struct {
	TotalAttacks      int64         `json:"totalAttacks"`
	ClassifiedAttacks int64         `json:"classifiedAttacks"`
	ReviewAttacks     int64         `json:"reviewAttacks"`
	Families          []DailyFamily `json:"families"`
	// CodeFamily is transient aggregation help, not a stored membership table.
	CodeFamily map[string]string `json:"-"`
}

// ClassifyDaily discovers troop families on this day, then independently mines
// overlapping setup conditions inside each family. No fixed family/setup cap is
// applied. Missing/invalid compositions remain in TotalAttacks and review.
func ClassifyDaily(input []DailyRecipe, options DailyOptions) DailyResult {
	if options.MinimumSetupAttacks <= 0 {
		options.MinimumSetupAttacks = 50
	}
	if options.MinimumSetupRecipes <= 0 {
		options.MinimumSetupRecipes = 5
	}
	if options.MinimumSetupShare <= 0 {
		options.MinimumSetupShare = .04
	}
	if options.MinimumSetupLift <= 0 {
		options.MinimumSetupLift = 1.3
	}
	if options.MaximumOverlap <= 0 {
		options.MaximumOverlap = .85
	}
	if options.MinimumNovelShare <= 0 {
		options.MinimumNovelShare = .4
	}
	result := DailyResult{CodeFamily: map[string]string{}}
	rows := make([]DailyRecipe, 0, len(input))
	baseline := dailyHousingBaseline(input)
	for _, row := range input {
		if row.Attacks <= 0 {
			continue
		}
		result.TotalAttacks += row.Attacks
		housing := housingTotal(row.TroopHousing)
		if row.Code == "" || baseline == 0 || float64(housing) < .9*float64(baseline) || float64(housing) > 1.1*float64(baseline) {
			result.ReviewAttacks += row.Attacks
			continue
		}
		rows = append(rows, row)
	}
	// Sorting gives deterministic prototypes regardless of map or query order.
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Attacks != rows[j].Attacks {
			return rows[i].Attacks > rows[j].Attacks
		}
		return rows[i].Code < rows[j].Code
	})
	evidenceKeys := make([]string, len(rows))
	for i := range rows {
		evidenceKeys[i] = recipeEvidenceKey(rows[i])
	}
	type prototype struct {
		housing map[int]int
		members []int
		core    []int
	}
	prototypes := []prototype{}
	for i, row := range rows {
		best, overlap := -1, .8
		for j, p := range prototypes {
			if !sameIdentitySupport(row.TroopHousing, p.housing, options.IdentitySupportTroops) ||
				!sameConditionalDominance(row.TroopHousing, p.housing, options.ConditionalCoreTroops) {
				continue
			}
			s := troopOverlap(row.TroopHousing, p.housing, options.SupportTroops, options.ConditionalCoreTroops)
			if s >= overlap {
				best, overlap = j, s
			}
		}
		if best < 0 {
			prototypes = append(prototypes, prototype{housing: row.TroopHousing, members: []int{i}})
		} else {
			prototypes[best].members = append(prototypes[best].members, i)
		}
	}
	groups := map[string][]int{}
	cores := map[string][]int{}
	for _, p := range prototypes {
		average := map[int]int64{}
		var total int64
		for _, i := range p.members {
			for id, h := range rows[i].TroopHousing {
				if h > 0 {
					average[id] += int64(h) * rows[i].Attacks
					total += int64(h) * rows[i].Attacks
				}
			}
		}
		var combatTotal int64
		for id, h := range average {
			if !options.SupportTroops[id] {
				combatTotal += h
			}
		}
		for id, h := range average {
			if !options.SupportTroops[id] && 4*h >= combatTotal &&
				(!options.ConditionalCoreTroops[id] || 4*h >= total) {
				p.core = append(p.core, id)
			}
		}
		if len(p.core) == 0 {
			bestID, best := 0, int64(0)
			for id, h := range average {
				if !options.SupportTroops[id] && (h > best || h == best && id < bestID) {
					bestID, best = id, h
				}
			}
			if bestID > 0 {
				p.core = []int{bestID}
			}
		}
		if len(p.core) == 0 {
			for _, i := range p.members {
				result.ReviewAttacks += rows[i].Attacks
			}
			continue
		}
		// A support troop can distinguish related armies, but must never be
		// the only named core while a fighting troop is present.
		for id, h := range average {
			if options.IdentitySupportTroops[id] && 20*h >= total {
				p.core = append(p.core, id)
			}
		}
		sort.Ints(p.core)
		key := signature(p.core)
		cores[key] = p.core
		for _, i := range p.members {
			valid := true
			for _, id := range p.core {
				minimumShare := 10
				if options.IdentitySupportTroops[id] {
					minimumShare = 20
				}
				if minimumShare*rows[i].TroopHousing[id] < housingTotal(rows[i].TroopHousing) {
					valid = false
					break
				}
			}
			if valid {
				groups[key] = append(groups[key], i)
			} else {
				result.ReviewAttacks += rows[i].Attacks
			}
		}
	}
	for key, members := range groups {
		family := DailyFamily{Signature: key, CoreTroopIDs: cores[key], RecipeCount: distinctRecipeCount(evidenceKeys, members)}
		for _, i := range members {
			family.Attacks += rows[i].Attacks
		}
		// A family below 0.1% of all attacks has too little public usage.
		// Its attacks remain in the daily denominator and review count.
		if family.Attacks*1000 < result.TotalAttacks {
			result.ReviewAttacks += family.Attacks
			continue
		}
		for _, i := range members {
			result.CodeFamily[rows[i].Code] = key
		}
		result.ClassifiedAttacks += family.Attacks
		family.RepresentativeCode = representative(rows, members)
		family.Setups = mineDailySetups(rows, evidenceKeys, members, family.CoreTroopIDs, result.TotalAttacks, options)
		result.Families = append(result.Families, family)
	}
	sort.Slice(result.Families, func(i, j int) bool {
		if result.Families[i].Attacks != result.Families[j].Attacks {
			return result.Families[i].Attacks > result.Families[j].Attacks
		}
		return result.Families[i].Signature < result.Families[j].Signature
	})
	return result
}

// The attack-weighted median is resistant to rare partial share codes while
// still following the housing capacity represented by this day's armies.
func dailyHousingBaseline(input []DailyRecipe) int {
	type housingUsage struct {
		housing int
		attacks int64
	}
	usage := make([]housingUsage, 0, len(input))
	var total int64
	for _, row := range input {
		if row.Code == "" || row.Attacks <= 0 {
			continue
		}
		if housing := housingTotal(row.TroopHousing); housing > 0 {
			usage = append(usage, housingUsage{housing, row.Attacks})
			total += row.Attacks
		}
	}
	if total == 0 {
		return 0
	}
	sort.Slice(usage, func(i, j int) bool { return usage[i].housing < usage[j].housing })
	var cumulative int64
	for _, row := range usage {
		cumulative += row.attacks
		if cumulative > total/2 {
			return row.housing
		}
	}
	return usage[len(usage)-1].housing
}

func sameIdentitySupport(a, b map[int]int, identity map[int]bool) bool {
	for id, enabled := range identity {
		if !enabled {
			continue
		}
		if (a[id] > 0) != (b[id] > 0) {
			return false
		}
	}
	return true
}

func sameConditionalDominance(a, b map[int]int, conditional map[int]bool) bool {
	at, bt := housingTotal(a), housingTotal(b)
	for id, enabled := range conditional {
		if enabled && (at > 0 && 4*a[id] >= at) != (bt > 0 && 4*b[id] >= bt) {
			return false
		}
	}
	return true
}

func signature[T any](value T) string { b, _ := json.Marshal(value); return string(b) }

func housingTotal(m map[int]int) int {
	n := 0
	for _, v := range m {
		if v > 0 {
			n += v
		}
	}
	return n
}

func troopOverlap(a, b map[int]int, support, conditional map[int]bool) float64 {
	var at, bt, shared float64
	aTotal, bTotal := housingTotal(a), housingTotal(b)
	weightFor := func(id int) float64 {
		if support[id] || conditional[id] && 4*a[id] < aTotal && 4*b[id] < bTotal {
			return .15
		}
		return 1
	}
	for id, v := range a {
		if v <= 0 {
			continue
		}
		weight := weightFor(id)
		at += float64(v) * weight
		if b[id] > 0 {
			shared += math.Min(float64(v), float64(b[id])) * weight
		}
	}
	for id, v := range b {
		if v <= 0 {
			continue
		}
		weight := weightFor(id)
		bt += float64(v) * weight
	}
	if at == 0 || bt == 0 {
		return 0
	}
	return shared / math.Max(at, bt)
}

func representative(rows []DailyRecipe, members []int) string {
	// A real, most-used code is preferable to a synthetic averaged army.
	best := members[0]
	for _, i := range members {
		if rows[i].Attacks > rows[best].Attacks || rows[i].Attacks == rows[best].Attacks && rows[i].Code < rows[best].Code {
			best = i
		}
	}
	return rows[best].Code
}

// This evidence key excludes code-only differences such as Warden mode, pets,
// siege placement, and CC troops. Five such codes are still one recipe for the
// setup-support gate. Hero-associated equipment and spell counts remain visible.
func recipeEvidenceKey(row DailyRecipe) string {
	equipment := append([]HeroEquipment(nil), row.Equipment...)
	sort.Slice(equipment, func(i, j int) bool {
		if equipment[i].HeroID != equipment[j].HeroID {
			return equipment[i].HeroID < equipment[j].HeroID
		}
		return equipment[i].EquipmentID < equipment[j].EquipmentID
	})
	return signature(struct {
		Troops    map[int]int
		Spells    map[int]int
		Equipment []HeroEquipment
	}{row.TroopHousing, row.Spells, equipment})
}

func distinctRecipeCount(keys []string, members []int) int {
	seen := make(map[string]struct{}, len(members))
	for _, i := range members {
		seen[keys[i]] = struct{}{}
	}
	return len(seen)
}

func MatchSetup(row DailyRecipe, conditions []SetupCondition) bool {
	for _, c := range conditions {
		switch c.Kind {
		case "spell":
			if row.Spells[c.ID] < c.Minimum {
				return false
			}
		case "equipment":
			found := false
			for _, e := range row.Equipment {
				if e.HeroID == c.HeroID && e.EquipmentID == c.ID {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		default:
			return false
		}
	}
	return len(conditions) > 0
}

func mineDailySetups(rows []DailyRecipe, evidenceKeys []string, members []int, core []int, allAttacks int64, options DailyOptions) []DailySetup {
	var total int64
	for _, i := range members {
		total += rows[i].Attacks
	}
	if distinctRecipeCount(evidenceKeys, members) < options.MinimumSetupRecipes {
		return nil
	}
	minimum := options.MinimumSetupAttacks
	if x := int64(math.Ceil(float64(total) * options.MinimumSetupShare)); x > minimum {
		minimum = x
	}
	// A discovered variation must be meaningful across the full daily
	// Legend population, not only within a small troop family.
	if x := (allAttacks + 99) / 100; x > minimum {
		minimum = x
	}
	featureMap := map[string]SetupCondition{}
	for _, i := range members {
		for _, e := range rows[i].Equipment {
			c := SetupCondition{Kind: "equipment", HeroID: e.HeroID, ID: e.EquipmentID, Minimum: 1}
			featureMap[signature(c)] = c
		}
		for id, count := range rows[i].Spells {
			for n := 1; n <= count && n <= 12; n++ {
				c := SetupCondition{Kind: "spell", ID: id, Minimum: n}
				featureMap[signature(c)] = c
			}
		}
	}
	features := make([]SetupCondition, 0, len(featureMap))
	for _, c := range featureMap {
		features = append(features, c)
	}
	sort.Slice(features, func(i, j int) bool { return signature(features[i]) < signature(features[j]) })
	type candidate struct {
		setup DailySetup
		mask  []bool
		score float64
	}
	candidates := []candidate{}
	for a := 0; a < len(features); a++ {
		for b := a + 1; b < len(features); b++ {
			left, right := features[a], features[b]
			if left.Kind == "spell" && right.Kind == "spell" {
				continue
			}
			if left.Kind == "equipment" && right.Kind == "equipment" && left.HeroID != right.HeroID {
				continue
			}
			if left.Kind == right.Kind && left.ID == right.ID {
				continue
			}
			mask := make([]bool, len(members))
			var la, ra, both int64
			matched := make([]int, 0)
			for j, i := range members {
				l, r := MatchSetup(rows[i], []SetupCondition{left}), MatchSetup(rows[i], []SetupCondition{right})
				if l {
					la += rows[i].Attacks
				}
				if r {
					ra += rows[i].Attacks
				}
				if l && r {
					both += rows[i].Attacks
					matched = append(matched, i)
					mask[j] = true
				}
			}
			recipes := distinctRecipeCount(evidenceKeys, matched)
			if both < minimum || recipes < options.MinimumSetupRecipes {
				continue
			}
			p, q := float64(la)/float64(total), float64(ra)/float64(total)
			if p <= .04 || p >= .96 || q <= .04 || q >= .96 {
				continue
			}
			joint := float64(both) / float64(total)
			den := math.Sqrt(p * (1 - p) * q * (1 - q))
			if den == 0 {
				continue
			}
			phi := (joint - p*q) / den
			lift := joint / (p * q)
			if phi < .22 || lift < options.MinimumSetupLift {
				continue
			}
			conditions := []SetupCondition{left, right}
			eligible := make([]int, 0, len(matched))
			for j, ok := range mask {
				if ok {
					eligible = append(eligible, members[j])
				}
			}
			candidates = append(candidates, candidate{setup: DailySetup{Signature: signature(conditions), Conditions: conditions, Evidence: SetupEvidence{Attacks: both, RecipeCount: recipes, Share: joint, Association: phi, Lift: lift}, CoreTroopIDs: append([]int(nil), core...), RepresentativeCode: representative(rows, eligible)}, mask: mask, score: phi * math.Sqrt(joint)})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		return candidates[i].setup.Signature < candidates[j].setup.Signature
	})
	out := []DailySetup{}
	chosen := []candidate{}
	for _, c := range candidates {
		redundant := false
		for _, prior := range chosen {
			// Count-threshold variants of the same ingredient pair compete.
			a, b := c.setup.Conditions, prior.setup.Conditions
			if a[0].Kind == b[0].Kind && a[0].ID == b[0].ID && a[0].HeroID == b[0].HeroID && a[1].Kind == b[1].Kind && a[1].ID == b[1].ID && a[1].HeroID == b[1].HeroID {
				redundant = true
				break
			}
			intersection, union := int64(0), int64(0)
			for j, i := range members {
				x, y := c.mask[j], prior.mask[j]
				if x || y {
					union += rows[i].Attacks
				}
				if x && y {
					intersection += rows[i].Attacks
				}
			}
			if union > 0 && float64(intersection)/float64(union) >= options.MaximumOverlap {
				redundant = true
				break
			}
		}
		if !redundant {
			// A second spell paired with the same hero equipment needs
			// independent attack coverage. Multiple correlated spells in one
			// package should not each become another setup row.
			sharedGear := make([]bool, len(members))
			for _, prior := range chosen {
				if !sameEquipmentFeature(c.setup.Conditions, prior.setup.Conditions) {
					continue
				}
				for j, ok := range prior.mask {
					sharedGear[j] = sharedGear[j] || ok
				}
			}
			var novel int64
			for j, ok := range c.mask {
				if ok && !sharedGear[j] {
					novel += rows[members[j]].Attacks
				}
			}
			if novel > 0 && float64(novel)/float64(c.setup.Evidence.Attacks) < options.MinimumNovelShare {
				redundant = true
			}
			if novel == 0 && len(chosen) > 0 {
				// Only suppress when at least one prior pattern actually
				// shares the gear; otherwise this is a new independent gear.
				for _, prior := range chosen {
					if sameEquipmentFeature(c.setup.Conditions, prior.setup.Conditions) {
						redundant = true
						break
					}
				}
			}
			if !redundant {
				c.setup.Evidence.NovelAttacks = novel
				c.setup.Evidence.NovelShare = float64(novel) / float64(c.setup.Evidence.Attacks)
			}
		}
		if !redundant {
			out = append(out, c.setup)
			chosen = append(chosen, c)
		}
	}
	return out
}

func sameEquipmentFeature(left, right []SetupCondition) bool {
	for _, a := range left {
		if a.Kind != "equipment" {
			continue
		}
		for _, b := range right {
			if b.Kind == "equipment" && a.HeroID == b.HeroID && a.ID == b.ID {
				return true
			}
		}
	}
	return false
}
