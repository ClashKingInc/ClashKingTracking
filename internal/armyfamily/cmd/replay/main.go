package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"clashking_tracking/internal/armyfamily"
	"github.com/clashkinginc/clashy.go"
)

type item struct {
	ID       int `json:"id"`
	Quantity int `json:"quantity"`
}
type equipment struct {
	HeroID      int `json:"heroId"`
	EquipmentID int `json:"equipmentId"`
}
type composition struct {
	ShareCode  string      `json:"share_code"`
	MainTroops []item      `json:"main_troops"`
	Spells     []item      `json:"spells"`
	Equipment  []equipment `json:"equipment"`
}
type snapshot struct {
	Kind        string      `json:"kind"`
	Composition composition `json:"composition"`
}
type outcome struct {
	Code string `json:"code"`
	N    int64  `json:"n"`
}

func scan(path string, consume func([]byte) error) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	s.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for s.Scan() {
		if e = consume(s.Bytes()); e != nil {
			return e
		}
	}
	return s.Err()
}
func main() {
	root := flag.String("root", "", "local export directory with full-snapshot.jsonl and lab-outcomes/*.jsonl")
	daysFlag := flag.String("days", "", "optional comma-separated day list; defaults to available outcome files")
	inspect := flag.Bool("inspect", false, "print setup evidence for the family with most setups on each day")
	lift := flag.Float64("lift", 1.3, "minimum setup enrichment")
	overlap := flag.Float64("overlap", .85, "maximum weighted Jaccard overlap before deduplication")
	novel := flag.Float64("novel", .4, "minimum novel attack share among patterns sharing hero equipment")
	flag.Parse()
	if *root == "" {
		fmt.Fprintln(os.Stderr, "usage: replay -root /path/to/local/army-research [-days 2026-09-15,2026-09-16]")
		os.Exit(2)
	}
	days := []string{}
	if *daysFlag != "" {
		days = strings.Split(*daysFlag, ",")
	} else {
		files, e := filepath.Glob(filepath.Join(*root, "lab-outcomes", "*.jsonl"))
		if e != nil {
			panic(e)
		}
		for _, file := range files {
			days = append(days, strings.TrimSuffix(filepath.Base(file), ".jsonl"))
		}
	}
	sort.Strings(days)
	static, e := clashy.LoadStaticData()
	if e != nil {
		panic(e)
	}
	comps := map[string]composition{}
	e = scan(filepath.Join(*root, "full-snapshot.jsonl"), func(raw []byte) error {
		var r snapshot
		if e := json.Unmarshal(raw, &r); e != nil {
			return e
		}
		if r.Kind == "army" {
			comps[r.Composition.ShareCode] = r.Composition
		}
		return nil
	})
	if e != nil {
		panic(e)
	}
	supportNames := map[string]bool{"Healer": true, "Druid": true, "Apprentice Warden": true, "Headhunter": true, "Ice Golem": true, "Wall Breaker": true, "Super Wall Breaker": true, "Sneaky Goblin": true, "Goblin": true, "Archer": true, "Barbarian": true, "Minion": true}
	support := map[int]bool{}
	identitySupport := map[int]bool{}
	conditionalCore := map[int]bool{}
	weight := map[int]int{}
	for _, c := range comps {
		for _, x := range c.MainTroops {
			if _, ok := weight[x.ID]; ok {
				continue
			}
			v := static.LookupByID(x.ID)
			w := 1
			if h, ok := v["housing_space"].(float64); ok && h > 0 {
				w = int(h)
			}
			weight[x.ID] = w
			if supportNames[fmt.Sprint(v["name"])] {
				support[x.ID] = true
			}
			if fmt.Sprint(v["name"]) == "Healer" {
				identitySupport[x.ID] = true
			}
			if fmt.Sprint(v["name"]) == "Furnace" {
				conditionalCore[x.ID] = true
			}
		}
	}
	previous := map[string]bool{}
	previousSetups := map[string]bool{}
	for _, day := range days {
		rows := []armyfamily.DailyRecipe{}
		missing := int64(0)
		e = scan(filepath.Join(*root, "lab-outcomes", day+".jsonl"), func(raw []byte) error {
			var o outcome
			if e := json.Unmarshal(raw, &o); e != nil {
				return e
			}
			c, ok := comps[o.Code]
			if !ok {
				missing += o.N
				rows = append(rows, armyfamily.DailyRecipe{Attacks: o.N})
				return nil
			}
			r := armyfamily.DailyRecipe{Code: o.Code, Attacks: o.N, TroopHousing: map[int]int{}, Spells: map[int]int{}}
			for _, x := range c.MainTroops {
				r.TroopHousing[x.ID] += x.Quantity * weight[x.ID]
			}
			for _, x := range c.Spells {
				r.Spells[x.ID] += x.Quantity
			}
			for _, x := range c.Equipment {
				r.Equipment = append(r.Equipment, armyfamily.HeroEquipment{HeroID: x.HeroID, EquipmentID: x.EquipmentID})
			}
			rows = append(rows, r)
			return nil
		})
		if e != nil {
			panic(e)
		}
		if len(rows) == 0 {
			fmt.Printf("%s empty outcome file; no replay\n", day)
			continue
		}
		start := time.Now()
		result := armyfamily.ClassifyDaily(rows, armyfamily.DailyOptions{SupportTroops: support,
			IdentitySupportTroops: identitySupport, ConditionalCoreTroops: conditionalCore,
			MinimumSetupLift: *lift, MaximumOverlap: *overlap, MinimumNovelShare: *novel})
		elapsed := time.Since(start)
		keys := map[string]bool{}
		setupKeys := map[string]bool{}
		setups := 0
		setupCounts := make([]int, 0, len(result.Families))
		for _, f := range result.Families {
			keys[f.Signature] = true
			setups += len(f.Setups)
			setupCounts = append(setupCounts, len(f.Setups))
			for _, setup := range f.Setups {
				setupKeys[f.Signature+"/"+setup.Signature] = true
			}
		}
		sort.Ints(setupCounts)
		median, p90, maximum, moreThanTen := 0, 0, 0, 0
		if len(setupCounts) > 0 {
			median = setupCounts[len(setupCounts)/2]
			p90 = setupCounts[(9*(len(setupCounts)-1))/10]
			maximum = setupCounts[len(setupCounts)-1]
			for _, count := range setupCounts {
				if count > 10 {
					moreThanTen++
				}
			}
		}
		stable := 0
		for key := range keys {
			if previous[key] {
				stable++
			}
		}
		stableSetups := 0
		for key := range setupKeys {
			if previousSetups[key] {
				stableSetups++
			}
		}
		fmt.Printf("%s recipes=%d attacks=%d classified=%d review=%d missingSnapshot=%d families=%d setups=%d setupMedian=%d setupP90=%d setupMax=%d familiesOver10=%d repeatedPriorGroups=%d repeatedPriorSetups=%d runtime=%s\n", day, len(rows), result.TotalAttacks, result.ClassifiedAttacks, result.ReviewAttacks, missing, len(result.Families), setups, median, p90, maximum, moreThanTen, stable, stableSetups, elapsed)
		if *inspect && maximum > 0 {
			for _, family := range result.Families {
				if len(family.Setups) != maximum {
					continue
				}
				fmt.Printf("  family=%s attacks=%d distinctRecipes=%d\n", family.Signature, family.Attacks, family.RecipeCount)
				for _, setup := range family.Setups {
					fmt.Printf("    %s attacks=%d recipes=%d share=%.3f phi=%.3f lift=%.2f\n", setup.Signature, setup.Evidence.Attacks, setup.Evidence.RecipeCount, setup.Evidence.Share, setup.Evidence.Association, setup.Evidence.Lift)
				}
				break
			}
		}
		previous = keys
		previousSetups = setupKeys
	}
}
