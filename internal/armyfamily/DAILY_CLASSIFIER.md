# Daily army setup discovery

`ClassifyDaily` accepts attack-use counts for distinct canonical army codes on one
finalized day. The caller supplies main-army troop housing, spell counts, and
hero-associated equipment. It excludes Clan Castle troops, siege machines,
pets, and Warden mode from troop identity. The caller normalizes share codes so
the representative places a siege machine in the Clan Castle slot. Unknown or
incomplete armies remain in `totalAttacks` and `reviewAttacks` but have no public
family row. The transient `CodeFamily` map and `MatchSetup` conditions support
actual attack-outcome aggregation without storing all compositions.

The troop step uses a deterministic, usage-sorted greedy prototype: a recipe
joins a prototype at 80% housing overlap after support troops are downweighted.
Prototypes with the same major fighting troops (at least 25% average combat housing,
or the largest combat troop) become one family. Each assigned recipe must carry
at least 10% housing for every core troop. The complete-army baseline is the
daily attack-weighted median housing; armies outside ±10% are reviewed. Meaningful
Healer support is retained in identity so healerless armies remain distinguishable.
Furnace is conditional: below 25% of the full army's housing it is
downweighted for overlap and excluded from the core, while an army dedicating
at least a quarter of its housing to Furnaces can form a separate Furnace-led
prototype. This prevents one to three Furnaces from splitting a Rocket Balloon
+ Super Minion family without erasing armies built around five or more Furnaces
at the observed 320-housing baseline. The cutoff is a housing share, not a
global ban on the troop.
Families below 0.1% of daily attacks are reviewed rather than stored, without
reducing the attack denominator. Family keys are sorted troop-ID JSON arrays. Spells and equipment
never create a top-level family.

Within each family, equipment-to-spell and same-hero equipment-pair conditions
are mined from observed attacks. A setup needs at least five distinct
troop/spell/equipment recipes, 50 attacks, 4% family usage, and 1% of all
daily Legend-I attacks (whichever is largest), positive association
`phi >= .22`, and enrichment `lift >= 1.3`.
Identical ingredient pairs with different spell thresholds and occurrence sets
with weighted Jaccard overlap at least .85 are deduplicated. When a candidate
uses hero equipment already represented by selected patterns, at least 40% of
its matched attacks must be novel relative to those patterns. This curbs lists
of correlated spell pairs around the same equipment without suppressing an
independent gear package. There is no fixed setup count. The gates are
exploratory heuristics, not a significance test or a
claim that a pair is a named tactic. Setup conditions form the stable JSON key;
setups can overlap and a day without a supported setup does not prove zero use.
Each selected setup stores its novel attack count and fraction alongside support,
association, and lift, so the conditional redundancy decision is inspectable.

Compared with the offline army lab's `rework_parents.py`, this implementation
uses every observed daily recipe as a possible prototype, ordered by attack
usage, instead of a weighted 20,000-recipe sample. It applies overlap to raw
housing with explicit support weighting rather than the lab's preprocessed
troop matrix and unit-vector assignment radius. It does not use BIRCH, NMF,
reviewed note candidates, or manually assigned names. The lab's setup miner
limited display to six patterns and counted raw code rows; this classifier has
no display cap and counts semantically distinct recipes after ignoring code-only
Warden, pet, and siege variants. Those choices prioritize repeatable daily
keys and avoid inflating the five-recipe gate.

For an offline export containing `full-snapshot.jsonl` and
`lab-outcomes/YYYY-MM-DD.jsonl`, run:

```sh
go run ./internal/armyfamily/cmd/replay -root /path/to/army-research
```

The command reads local files only and prints attack coverage, family/setup
counts, setup distribution, key recurrence, and classifier runtime. A stale
snapshot reports its missing attack usage explicitly.

The revised classifier produced 17/19 families and 62/62 setups on the complete
September15/16 exports, with 721/72,265 and 666/80,876 review attacks. The numbers
below document the earlier threshold experiments, not the revised output.

The available local export has complete compositions for September 15 and 16,
2026. Those days produced 29 and 37 troop families, 49 and 50 supported setups,
and 71/72,265 and 87/80,876 review attacks, respectively. The largest family
had 10 setups on each day; 28 of the first day's 29 family keys and 33 of its
49 setup keys reappeared on the next day. Classification took about 2.5 and
3.4 seconds on this machine. September 12-14 outcome files are empty; the
September 17-18 snapshot lacks 29,764 and 35,543 attack uses' compositions, so
those later files cannot establish full-coverage rates.

Sensitivity on the two complete export days: at lift 1.15 with the conditional
novelty gate effectively disabled, the catalog held 87/99 setups and the
largest family held 22. Raising lift to 1.3 gave 73/79 and still 22 in the
largest family. Adding a 20% novel-attack gate gave 54/55, with maximum 12/13;
30% gave 51/53 and maximum 11/12; the selected 40% gave 49/50 and maximum
10/10. Changing weighted Jaccard deduplication from .85 to .70 at the 20%
novelty setting barely changed totals (55/54), so conditional novelty explains
most of the reduction. These are local observations, not threshold validation
against expert-labeled setups.
