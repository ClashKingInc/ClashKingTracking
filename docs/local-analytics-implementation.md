# Local analytics implementation — September 22, 2026

## September 23 data revision: combat core, variants, sieges, and rank history

This section supersedes the earlier local replay figures and the earlier 25%-of-total-housing core rule below. The current classifier selects fighting troops contributing at least 25% of *combat* housing, with a largest-fighting-troop fallback; Healer is added separately when it materially distinguishes a family. Each assigned army still needs at least 10% of its full housing for each fighting core. Furnace is conditional: below 25% of the *full* army's housing it is downweighted for prototype overlap and excluded as a core; at or above that share it can define a distinct Furnace-led family. The six-day local replay now has one stable `[4000007,4000057,4000084]` Healer + Rocket Balloon + Super Minion family with 28,843 attacks. The previous two keys had 13,961 and 14,824 attacks; even an identical representative code containing `3x150` appeared under both keys on different days. After the correction that code belongs to the single merged key on all six days. Genuinely Furnace-heavy families remain, including one whose representative has `5x150`. These are observed classifications, not hand-labeled army names.

Setup discovery retains the five-distinct-semantic-recipes, minimum-50-attacks, 4%-of-family, phi ≥0.22, lift ≥1.3, overlap, and novelty rules, and now additionally requires at least 1% of *all daily Legend-I attacks* for each variant. The API also hides variant rows below 1% of all attacks in the selected range/cohort, without changing totals. This is an attack-usage floor, not a fixed maximum count or a p-value. Lift measures enrichment within a family; it does not by itself establish that a Warden-equipment/spell combination is a meaningful army variation, which is why full-population support and distinct recipes remain necessary. Variants are discovered from Overall and counted in Top 1,000/200 using the same definitions; their small-cohort counts are not independently significance-tested.

Forward Goose migration036 adds `army_setup_daily_stats.siege_usage jsonb NOT NULL DEFAULT '[]'` (an array of `{id,attacks}`) to preserve **observed** normalized Clan Castle siege counts per day, cohort, family, and variant. The same selected siege is extracted from each normalized attack code before aggregation; the display representative code is never used to infer percentages. Counts may sum below `attack_count` when a code has no identified siege. The API combines six days' counts, sorts descending, and exposes only the top three as `sieges:[{id,attacks,usageRate}]`, with usageRate divided by that family/variant's own attacks.

`GET /v2/stats/army-setups` and its timeline now expose `dailyRank`, `previousDayRank`, and `rankChange` (positive is an improvement). List ranks are anchored to the *latest completed day in the selected range*, even when an individual family was last observed earlier; missing observation on either day yields null, never zero or a guessed trend. Ranking compares families against families, or variants against variants within the selected family, using the selected cohort and `usage`/`tripleRate` sort. `GET /v2/stats/army-setups/rank-history` accepts `groupKey`, optional `variantKey`, `rankLimit` (omitted, 1000, or 200), `sort`, and the date-window keys. It returns `{firstDay,lastDay,leagueTierId,rankLimit,groupKey,variantKey,sort,points:[{day,attacks,rank,previousDayRank,rankChange}]}`; all four measurements can be null on a completed day without that observation.

Only the disposable local `clashking_dev` database on port54330 received migration036 and the September15–20 replay. The prior combat-core/variant revision had 660 cohort/day observation rows (312 overview, 348 variant); the conditional-Furnace replay has 648 (300 overview, 348 variant). The earlier pre-cutoff revision had 1,135 total rows, but its *variant-only* count was not saved, so 1,135→660 must not be attributed solely to the cutoff. Overall classified attacks are now 463,589 of unchanged 470,192 total attacks, and the selected-range list shows 18 overview groups. The local API on `127.0.0.1:8787` returned HTTP200 with these figures and the new fields; no production migration, deployment, external tunnel check, or visual test was performed for this revision.

Real local response excerpts:

```json
{"groupKey":"[4000007,4000057,4000084]","attacks":28843,"sieges":[{"id":4000091,"attacks":19448,"usageRate":0.6742710536351975},{"id":4000087,"attacks":5393,"usageRate":0.18697777623686856},{"id":4000135,"attacks":2327,"usageRate":0.08067815414485317}],"dailyRank":5,"previousDayRank":5,"rankChange":0}
```

The matching rank-history endpoint returned ranks 5, 4, 5, 5, 5, 5 from September15–20; September15 has a null previous-day rank because no prior completed observation was available in the local proof window.

## Follow-up revision: observed housing, presentation, and comparisons

The current source replaces the 200-housing cutoff with the daily attack-weighted median housing, accepting armies within ±10%. Healers are retained as meaningful support in family identities, so healer and healerless armies need not collapse into one overview. Families below 0.1% of daily overall attacks are not persisted; those attacks remain in `totalAttacks`. There is still no maximum setup count.

Forward migration035 removes `army_analysis_completed_at`. A non-null `classified_army_attacks` marks a completed atomic write, including zero. The schema excerpt below describes the resulting schema, not a replacement for previously applied migration034.

The API adds per-army cohort comparisons and whole-league benchmark time series. CWL responses add available stored seasons and season-level history. The app now uses expandable army/variant cards, fixed 90-day War/Ranked/Troop Stats pages, date-scrubbable charts, percentage-based CWL roster mixes, and complete ordered distribution bars. An estimated CWL roster mix is explicitly not an observed war lineup.

The six-day examples in the original sections below are historical evidence from the previous classifier revision. The revised local database replay has now completed after Goose migration035: 1,135 observation rows, 470,192 total attacks, 465,746 classified attacks, and 18 visible overview groups across the six-day window. Read-only endpoint proof is saved in `/private/tmp/clashking-analytics-endpoint-examples.json`. A separate offline replay of complete export files produced 17 families/62 setups from 72,265 attacks on September15 (71,544 classified) and 19 families/62 setups from 80,876 attacks on September16 (80,210 classified). Those export populations differ from the database populations and should not be compared as identical samples.

### Updated endpoint fields and validation

Actual HTTP validation of `/v2/stats/army-setups?time[after]=2026-09-15&time[before]=2026-09-20` returned 200 through both localhost8787 and the local phone-facing tunnel. It returned 470,192 total attacks, 465,746 classified attacks and 18 groups. The first group's comparison rows contained 187,681/470,192 attacks overall (53.19% three-star rate), 16,337/47,493 in Top1,000 (76.28%), and 3,439/9,486 in Top200 (83.75%). These rates describe the army; timeline benchmark rates separately describe the entire league cohort.

The JSON fragments below are illustrative shapes, not database extracts. `GET /v2/stats/army-setups` still returns the existing overview shape. Overall responses now attach `comparisons` to each item; requesting `rankLimit=1000` or `rankLimit=200` selects that cohort instead. The overview list excludes groups below 0.1% of the selected period's attacks; setup variants are not subjected to this list-only cutoff.

```json
{"comparisons":[
  {"rankLimit":null,"attacks":400,"totalAttacks":1000,"usageRate":0.4,"threeStarRate":0.55},
  {"rankLimit":1000,"attacks":100,"totalAttacks":200,"usageRate":0.5,"threeStarRate":0.72},
  {"rankLimit":200,"attacks":30,"totalAttacks":50,"usageRate":0.6,"threeStarRate":0.8}
]}
```

The existing army timeline endpoint adds `benchmarks`. These are daily hit rates for all attacks in each whole-league cohort, not rates for this army. The army's own daily measurements remain in `items`; absent observations remain absent, rather than being rendered as zero.

```json
{"benchmarks":[{"rankLimit":null,"points":[{"day":"2026-09-20","attacks":1000,"threeStarRate":0.6}]}]}
```

The existing CWL participation endpoint now also returns stored season choices and global historical totals. It does not invent older seasons.

```json
{"availableSeasons":["2026-09"],"history":[{"season":"2026-09","clanCount":364,"registeredPlayerCount":9701,"groupCount":46}]}
```

Presentation decisions: ordered horizontal bars preserve every Town Hall/league bucket; long location lists use a top group plus Others. CWL roster estimates use proportional integer places summing to the selected war size. Separate player-history charts avoid putting player and group counts on the same scale. League metadata now uses exact game IDs: CWL `48000022` is Legend League, and legacy player `29000022` is Legend League; neither is an unnamed new tier. Numeric API IDs remain unchanged.

`GET /v2/counts/clans/member-bins` adds a read-only histogram over existing `basic_clan.member_count`, with no schema change. The contract is `{totalClans, items:[{minMembers,maxMembers,count}]}`. Bins are 1–5 through 46–50, plus 0–0 when empty clans exist. Local HTTP validation returned 3,476,772 clans across 11 non-overlapping bins, including five empty clans; bin counts sum exactly to `totalClans`. The endpoint is included in the refreshed authoritative Expo contract package.

Implemented locally in Tracking, the authoritative schema repository, the API, and Expo. No production database changes, deployment, scheduler enablement for the new classifier, phone install, or visual tests were performed. GPT-6 Sol medium agents handled the classifier, CWL batch, and closeout/App work.

## 1. What changed

| Before | Now |
|---|---|
| The Armies screen used exact-composition analytics with fallback names. | The active Armies screen uses troop-group overview rows, then supported setup observations. |
| Separate spell/troop/setup lists could become a large catalog. | One evidence-filtered setup list per troop group; no fixed maximum. |
| A changing representative or generated name could be confused with identity. | Sorted core troop IDs identify the overview; sorted equipment/spell conditions identify its setup. |
| Detailed CWL summaries were expected in archive packs that did not contain them. | CWL participation reads a compact season/league/size aggregate; optional top-four same-TH hit rates use raw archived wars in the manual batch. |
| A failed Legend closeout returned before Ranked processing. | Ranked is attempted independently before combined errors are returned, with independent completion markers. |

Existing raw data, old exact-composition/family tables, and endpoints used elsewhere have not been deleted. This is not a compatibility layer for the new endpoint: the new Armies screen uses the new contract. Removing unrelated existing consumers would be separate cleanup.

## 2. Daily flow

```text
Completed Legend day (05:10 UTC to next 05:10 UTC)
       │
       ├─ attacks only: battles_ranked, battle_mode=2, direction=1
       └─ that day's leaderboard snapshot determines eligible population/cohorts
       │
Normalize codes; move siege to CC; preserve a displayable representative
       │
Group transient recipes using troop housing overlap
       │
Consolidate prototypes by core fighting troops
       │
Mine meaningful hero-equipment / spell setup associations inside each group
       │
Remove weak, near-duplicate, and largely redundant setup patterns
       │
Count actual outcomes for Overall / Top 1,000 / Top 200
       │
Verify each cohort count against existing legend_daily_stats
       │
Atomic daily transaction
       ├─ replace army_setup_daily_stats for that day
       └─ stamp classified count + completion in legend_daily_stats
       │
API: overview → setups → daily observations
       │
Expo: core troop imagery → setup imagery → representative LegendArmy
```

There is no new attack deduplication pass. The source uses attack rows once and joins the existing unique daily player snapshot. Missing/invalid/incomplete compositions stay in the cohort attack denominator but have no public group row.

A failure before transaction commit preserves the prior published result. Reads use one repeatable-read transaction so totals and observations come from one consistent database view. Rebuilding a day replaces that day's observations rather than appending another copy.

### Identity and history

An overview such as `[4000132]` is the sorted list of its core troop IDs. A setup key is the canonical JSON condition list, scoped to its overview key. Neither contains a representative code, generated name, row number, or model registry ID.

The same core and condition keys line up across days. Discovery uses that day's data rather than yesterday's assignments. A new competing pattern does not silently rewrite previously stored days. Explicitly replaying a historical day can change its result, because replay is an intentional recalculation.

A representative is a real example, not a claim that every matching attack used its exact quantities. Setup patterns can overlap. Their attack counts must not be added together to calculate their parent's overview.

The UI returns no generated family names. It uses actual troop, hero, equipment and spell imagery/names from the existing catalog and the existing LegendArmy composition renderer.

## 3. Selection rules and decisions made

No maximum number of setups is imposed. Instead, the implemented starting rules are:

| Rule | Starting value | Purpose |
|---|---:|---|
| Troop housing overlap | 80% after support weighting | Find similar main armies. |
| Core fighting troop selection | At least 25% average housing; fallback largest combat troop | Produce compact overview identities. |
| Assigned army's contribution for each core troop | At least 10% housing | Prevent an incidental troop from defining its overview. |
| Complete main army | Within ±10% of the daily attack-weighted median housing | Learn the baseline from observed armies; exclude outliers from classification, not totals. |
| Setup attack support | At least 50 attacks AND 4% of its troop group | Avoid tiny daily patterns. |
| Distinct semantic recipes | At least 5 | A single exact composition cannot manufacture a setup. |
| Association / enrichment | Phi ≥ 0.22 and lift ≥ 1.3 | Require co-occurrence beyond an incidental combination. |
| Near-duplicate occurrence sets | Weighted Jaccard ≥ 0.85 | Collapse essentially equivalent patterns. |
| Reusing already-selected hero equipment | At least 40% novel matched attacks | Suppress many correlated spell combinations around the same gear. |

Warden mode, pets, siege and CC contents do not create troop identity. Warden mode and pets are retained in representative codes. Hero equipment is associated with its hero, not treated as anonymous equipment.

Current mining considers equipment–spell and same-hero equipment pairs. It does not discover arbitrary long multi-hero/spell packages or pure spell-only setups. This is a deliberate bounded first implementation, not a claim that every tactical setup has been recognized.

These are support/association heuristics, **not a formal statistical-significance test**. No p-value or multiple-testing correction is claimed. They need review against real examples before production use.

### Difference from the selected Army Lab experiment

This uses troop overlap, not BIRCH. It is not a byte-for-byte port of the Lab run: daily prototypes are deterministic and usage-sorted over all daily recipes, rather than drawn from the Lab's weighted 20,000-recipe sample. It uses raw housing with explicit support weighting rather than the Lab's preprocessed troop vectors/radius. The semantic recipe gate ignores Warden/pet/siege-only code changes. These decisions are documented in `internal/armyfamily/DAILY_CLASSIFIER.md`.

The novelty gate was tested because lift alone still produced up to 22 patterns in a large family. On two complete export days, a 20% novelty gate reduced the maximum to 12–13; 30% to 11–12; 40% to 10. The final database replay reaches at most 8–10 per group, without imposing that limit.

Setup support is discovered against the day's Overall population. Top 1,000 and Top 200 then count the same definitions, making comparisons meaningful. Those smaller-cohort rows are not independently declared statistically supported and may contain small counts.

## 4. Actual local replay results

Only September 15–20 had both local battle records and matching daily ranking snapshots. Older raw battles exist, but they do not establish a complete multiweek cohort replay.

| Day | Total attacks | Classified | Troop groups | Supported setups |
|---|---:|---:|---:|---:|
| 2026-09-15 | 72,296 | 72,193 | 29 | 48 |
| 2026-09-16 | 80,902 | 80,797 | 37 | 53 |
| 2026-09-17 | 80,738 | 80,598 | 40 | 57 |
| 2026-09-18 | 79,956 | 79,793 | 41 | 57 |
| 2026-09-19 | 78,283 | 78,147 | 37 | 58 |
| 2026-09-20 | 78,017 | 77,868 | 39 | 60 |

Overall: **470,192 attacks**, **469,396 classified**, **796 hidden from family rows but retained in totalAttacks**. There are **1,126 stored rows** across all three cohorts and six days, with 18 completion markers.

More than 99.9% of classified attack volume on successive days belongs to troop-group keys that already appeared the previous day. This measures key continuity, not expert-verified tactical correctness. Setup-key recurrence is lower because patterns can cross the support gates; that is why unsupported days are null observations rather than zeros.

## 5. API endpoints and real responses

All routes are public GET endpoints registered in the canonical contract and worker router. Generated OpenAPI validates locally. These examples come from the actual endpoint query functions against the local database, not invented fixtures; they are not proof that a running production or phone-facing HTTP server was updated.

Percentages are ratios in JSON: `0.40` means 40%. `averageDestruction` is already on the 0–100 scale.

### GET /v2/stats/army-setups

Parameters: `time[after]`, `time[before]` (inclusive dates, default 30 days, maximum 90), `leagueTierId` (currently only 105000036, Legend I), optional `rankLimit=1000|200` (omitted means Overall), `sort=usage|tripleRate`, `limit=1..200` (default100), and optional `groupKey`.

Without groupKey, items are independent troop-group overview rows. With groupKey, items are supported setups under that group. `totalAttacks` appears once at response level; every item's usage uses that same selected cohort/date denominator.

Example request:
`/v2/stats/army-setups?time[after]=2026-09-15&time[before]=2026-09-20`

Below is the response with its items array shortened to the first real item:
```json
{
  "firstDay": "2026-09-15",
  "lastDay": "2026-09-20",
  "completedDays": [
    "2026-09-15",
    "2026-09-16",
    "2026-09-17",
    "2026-09-18",
    "2026-09-19",
    "2026-09-20"
  ],
  "totalAttacks": 470192,
  "classifiedAttacks": 469396,
  "leagueTierId": 105000036,
  "rankLimit": null,
  "items": [
    {
      "groupKey": "[4000132]",
      "variantKey": "",
      "coreTroops": [
        4000132
      ],
      "conditions": [],
      "shareCode": "h0p9e14_32-1p3e39_48-2p16e5_24-7p4e52_60i1x5-2x10-2x57-2x58-1x135d1x5-1x9-2x109u2x5-5x7-1x23-3x28-3x109-1x123-9x132s1x70-3x98-3x120",
      "attacks": 188529,
      "starCounts": {
        "zero": 205,
        "one": 24664,
        "two": 63523,
        "three": 100137
      },
      "usageRate": 0.40096173478068536,
      "threeStarRate": 0.5311490539916935,
      "averageDestruction": 92.95512096282269,
      "observedDays": 6
    }
  ]
}
```

### GET /v2/stats/army-setups?groupKey=...

Example request: same dates plus URL-encoded `groupKey=[4000132]`.

One real setup item:
```json
{
  "firstDay": "2026-09-15",
  "lastDay": "2026-09-20",
  "completedDays": [
    "2026-09-15",
    "2026-09-16",
    "2026-09-17",
    "2026-09-18",
    "2026-09-19",
    "2026-09-20"
  ],
  "totalAttacks": 470192,
  "classifiedAttacks": 469396,
  "leagueTierId": 105000036,
  "rankLimit": null,
  "items": [
    {
      "groupKey": "[4000132]",
      "variantKey": "[{\"kind\":\"equipment\",\"heroId\":28000000,\"id\":90000010,\"minimum\":1},{\"kind\":\"equipment\",\"heroId\":28000000,\"id\":90000051,\"minimum\":1}]",
      "coreTroops": [
        4000132
      ],
      "conditions": [
        {
          "kind": "equipment",
          "id": 90000010,
          "heroId": 28000000,
          "minimum": 1
        },
        {
          "kind": "equipment",
          "id": 90000051,
          "heroId": 28000000,
          "minimum": 1
        }
      ],
      "shareCode": "h0p11e10_51-1p9e39_48-2p3e5_24-7p4e52_60i1x0-3x5-1x57-1x58-1x123-1x135d1x5-1x70-1x120u2x5-5x7-4x26-3x28-1x82-3x109-9x132s1x9-3x98-2x109-2x120",
      "attacks": 26094,
      "starCounts": {
        "zero": 28,
        "one": 3085,
        "two": 8749,
        "three": 14232
      },
      "usageRate": 0.05549647803450505,
      "threeStarRate": 0.5454127385605887,
      "averageDestruction": 93.07315858051659,
      "observedDays": 6
    }
  ]
}
```

A variant's usage is its share of **all selected cohort attacks**, not its percentage within its parent. If we later want within-parent usage, it needs a separately labeled value.

### GET /v2/stats/army-setups/timeline

Requires `groupKey`; optional `variantKey` selects a setup rather than the overview. The date window permits up to365 days. It returns one entry per completed day. A missing supported setup is `observation:null`, not a zero-attack record. Days whose aggregate run never completed are excluded and identifiable through completedDays.

Response shortened to the first day:
```json
{
  "firstDay": "2026-09-15",
  "lastDay": "2026-09-20",
  "completedDays": [
    "2026-09-15",
    "2026-09-16",
    "2026-09-17",
    "2026-09-18",
    "2026-09-19",
    "2026-09-20"
  ],
  "totalAttacks": 470192,
  "classifiedAttacks": 469396,
  "leagueTierId": 105000036,
  "rankLimit": null,
  "items": [
    {
      "day": "2026-09-15",
      "totalAttacks": 72296,
      "observation": {
        "groupKey": "[4000132]",
        "variantKey": "",
        "coreTroops": [
          4000132
        ],
        "conditions": [],
        "shareCode": "h0p9e14_32-1p3e39_48-2p16e5_24-7p4e52_60i1x5-2x10-2x57-2x58-1x135d1x5-1x9-2x109u2x5-5x7-1x23-3x28-3x109-1x123-9x132s1x70-3x98-3x120",
        "attacks": 32378,
        "starCounts": {
          "zero": 37,
          "one": 4279,
          "two": 11001,
          "three": 17061
        },
        "usageRate": 0.4478532698904504,
        "threeStarRate": 0.5269318673173142,
        "averageDestruction": 92.88427327197479,
        "observedDays": 1
      }
    }
  ]
}
```

Keys must be URL-encoded when placed in a URL. The list route rejects variantKey; use the timeline route for a particular setup's daily history. Unknown parameters and unsupported league/rank values return400.

### GET /v2/stats/cwl

Optional `season=YYYY-MM`; omission selects the newest stored season. It no longer accepts the old attack-analytics date/TH filter contract.

Response shortened to one real bucket:
```json
{
  "season": "2026-09",
  "clanCount": 364,
  "registeredPlayerCount": 9701,
  "groupCount": 46,
  "items": [
    {
      "leagueId": 48000022,
      "warSize": 15,
      "clanCount": 12,
      "registeredPlayerCount": 281,
      "groupCount": 2,
      "townHallDistribution": [
        {
          "count": 234,
          "level": 18
        },
        {
          "count": 5,
          "level": 17
        },
        {
          "count": 2,
          "level": 16
        },
        {
          "count": 3,
          "level": 15
        },
        {
          "count": 4,
          "level": 14
        },
        {
          "count": 3,
          "level": 13
        },
        {
          "count": 3,
          "level": 12
        },
        {
          "count": 4,
          "level": 11
        },
        {
          "count": 1,
          "level": 9
        },
        {
          "count": 2,
          "level": 8
        },
        {
          "count": 5,
          "level": 7
        },
        {
          "count": 6,
          "level": 6
        },
        {
          "count": 3,
          "level": 5
        },
        {
          "count": 1,
          "level": 4
        },
        {
          "count": 3,
          "level": 3
        },
        {
          "count": 2,
          "level": 2
        }
      ],
      "sameTownHallHitRates": null,
      "finalizedWars": 10,
      "archivedWars": 0,
      "calculatedAt": "2026-09-23T00:35:46.212Z"
    }
  ]
}
```

The full captured responses are in `local-analytics-endpoint-examples.json` next to this report.

## 6. Storage — complete new table definitions

### army_setup_daily_stats and the existing denominator extension

This is one daily table for both overview and setup observations. Empty variant_key means overview; nonempty means a supported setup. No model registry, segment registry, stored exact-code assignment catalog, or exhaustive daily composition table was introduced.

```sql
-- +goose Up
-- Daily troop-overlap observations. Identity comes from core troops and setup
-- conditions, never a display name, row order or changing representative.
CREATE TABLE public.army_setup_daily_stats (
    day date NOT NULL,
    league_tier_id integer NOT NULL CHECK (league_tier_id > 0),
    rank_limit integer CHECK (rank_limit IN (200,1000)),
    group_key text NOT NULL CHECK (length(group_key) BETWEEN 1 AND 256),
    variant_key text NOT NULL DEFAULT '' CHECK (length(variant_key) <= 1024),
    core_troops integer[] NOT NULL CHECK (cardinality(core_troops) > 0),
    conditions jsonb NOT NULL DEFAULT '[]' CHECK (jsonb_typeof(conditions) = 'array'),
    representative_share_code text NOT NULL CHECK (length(representative_share_code) > 0),
    attack_count bigint NOT NULL CHECK (attack_count > 0),
    zero_star_count bigint NOT NULL CHECK (zero_star_count >= 0),
    one_star_count bigint NOT NULL CHECK (one_star_count >= 0),
    two_star_count bigint NOT NULL CHECK (two_star_count >= 0),
    three_star_count bigint NOT NULL CHECK (three_star_count >= 0),
    destruction_percentage_sum bigint NOT NULL CHECK (destruction_percentage_sum >= 0),
    evidence jsonb NOT NULL DEFAULT '{}' CHECK (jsonb_typeof(evidence) = 'object'),
    calculated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (zero_star_count + one_star_count + two_star_count + three_star_count = attack_count),
    CHECK (destruction_percentage_sum <= attack_count * 100),
    CHECK ((variant_key = '' AND conditions = '[]'::jsonb) OR (variant_key <> '' AND jsonb_array_length(conditions) > 0)),
    UNIQUE NULLS NOT DISTINCT (day,league_tier_id,rank_limit,group_key,variant_key)
);
CREATE INDEX army_setup_daily_identity ON public.army_setup_daily_stats(group_key,variant_key,day);
ALTER TABLE public.legend_daily_stats
    ADD COLUMN classified_army_attacks bigint,
    ADD CONSTRAINT legend_daily_classified_attacks_check CHECK (
        classified_army_attacks BETWEEN 0 AND attack_count
    );
COMMENT ON COLUMN public.army_setup_daily_stats.variant_key IS
    'Empty means independent troop-group overview. Setup rows may overlap and must not be summed to produce overview counts.';


```

Real overview and setup rows from September20:
```json
[
  {
    "day": "2026-09-20",
    "league_tier_id": 105000036,
    "rank_limit": null,
    "group_key": "[4000132]",
    "variant_key": "",
    "core_troops": [
      4000132
    ],
    "conditions": [],
    "representative_share_code": "h0p9e14_32-1p3e39_48-2p16e5_24-7p4e52_60i1x5-2x10-2x57-2x58-1x135d1x5-1x9-2x109u2x5-5x7-1x23-3x28-3x109-1x123-9x132s1x70-3x98-3x120",
    "attack_count": 28600,
    "zero_star_count": 23,
    "one_star_count": 3502,
    "two_star_count": 9668,
    "three_star_count": 15407,
    "destruction_percentage_sum": 2664061,
    "evidence": {
      "recipeCount": 10109
    },
    "calculated_at": "2026-09-23T00:43:57.152906+00:00"
  },
  {
    "day": "2026-09-20",
    "league_tier_id": 105000036,
    "rank_limit": null,
    "group_key": "[4000132]",
    "variant_key": "[{\"kind\":\"equipment\",\"heroId\":28000000,\"id\":90000010,\"minimum\":1},{\"kind\":\"equipment\",\"heroId\":28000000,\"id\":90000051,\"minimum\":1}]",
    "core_troops": [
      4000132
    ],
    "conditions": [
      {
        "id": 90000010,
        "kind": "equipment",
        "heroId": 28000000,
        "minimum": 1
      },
      {
        "id": 90000051,
        "kind": "equipment",
        "heroId": 28000000,
        "minimum": 1
      }
    ],
    "representative_share_code": "h0p11e10_51-1p9e39_48-2p3e5_24-7p4e52_60i1x0-3x5-1x57-1x58-1x123-1x135d1x5-1x70-1x120u2x5-5x7-4x26-3x28-1x82-3x109-9x132s1x9-3x98-2x109-2x120",
    "attack_count": 4291,
    "zero_star_count": 5,
    "one_star_count": 473,
    "two_star_count": 1458,
    "three_star_count": 2355,
    "destruction_percentage_sum": 400498,
    "evidence": {
      "lift": 6.0863316345659815,
      "share": 0.15003496503496502,
      "attacks": 4291,
      "novelShare": 1,
      "association": 0.9474465525492975,
      "recipeCount": 1804,
      "novelAttacks": 4291
    },
    "calculated_at": "2026-09-23T00:43:57.152906+00:00"
  }
]
```

The setup evidence stores attacks, share, recipeCount, lift, association, novelAttacks and novelShare. Evidence is calculated from the Overall discovery population; the row's outcome counts belong to its own rank cohort.

### Existing legend_daily_stats

Its existing primary key is `(day,cohort)`. It already stores:
- day date; cohort text;
- attack_count, distinct_player_count, zero_star_count, one_star_count, two_star_count, three_star_count, destruction_percentage_sum, duration_seconds_sum: bigint NOT NULL;
- hero_stats, pet_stats, equipment_stats, pet_hero_assignments, troop_stats, spell_stats, siege_stats, equipment_pair_stats, pet_combo_stats: jsonb NOT NULL DEFAULT '[]'.

Migration035 removes the analysis timestamp introduced by migration034. Nullable `classified_army_attacks bigint` is sufficient: null means no completed classification, while zero is a completed analysis with no classified attacks. It is written atomically with the observations and constrained to the source attack count. The existing cohort check also allows legacy top_100; this new feature only writes Overall/top_1000/top_200. Existing distinct-player/item fields remain for unrelated consumers; this new Army UI exposes attack usage only.

A real denominator/completion projection, with unchanged existing stats fields omitted:
```json
{
  "day": "2026-09-20",
  "cohort": "legend_i",
  "attack_count": 78017,
  "classified_army_attacks": 77868
}
```

### cwl_participation

One row per season/league/war size. Overall counts are derived by summing eligible buckets; there is no second season-total table.

```sql
-- +goose Up
-- Manually rebuilt CWL participation buckets; raw groups and wars remain canonical.
CREATE TABLE public.cwl_participation (
    season text NOT NULL,
    cwl_league_id integer NOT NULL,
    war_size smallint NOT NULL,
    group_count bigint NOT NULL,
    clan_count bigint NOT NULL,
    registered_player_count bigint NOT NULL,
    townhall_counts jsonb NOT NULL,
    same_th_hitrates jsonb,
    finalized_wars bigint NOT NULL,
    archived_wars bigint NOT NULL,
    refreshed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (season, cwl_league_id, war_size),
    CONSTRAINT cwl_participation_season_check CHECK (season ~ '^[0-9]{4}-(0[1-9]|1[0-2])$'),
    CONSTRAINT cwl_participation_league_check CHECK (cwl_league_id > 48000000),
    CONSTRAINT cwl_participation_size_check CHECK (war_size BETWEEN 1 AND 50),
    CONSTRAINT cwl_participation_counts_check CHECK (
        group_count >= 0 AND clan_count >= 0 AND registered_player_count >= 0
        AND finalized_wars >= 0 AND archived_wars >= 0 AND archived_wars <= finalized_wars
    ),
    CONSTRAINT cwl_participation_townhall_check CHECK (jsonb_typeof(townhall_counts) = 'array'),
    CONSTRAINT cwl_participation_hitrates_check CHECK (
        same_th_hitrates IS NULL OR jsonb_typeof(same_th_hitrates) = 'array'
    )
);


```

Real stored bucket:
```json
{
  "season": "2026-09",
  "war_size": 15,
  "clan_count": 12,
  "group_count": 2,
  "refreshed_at": "2026-09-23T00:35:46.212309+00:00",
  "archived_wars": 0,
  "cwl_league_id": 48000022,
  "finalized_wars": 10,
  "townhall_counts": [
    {
      "count": 234,
      "level": 18
    },
    {
      "count": 5,
      "level": 17
    },
    {
      "count": 2,
      "level": 16
    },
    {
      "count": 3,
      "level": 15
    },
    {
      "count": 4,
      "level": 14
    },
    {
      "count": 3,
      "level": 13
    },
    {
      "count": 3,
      "level": 12
    },
    {
      "count": 4,
      "level": 11
    },
    {
      "count": 1,
      "level": 9
    },
    {
      "count": 2,
      "level": 8
    },
    {
      "count": 5,
      "level": 7
    },
    {
      "count": 6,
      "level": 6
    },
    {
      "count": 3,
      "level": 5
    },
    {
      "count": 1,
      "level": 4
    },
    {
      "count": 3,
      "level": 3
    },
    {
      "count": 2,
      "level": 2
    }
  ],
  "same_th_hitrates": null,
  "registered_player_count": 281
}
```

Migrations033 and034 were applied with Goose only to the local development database and also exercised in a disposable schema-owned test database. Previously applied migrations were not rewritten.

## 7. CWL calculation behavior

The batch defaults to the most recent source season. It aggregates registered roster members, not players who made attacks. Groups missing a valid league or size are skipped. A preflight rejects a season if the same clan or player appears across multiple eligible groups, rather than silently double-counting root totals. September's local source has zero such conflicts.

It discovered a real source mismatch during implementation: stored round data is an array of war-tag arrays, and finalized wars can use either warended or warEnded. Both are handled.

September results: **46 groups, 364 clans, 9,701 registered players, 13 league/size buckets, 280 finalized wars**. These are counts in the imported local dataset, not a claim of global season coverage.

Only the current four highest league IDs (48000019–48000022) are eligible for optional hit-rate calculation. The same manual pass reads available archived war payloads and counts only equal attacker/defender TH. No finalized-war scheduler hook or CWL archive-summary format was added.

No local raw archive payloads were readable in this run, so sameTownHallHitRates is null. A rate calculated later from partial archive coverage describes only archivedWars out of finalizedWars; the API exposes both counts and must not be presented as full-season coverage.

## 8. App changes

The active Armies route now requests these overview/setup endpoints. It uses shared SelectionPicker, Surface, CKText, existing game assets, and LegendArmy rather than creating a separate composition renderer. It supports Overall/Top1,000/Top200, usage/three-star sorting, setup drilldown and readable daily rows. The existing one-hour analytics cache remains.

The CWL page shows the returned season, totals, league/size buckets, and TH distributions. It does not invent a past-month picker when those months have not been calculated. Missing hit rates display as unavailable.

The design skill guided component reuse and hierarchy. These screens were validated with semantic component tests and a build, not visual inspection. A dead legacy ArmiesSection remains in the shared Stats file to avoid deleting adjacent pre-existing dirty work; it is not the active route.

## 9. Running locally

From clashking_tracking:

```sh
TIMESCALE_URL='postgres://clashking_local:clashking_local@127.0.0.1:54330/clashking_dev?sslmode=disable' \
GOCACHE=/private/tmp/clashking_tracking-go-cache \
go run ./cmd/rebuild-army-setups \
  --days=2026-09-15,2026-09-16,2026-09-17,2026-09-18,2026-09-19,2026-09-20
```

The command refuses a nonlocal database and incomplete days. It also requires existing daily cohort totals/snapshots; it deliberately does not silently rebuild unrelated source data.

CWL:
```sh
TIMESCALE_URL='postgres://clashking_local:clashking_local@127.0.0.1:54330/clashking_dev?sslmode=disable' \
go run ./cmd/rebuild-cwl-participation --season=2026-09
```

Existing TIMESCALE_HOST-related environment variables take precedence in the CWL command. All resolved connection settings are still validated against the local-only target. Supplying a local --archive-origin enables optional top-four archive reads; omit it for participation only. The archive server must support the stored compressed byte ranges. Redirects to other hosts are rejected.

## 10. Validation and remaining decisions

Passed:
- Tracking classifier, writer, closeout-isolation and CWL batch tests.
- Disposable Goose migration / CWL database fixture checks, including duplicate membership and stored-round shapes.
- Local endpoint query proof against the six-day replay.
- API contracts/client/worker typecheck and focused analytics lint/tests.
- Generated OpenAPI:365 operations,127 schema references validated.
- Expo typecheck, all14 Stats suites /68 tests, localization check, and web export build.
- Local reconciliation of18 cohort totals and all1,126 stored observations.

The broader API suite has1,446 passing tests and11 failures in existing roster work: two outdated route-count assertions and nine missing static-metadata mocks. These were not changed to make the analytics suite appear green. Expo lint also reports the one unused legacy ArmiesSection warning. No visual tests were run.

Still deliberately not done:
1. No deployment or new daily-classifier scheduling. The manual implementation is ready for local review; enabling it in production needs a separate decision.
2. No fabricated CWL hit rates. Local raw archived wars are needed to calculate real top-four values.
3. No claim of a weeks-long replay: only six days currently have the required local snapshots.
4. No arbitrary exact quantity/exclusion filtering, player-usage calculation, generated family naming, or fixed setup cap.
5. No ranked historical data backfill or claim that production closeout recovered; this pass makes failure isolation testable locally.

Review questions that emerged:
- Are the 8–10 setups in the largest groups still too similar in practice? The algorithm has no count cap, so further reduction should come from stronger semantic/novelty evidence.
- Is the pair-based setup miner sufficient, or should a later experiment identify larger multi-hero/spell packages? That should be evaluated on data rather than promised as already supported.
- Should tiny Top200 setup counts be visually suppressed independently? Currently definitions come from Overall and counts are honestly returned for every nonzero cohort observation.
- We need more local daily ranking snapshots before judging multiweek continuity or family migration behavior confidently.
