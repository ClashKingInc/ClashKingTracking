# Basic player tracking (`basicplayers`)

## What this process is for

Basic player tracking is the slow, broad refresh for known player profiles. It keeps searchable profile columns useful without paying the cost of detailed change history and notification work for every player.

## When it runs

It runs continuously as `basicplayers` at `basicplayers.requests_per_second`. Work is read from SQL in tag order and wraps after reaching the end. Requests continue flowing while individual slow profiles retry; a profile that still times out, returns a truncated response, or is rate-limited after bounded retries remains in SQL and is tried on the next complete pass.

## How a player becomes a target

Every eligible row already present in `basic_player` can be selected. This process does not create a separate “tracked” membership and does not use war participation or a battle-log TTL.

## Decision flow

```text
Load a page of basic_player tags
  -> GET current player
  -> turn the response into basic profile columns
  -> update basic profile columns only if at least one differs
  -> replace the player's compact profile-detail JSON
  -> continue from the page cursor
```

Pseudocode:

```text
page = load_basic_player_tags_after(cursor)
for tag in page, with bounded concurrency:
  player = GET /players/{tag}
  write tag, name, league, clan, town hall, and trophies if changed
  write heroes, hero equipment, and achievements as three JSON arrays
advance or wrap cursor
```

## Clash API used

- `GET /v1/players/{playerTag}`.

## Data read and written

Reads `basic_player` for paging. It writes current `basic_player` profile columns through the shared changed-row upsert and one `player_profile_details` row for every successfully fetched full profile. It does not write `player_change_history`, `player_stat_changes`, `player_online_events`, `player_timers`, or battle logs.

`player_profile_details` deliberately has one row per player rather than one row per hero, item, or achievement:

```text
player_tag | townhall_level | heroes JSONB | equipment JSONB | achievements JSONB | observed_at
```

The arrays contain only fields useful for progress statistics:

- Heroes: `name`, `level`, `max_level`, and `village`.
- Equipment: `name`, `level`, `max_level`, `village`, and `rarity` when known.
- Achievements: `name`, `stars`, `value`, `target`, and `village`.

Troops, spells, achievement descriptions, and completion text are not stored. `observed_at` records when that complete profile was last seen.

## Global progress statistics

The detail table itself defines the sample. A player present only in `basic_player` is not included because their detailed progress is unknown. For example, a weekly equipment rollup can expand `equipment`, group it by `townhall_level` and equipment name, and divide the number of matching equipment entries by the number of detail rows at that Town Hall to calculate an unlock rate.

These are occasional full-table batch scans, so the raw table has no JSON GIN indexes. Its primary key supports player replacement/deletion, and a small Town Hall index supports sample counts and targeted inspection. The eventual weekly result should be stored as a compact rollup for API/dashboard reads instead of rerunning JSON expansion for every request.

An equipment-level and unlock-rate rollup has this shape:

```sql
WITH samples AS (
    SELECT townhall_level, count(*) AS players
    FROM player_profile_details
    GROUP BY townhall_level
), equipment_progress AS (
    SELECT
        profile.townhall_level,
        item.name,
        item.rarity,
        item.level,
        item.max_level
    FROM player_profile_details profile
    CROSS JOIN LATERAL jsonb_to_recordset(profile.equipment) AS item(
        name text,
        level integer,
        max_level integer,
        village text,
        rarity text
    )
)
SELECT
    progress.townhall_level,
    progress.name,
    progress.rarity,
    samples.players AS sampled_players,
    count(*) AS unlocked_players,
    round(count(*)::numeric / samples.players, 4) AS unlock_rate,
    round(avg(progress.level), 2) AS average_level,
    count(*) FILTER (WHERE progress.level >= progress.max_level) AS maxed_players
FROM equipment_progress progress
JOIN samples USING (townhall_level)
GROUP BY progress.townhall_level, progress.name, progress.rarity, samples.players;
```

Heroes use the same expansion over `heroes`. Achievements can group `achievements` by Town Hall and name to calculate average stars/value or completion rates using `stars` and `target`. These queries count only rows in `player_profile_details`, so an unpolled `basic_player` never lowers an unlock or completion rate.

## Valkey and events

There is no per-player snapshot cache and no event publication. Broad refreshes are useful for search and display, not a sufficient reason to send an upgrade or activity event.

## Interaction with other processes

`trackedplayers` may update the same basic profile columns more frequently for its smaller target set. Both use the same conditional SQL upsert, so an older unchanged response does not generate a write. War processes never write `basic_player`.

## Configuration

- `basicplayers.requests_per_second`
- `target_page_multiplier`
- Timescale/PostgreSQL and proxy settings

## Outages and restarts

Requests pause at the shared availability gate. The database paging cursor allows normal continuation after restart. A failed individual target is handled by the shared Clash retry/error rules.

## What it deliberately does not do

- No activity score or online event.
- No stat deltas or upgrade events.
- No troop or spell storage.
- No battle-log, war, clan, Raid Weekend, or notification work.
