# Basic player tracking (`basicplayers`)

## What this process is for

Basic player tracking is the slow, broad refresh for known player profiles. It keeps searchable profile columns useful without paying the cost of detailed change history and notification work for every player.

## When it runs

It runs continuously as `basicplayers` at `basicplayers.requests_per_second`. Work is paged in tag order and wraps after reaching the end.

## How a player becomes a target

Every eligible row already present in `basic_player` can be selected. This process does not create a separate “tracked” membership and does not use war participation or a battle-log TTL.

## Decision flow

```text
Load a page of basic_player tags
  -> GET current player
  -> turn the response into basic profile columns
  -> SQL updates only if at least one column differs
  -> continue from the page cursor
```

Pseudocode:

```text
page = load_basic_player_tags_after(cursor)
for tag in page, with bounded concurrency:
  player = GET /players/{tag}
  write tag, name, league, clan, town hall, and trophies if changed
advance or wrap cursor
```

## Clash API used

- `GET /v1/players/{playerTag}`.

## Data read and written

Reads `basic_player` for paging. It writes only current `basic_player` profile columns through the shared changed-row upsert. It does not write `player_change_history`, `player_stat_changes`, `player_online_events`, `player_timers`, or battle logs.

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
- No battle-log, war, clan, Raid Weekend, or notification work.
