# Global clan tracking (`globalclans`)

## What this process is for

Global clan tracking keeps the broad `basic_clan` catalogue current. It is the durable owner of clan profile data, the current member list, and membership history. It does not decide whether Discord should receive a join or leave message.

## When it runs

It runs continuously as the `globalclans` script. Two loops run at the same time:

- The priority loop handles clans considered active and uses `globalclans.priority_requests_per_second`.
- The non-priority loop handles the remaining clans and uses `globalclans.non_priority_requests_per_second`.

Each loop has its own cursor. A large inactive population therefore cannot stop active clans from being revisited.

## How a clan becomes a target

Targets come from `basic_clan`. The database query assigns each clan to exactly one pool using its stored activity state. Pages are ordered by clan tag and resume after the last tag read. `target_page_multiplier` controls how many seconds of work are loaded in one page.

## Decision flow

```text
Load next target page
  -> request the current clan
  -> request failed permanently? record the failure or remove an invalid clan
  -> response unchanged? only refresh tracking state that needs refreshing
  -> response changed? update the clan and compare member tags
  -> store profile changes, joins, leaves, records, and discovered players
```

Pseudocode:

```text
for each target pool:
  page = load_clans_after(cursor)
  for clan_tag in page, at the pool rate:
    clan = GET /clans/{clan_tag}
    previous = load basic_clan
    changes = compare(previous, clan)
    write current clan and durable history in one batch
  advance or wrap cursor
```

## Clash API used

- `GET /v1/clans/{clanTag}` through the configured ClashKing proxy.

No current-war, CWL, player-profile, battle-log, or Raid Weekend endpoint is called here.

## Data read and written

Reads:

- `basic_clan` for targets and the previous snapshot.
- Tracking cursor/state tables used by the paging framework.

Writes:

- `basic_clan`: current profile, public-war-log flag, members, activity fields, and known Clash IDs.
- `clan_records`: best clan points and war streak records.
- `basic_player`: inexpensive member facts learned from the clan response.
- `join_leave_history`: durable join and leave rows.
- `clan_change_history`: durable description, level, league, and other supported profile changes.

Only columns whose incoming values are different are updated. This reduces database writes when a clan response is unchanged.

## Valkey and events

Global clan tracking does not publish join or leave events. That is intentional: it tracks millions of clans, while only a small configured subset has a live consumer. `trackedclans` owns live event emission.

It uses the shared availability gate but has no clan-specific Valkey cache.

## Interaction with other processes

```mermaid
flowchart LR
  SQL[(basic_clan)] --> G[globalclans]
  G --> API[Clash clan endpoint]
  API --> G
  G --> SQL
  G --> H[(join_leave_history)]
  G --> P[(basic_player)]
  SQL --> T[trackedplayers and war discovery]
```

`trackedplayers` can use the stored member lists of recently active server clans. War discovery uses `public_war_log` and `last_war_at`. Live clan tracking may observe the same clan more often, but it does not replace this process as the durable global owner.

## Configuration

- `globalclans.priority_requests_per_second`
- `globalclans.non_priority_requests_per_second`
- `target_page_multiplier`
- Timescale/PostgreSQL connection settings
- Proxy origin and credentials shared by all Clash callers

## Outages and restarts

Every request waits at the shared availability gate. A proxy outage pauses without changing game time. Official Clash maintenance also pauses; this process needs no clock shifting because clan snapshots have no scheduled end time. Database cursors let a restart resume broad paging without an expensive full count.

## What it deliberately does not do

- It never publishes join/leave messages.
- It does not poll wars, CWL, Raid Weekend, battle logs, or full player profiles.
- It does not own notification configuration.
- It does not infer live event interest per response.
