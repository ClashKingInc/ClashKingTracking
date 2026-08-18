# Priority player tracking (`trackedplayers`)

## What this process is for

Priority player tracking keeps a smaller useful set of players fresh, detects meaningful changes, stores positive stat deltas, and records whether any activity was observed during a poll.

## When it runs

It runs continuously as `trackedplayers` at `trackedplayers.requests_per_second`. SQL targets are paged into one small in-memory cycle, active verified-app targets from Valkey are appended, and the complete cycle is fed through one bounded worker pool. This prevents one slow retry at a page boundary from idling the remaining request budget.

## How a player becomes a target

The target union is:

1. Town Hall 9 or higher members stored on server clans whose server used ClashKing within 90 days.
2. Verified app accounts in the `tracking:verified_players` sorted set whose seven-day expiry has not passed. These are accepted without a Town Hall filter because their current profile is not known until it is fetched.

The SQL source is deduplicated by its query. The verified-account pass is independent, so a verified account that is also in the SQL set can receive one extra poll per full cycle. That avoids a database membership check for every verified account. Bookmarked players are not targets merely because they are bookmarked.

## Decision flow

```text
Load target
  -> GET current player
  -> load previous compressed snapshot from Valkey
  -> first snapshot? establish baseline without inventing deltas
  -> compare current and previous
     -> update changed basic profile columns
     -> store supported profile history
     -> store positive stat deltas
     -> if any activity signal changed, write one online event
     -> if current clan changed, update/remove the player->clan hash entry
  -> replace snapshot only after SQL succeeds
```

Pseudocode:

```text
player = GET /players/{tag}
previous = Valkey GET player snapshot
if previous exists:
  changes = compare(previous, player)
  stats = positive_deltas(previous, player)
  activity_detected = any_activity_signal_changed(previous, player)
  transaction:
    upsert changed basic_player columns
    insert profile changes
    insert positive stat changes
    if activity_detected: insert exactly one player_online_events row
update verified player->clan mapping only when different
store new snapshot
```

## Activity and statistics

Activity is a yes/no result for the whole poll. Donations, attack wins, war stars, relevant achievements, equipment, name, or another configured activity signal may set it to true, but several changed signals still create one `player_online_events` row. No score is calculated or stored.

Positive deltas are stored for:

- `donated`
- `received`
- `clan_games`
- `capital_gold_donated`
- `season_pass` from the Well Seasoned achievement

Each row stores `event_time`, previous value, current value, and delta. Event time is the observation identity; there is no separate season key.

## Clash API used

- `GET /v1/players/{playerTag}`.

## Data read and written

Reads target tables, `basic_clan.members`, and the previous Valkey snapshot. Writes conditional `basic_player` fields, `player_change_history`, `player_stat_changes`, and `player_online_events`.

Valkey state:

- Player snapshot keys for comparisons have a 30-day TTL. Reading an active snapshot refreshes its TTL only after it falls below 23 days, in the same Valkey operation and without uploading the payload again.
- `tracking:tracked_player_snapshot_targets` remembers the last completed target union. After a complete pass, snapshots removed from the union are shortened to a one-day TTL. A target returning during that day is restored to 30 days on its next read.
- `tracking:verified_players`: seven-day verified-account target expiry.
- The verified player-to-current-clan hash used by Capital tracking and Raid reminders.

The clan mapping is written only when the clan changes. Leaving a clan removes the mapping.

## Events and interaction

Once per configured refresh interval, the process loads the enabled player-log types for recently active servers into a small in-memory clan registry. A supported profile change publishes only when the player's current clan has a matching log consumer. This avoids a SQL or Valkey interest lookup for each player response.

This registry never changes who is polled or what is stored. Every Town Hall 9+ configured-clan member and every active verified account still receives the same profile polling, `basic_player` updates, history, stat-delta, and activity handling; the registry only decides whether a live outbound event has somewhere to go.

The event contains the player identity, matched change/log types, and only the previous/current values for those changes. It does not carry two complete player profiles, publish because an unrelated broad field refreshed, or publish a counter reset.

```mermaid
flowchart LR
  A[API login] --> V[verified target cache]
  C[(active server clan members)] --> P
  V --> P
  P --> API[Clash player endpoint]
  P --> S[(player stats and history)]
  P --> H[player-to-clan hash]
  H --> R[capital tracking]
```

## Configuration

- `trackedplayers.requests_per_second`
- `trackedplayers.target_refresh_seconds` controls how often the in-memory player-event interest registry reloads.
- `target_page_multiplier`
- Valkey, Timescale/PostgreSQL, event stream, and proxy settings

## Outages and restarts

The availability gate pauses requests. The previous snapshot remains intact until SQL storage succeeds, preventing a failed write from becoming the new comparison baseline. Stat event times are reserved so retries do not create duplicate deltas.

## What it deliberately does not do

- It does not write a numeric activity score.
- It does not write `battlelogs_tracking_ttl`.
- It does not create war timers or write war data.
- It does not target bookmarked players or send mobile Legend notifications.
