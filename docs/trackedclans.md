# Live clan and war tracking (`trackedclans`)

## What this process is for

`trackedclans` provides fast polling only where a configured Discord feature needs live clan or war behavior. It keeps live comparison snapshots and publishes requested events. It does not replace `globalclans` as the durable owner of the global clan/member tables.

## When it runs

It runs continuously as `trackedclans`. A clan loop and a current-war loop run independently at `trackedclans.requests_per_second`. The target registry is reloaded every `trackedclans.target_refresh_seconds`.

## How a clan becomes a target

The registry is built in one SQL query from active-server configuration:

- Join or leave logs enable clan polling and member events.
- War logs or war panels enable war polling and live war events.
- Discord war reminders enable war polling so a new war can be scheduled, but do not by themselves enable attack/state event publication.

Servers must have used ClashKing within 90 days, configuration must be enabled, and clan tags must be present. The result is kept in memory, so each API response does not perform an interest lookup.

## Clan decision flow

```text
Target has join/leave interest
  -> GET current clan
  -> compare with the prior live Valkey snapshot
  -> publish member_join and member_leave only for actual tag differences
  -> replace the live snapshot
```

These events are delivery signals. Durable `basic_clan` and `join_leave_history` writes remain the responsibility of `globalclans`.

## War decision flow

```text
Target has a war feature
  -> GET current war
  -> ignore CWL responses; the CWL process owns those
  -> compute key from sorted clan tags + preparation time
  -> upsert war_schedule and all participating player_timers
  -> publish war_schedule so reminders reconcile immediately
  -> if war log/panel interest exists, compare snapshot and emit requested changes
```

Pseudocode:

```text
war = GET /clans/{tag}/currentwar
if war is usable and not CWL:
  key = canonical_key(war.clan, war.opponent, war.preparation_start)
  transaction:
    upsert one war_schedule row
    upsert one timer per participating player
  publish war_schedule(key)
  if registry says publish_live_war_events:
    emit new_war, new_attacks, or war_state when comparison requires it
store current live snapshot
```

## Clash API used

- `GET /v1/clans/{clanTag}` for live member comparison.
- `GET /v1/clans/{clanTag}/currentwar` for regular/friendly war tracking.

The currently retained legacy helper code contains Capital/CWL functions, but the active `Run` path starts only clan and regular-war loops; `capital` and `cwl` are separate scripts.

## Data read and written

Reads `server_logs`, `reminders`, and server activity to build the registry. Live war discovery writes `war_schedule` and `player_timers`. Live comparison snapshots are held under `trackedclans.snapshot_prefix` in Valkey.

It does not write `basic_player`. It does not perform the global durable clan/member upsert.

## Events published

- `clan`: `member_join` and `member_leave` for configured consumers.
- `war_schedule`: a canonical war became available or was refreshed.
- `war`: live `new_war`, `new_attacks`, and `war_state` only when a war log/panel consumer exists.

## Interaction with other processes

```mermaid
flowchart LR
  Config[(logs, panels, reminders)] --> R[In-memory target registry]
  R --> T[trackedclans]
  T --> API[Clan and current-war endpoints]
  T --> S[(war_schedule)]
  T --> P[(player_timers)]
  T --> E[Valkey event stream]
  S --> W[war-discovery finalizer]
  E --> Rem[reminders / bot]
```

Both live and global discovery may see the same war. Their canonical key makes the SQL upsert idempotent, and the discovery queries exclude clans already represented by a schedule.

## Configuration

- `trackedclans.requests_per_second`
- `trackedclans.target_refresh_seconds`
- `trackedclans.snapshot_prefix`
- Event stream, SQL, Valkey, proxy, and `target_page_multiplier` settings

## Outages and restarts

Requests pause at the shared gate. The SQL schedule survives restarts. Valkey live snapshots may be absent after eviction; the first response then becomes a baseline rather than generating a false burst of joins, leaves, or attacks.

## What it deliberately does not do

- No global clan/member persistence.
- No join/leave event from the global crawler.
- No active CWL or Capital loop.
- No per-response SQL/Valkey interest lookup.
- No in-memory future reminder scheduler.
