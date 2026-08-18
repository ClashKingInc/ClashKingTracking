# Global war discovery (`war-discovery`)

## What this process is for

Global war discovery scans public war logs across the game, records an active war once, and performs the durable final fetch when it ends. It works independently of Discord live tracking.

## When it runs

Two continuous discovery loops and one finalizer run inside `war-discovery`:

- Active clans: a war was found within 30 days, using `wars.requests_per_second`.
- Dormant clans: no known war within 30 days, using `wars.dormant_requests_per_second`.
- Due schedules: every 15 seconds, load wars whose `next_run_at` has arrived.

A periodic cleanup removes expired `player_timers`.

## How a clan becomes a target

Both pools require `basic_clan.public_war_log = true`. They are separated by `basic_clan.last_war_at`. A clan is excluded while either side of one of its wars is already in `war_schedule`; this avoids repeatedly polling both perspectives of an active war.

Finding a war updates `last_war_at`, which naturally promotes a dormant clan into the active pool.

## Discovery decision flow

```text
Load next active or dormant clan page
  -> GET current war
  -> no war/private/not found? skip normally
  -> usable active war? compute canonical identity
  -> upsert war_schedule and player_timers
  -> publish war_schedule for reminder reconciliation
```

The canonical schedule key is a hash of the two alphabetically sorted clan tags and the original preparation start time. The viewpoint used to discover the war cannot change its identity.

## Final-war decision flow

```text
war_schedule.next_run_at is due
  -> fetch exact CWL war tag, or try both regular-war clan perspectives
  -> require canonical tags + preparation time to match this schedule
  -> API still says active? move next_run_at one minute forward
  -> ended? store permanent war, members, attacks, and missed attacks
  -> delete completed schedule and its reminder jobs
  -> still unavailable six hours after shifted end? remove the dead schedule
```

Pseudocode:

```text
for schedule in due_schedules:
  final = fetch_exact_war_from_either_side(schedule)
  if final is not ended:
    if now < shifted_end + 6 hours:
      reschedule(now + 1 minute)
    else:
      remove dead schedule and its war timers
  else:
    transaction:
      insert canonical war and attack data
      remove schedule
```

## Clash API used

- `GET /v1/clans/{clanTag}/currentwar` for regular/friendly discovery and completion.
- `GET /v1/clanwarleagues/wars/{warTag}` for a scheduled CWL final fetch.

## Data read and written

Reads `basic_clan`, `war_schedule`, and due player timers. Writes:

- `war_schedule`: temporary durable active-war clock and opponent mapping.
- `player_timers`: one `(player, war, schedule key)` participation row.
- Permanent war index, member, attack, and missed-attack tables after completion.
- `basic_clan.last_war_at` when a war is observed.

It never writes `basic_player`.

## Events and interaction

It publishes `war_schedule` after the schedule transaction commits. `reminders` uses this to create required clock rows. `trackedclans` can upsert the same schedule earlier for configured clans. The permanent finalizer is shared by any canonical schedule regardless of which process first found it.

```mermaid
flowchart LR
  C[(basic_clan)] --> A[Active 30-day pool]
  C --> D[Dormant pool]
  A --> API[Current-war endpoint]
  D --> API
  API --> S[(war_schedule)]
  S --> R[reminders]
  S --> F[durable finalizer]
  F --> W[(permanent war tables)]
```

## Configuration

- `wars.requests_per_second` (default supplied: 500)
- `wars.dormant_requests_per_second` (default supplied: 50)
- `wars.cwl_sync_seconds` is consumed by the separate CWL mode
- `target_page_multiplier`, SQL, event stream, and proxy settings

## Outages and restarts

Discovery waits at the availability gate. `war_schedule` is PostgreSQL-backed, so active clocks survive restarts without rebuilding an in-memory job list. During official Clash maintenance, `end_time`, `next_run_at`, related player expiry, and reminder run times are shifted together. A proxy-only outage pauses but does not shift game time.

At finalization, both clan perspectives are tried because one public endpoint may already say `notInWar` while its opponent still exposes the exact ended war. The canonical identity check prevents a newer war between the same clans from being stored under the old schedule. If neither perspective can expose the exact war, the finalizer retries for six hours and then removes the unrecoverable schedule instead of leaving a dead row forever.

## What it deliberately does not do

- No live Discord attack/state event production.
- No clan member or player profile updates.
- No duplicate schedule for the opposing viewpoint.
- No in-memory final-war job registry.
