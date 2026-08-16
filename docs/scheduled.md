# Scheduled statistics (`scheduled`)

## What this process is for

`scheduled` runs data refreshes whose cadence is defined by the product calendar or a fixed interval. These are bulk/statistical jobs, not one future row per recipient.

## When it runs

The main scheduled cycle uses `scheduled.interval_seconds`. Current player/clan leaderboards run alongside it at the leaderboard interval. Individual subjobs determine whether their calendar condition is currently due before calling Clash.

## Work owned here

- Current and historical player/clan leaderboard snapshots.
- Legend history completion/backfill for completed seasons.
- Ranked league group/member snapshots.
- Scheduled broad statistics and date-bound maintenance already implemented in `scripts/scheduled.go`.
- The leaderboards workload described separately in [leaderboards.md](leaderboards.md).

War finalization and reminders are excluded because their times vary per war and live in durable schedule tables.

## General decision flow

```text
Wake at configured interval
  -> determine which calendar jobs are due
  -> fetch all required pages with bounded rate/concurrency
  -> validate completeness before replacing a snapshot
  -> write/upsert final typed rows in one store operation
  -> record process/request/write statistics
```

Incomplete historical pulls do not replace a previously complete season. Current snapshot tables remove rows that disappeared from the fresh complete result.

## Clash API used

Depending on the due subjob: player profile, locations, player/clan ranking endpoints, historical Legend rankings through the proxy extension, and league-group data already represented by the code. Every request uses the shared availability gate.

## Data read and written

Writes typed leaderboard history/current tables, `legend_history`, current ranking tables, ranked league group members, and changed basic profile facts learned from rankings. Exact table ownership is described beside each subjob in the source and in [leaderboards.md](leaderboards.md).

## Events and Valkey

Scheduled statistics normally write SQL/cache snapshots and do not emit live Discord events. Leaderboard cache output is consumed by API reads. This process does not use the event stream as a job queue.

## Configuration

- `scheduled.interval_seconds`
- All `leaderboards.*` fields
- SQL, proxy, and shared stats settings

## Outages and restarts

An outage pauses API work. Fixed calendar work can run at the next cycle; it is not shifted like a war. Replacement operations validate complete data so a partial restart cannot wipe a good snapshot.

## What it deliberately does not do

- No per-war clocks or reminder jobs.
- No live join/leave, attack, or player-upgrade events.
- No separate `leaderboards` deployment; leaderboards share this scheduled runtime.
