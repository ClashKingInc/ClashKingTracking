# Scheduled statistics (`scheduled`)

## What this process is for

`scheduled` runs data refreshes whose cadence is defined by the product calendar or a fixed interval. These are bulk/statistical jobs, not one future row per recipient.

## When it runs

The main scheduled cycle uses `scheduled.interval_seconds`. The Legend player refresh runs alongside it. League closeout runs daily at 05:12 UTC, not on startup; Monday's closeout also discovers and finalizes completed Ranked seasons from player profiles and matching league-history entries, then records completion for the guarded weekly trophy reset.

## Work owned here

- Current and historical player/clan leaderboard snapshots.
- Forward-only Legend history completion for recent completed seasons. Already stored season IDs are skipped before parsing; routine discovery never backfills beyond a 35-day lookback or the newest stored completion, whichever is later. Older repairs require an explicit separate operation.
- Ranked league member snapshots and season tier aggregates.
- Legend daily hit-rate, usage, and immutable army-family aggregates.
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

Writes typed leaderboard history/current tables, `legend_history`, `legend_rankings_current`, `leaderboard_history_player_home`, Ranked league members, `league_hitrate_stats`, `ranked_league_tier_stats`, `legend_daily_stats`, immutable army families and memberships, daily family outcomes, and changed basic profile facts learned from rankings. Legend and Ranked closeouts delete and rebuild only their completed day or season, and every battle aggregate reads numeric attack direction `1` so the stored defense perspective cannot double-count it. Legend closeout writes the exact `legend_i`, `top_1000`, and `top_200` cohorts, with higher-ranked players intentionally represented in each applicable cohort.

Army-family matching compares each exact army directly with immutable anchors. Troop housing similarity must be at least 0.86, spell-capacity similarity at least 0.80, heroes must match exactly, and equipment similarity must be at least 0.75 with no more than two differing equipment IDs. New families retain an immutable representative share code and an optional name.

## Events and Valkey

Scheduled statistics write SQL snapshots and do not emit live Discord events. Player and clan leaderboards are not published to Valkey. This process does not use the event stream as a job queue.

Official player history retains valid ranking facts when optional clan metadata is incomplete, leaving all three clan snapshot fields absent rather than borrowing today's clan details. Upstream `previousRank=-1` means no previous rank and is stored as SQL NULL; other negative ranks remain invalid. Current clan ranking writes use only the existing five schema columns, without `updated_at`.

## Configuration

- `scheduled.requests_per_second`, shared by the ordinary scheduled workloads
- the Legend I live-player refresh has a dedicated fixed 20-request-per-second limiter
- `scheduled.interval_seconds`
- `leaderboards.interval_seconds`, `leaderboards.limit`, and `leaderboards.null_asset_url`
- SQL, proxy, and shared stats settings

## Outages and restarts

An outage pauses API work. Fixed calendar work can run at the next cycle; it is not shifted like a war. Replacement operations validate complete data so a partial restart cannot wipe a good snapshot.

## What it deliberately does not do

- No per-war clocks or reminder jobs.
- No live join/leave, attack, or player-upgrade events.
- No separate `leaderboards` deployment; leaderboards share this scheduled runtime.
