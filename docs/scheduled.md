# Scheduled statistics (`scheduled`)

## What this process is for

`scheduled` runs data refreshes whose cadence is defined by the product calendar or a fixed interval. These are bulk/statistical jobs, not one future row per recipient.

## When it runs

The main scheduled cycle uses `scheduled.interval_seconds`. Current player/clan leaderboards run alongside it at the leaderboard interval. CWL season statistics refresh the current and previous UTC month immediately at startup and weekly afterward. League closeout processes the latest eligible shifted Legend day on startup, then runs daily at 05:12 UTC; Monday's closeout also discovers and finalizes completed Ranked seasons from the IDs returned by player profiles and matching league-history entries.

## Work owned here

- Current and historical player/clan leaderboard snapshots.
- Legend history completion/backfill for completed seasons.
- Ranked league member snapshots and season tier aggregates.
- Legend daily hit-rate, usage, and immutable army-family aggregates.
- Current and previous UTC CWL group, clan, registered-player, and Town Hall totals.
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

Writes typed leaderboard history/current tables, `legend_history`, current ranking tables, Ranked league members, `league_hitrate_stats`, `ranked_league_tier_stats`, `legend_daily_stats`, immutable army families and memberships, daily family outcomes, changed basic profile facts learned from rankings, and `cwl_season_statistics`. Legend and Ranked closeouts delete and rebuild only their completed day or season, and every battle aggregate reads `direction='attack'` so the stored defense perspective cannot double-count it.

Army-family matching compares each exact army directly with immutable anchors. Troop housing similarity must be at least 0.86, spell-capacity similarity at least 0.80, heroes must match exactly, and equipment similarity must be at least 0.75 with no more than two differing equipment IDs. A new family name comes from Cloudflare AI Gateway when configured; invalid, duplicate, or unavailable AI output uses a deterministic fallback and records truthful provenance.

The CWL refresh calls the schema-owned reconciliation procedure, whose shared advisory transaction lock atomically replaces each selected season from separate group, distinct-clan, registered-player, and Town Hall aggregates. Groups without both a league and war size are excluded, while partial eligible totals remain visible. `go run ./cmd/cwl-season-stats-reconcile --scope all` performs the explicit idempotent all-season repair through the same procedure.

## Events and Valkey

Scheduled statistics normally write SQL/cache snapshots and do not emit live Discord events. Leaderboard cache output is consumed by API reads. This process does not use the event stream as a job queue.

## Configuration

- `scheduled.requests_per_second`, shared by every Clash request in this process, including leaderboards
- `scheduled.interval_seconds`
- `leaderboards.interval_seconds`, `leaderboards.limit`, and `leaderboards.null_asset_url`
- SQL, proxy, and shared stats settings

## Outages and restarts

An outage pauses API work. Fixed calendar work can run at the next cycle; it is not shifted like a war. Replacement operations validate complete data so a partial restart cannot wipe a good snapshot.

## What it deliberately does not do

- No per-war clocks or reminder jobs.
- No live join/leave, attack, or player-upgrade events.
- No separate `leaderboards` deployment; leaderboards share this scheduled runtime.
