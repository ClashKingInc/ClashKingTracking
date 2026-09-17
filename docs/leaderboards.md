# Leaderboards (inside `scheduled`)

## What this workload is for

Scheduled ranking jobs maintain official current/history tables. The adjacent leaderboard loop refreshes Legend I players and publishes SQL materialized snapshots for player Town Hall and league boards. No player or clan leaderboard is published to Valkey.

## When it runs

The live Legend I loop runs every `leaderboards.interval_seconds` with its own fixed 20-request-per-second limiter. A separate maintenance loop checks every minute and refreshes player Town Hall/league materialized views once six hours have elapsed since the last successful refresh. It populates uninitialized views on its first run. Ordinary official ranking fetches still share `scheduled.requests_per_second`.

## Targets and endpoints

Locations come from the Clash locations endpoint. For each supported location and ranking type, the process calls the appropriate player or clan ranking endpoint, following paging tokens until the configured limit or endpoint completion.

Supported groups include Home Village player/clan, Builder Base player/clan, Clan Capital clan rankings, Legend season history, and ranked league group members where implemented.

## Decision flow

```text
Load locations
  -> request each supported board with bounded concurrency
  -> normalize league/location/clan/player fields
  -> validate page/result completeness
  -> upsert current rows and delete stale rows for that exact board
  -> append/replace completed historical snapshot
  -> fetch every stored Legend I player at 20 requests per second
  -> atomically replace legend_rankings_current
  -> publish SQL-backed live Legend leaderboard

basic_player
  -> every six hours: refresh player_townhall_leaderboards / player_league_leaderboards
  -> API reads up to 500 rows per board, without rebuilding ranks per request
```

## Data written

- `player_rankings_current` and `clan_rankings_current`.
- Typed leaderboard history tables by game mode.
- `legend_history` for completed Legend seasons.
- `legend_rankings_current`, atomically replaced only after the full Legend I refresh.
- `ranked_league_group_members`.
- Changed `basic_player` facts learned from ranked players.
- `player_townhall_leaderboards`: ranked tiers only, ordered by league descending, trophies descending, then tag.
- `player_league_leaderboards`: exact ranked tier, ordered by trophies descending, then tag.
- `tracking_scheduled_jobs`: durable refresh completion and weekly reset progress.

The SQL snapshots include ranked players with zero trophies. Unranked and unknown leagues are excluded. Snapshots use existing `basic_player` observations; there is no separate Town Hall/league candidate-fetch pass. Clan display metadata is still joined from `basic_clan` by the API.

Leaderboard writes to `basic_player` are accepted because volume is bounded and the shared SQL upsert writes only changed columns.

## Configuration

- `scheduled.requests_per_second`
- fixed 20 requests per second for the live Legend I player refresh
- `leaderboards.interval_seconds`
- `leaderboards.limit`
- `leaderboards.null_asset_url`
- SQL/proxy settings

## Failure behavior

A board is replaced only with a complete accepted result. API errors are recorded per request. Availability pauses the entire fetch path, and a later scheduled pass retries naturally.

Recognized "rankings not found for location" errors on non-global boards are counted and skipped without replacing existing data. Global 404s and unrelated errors remain failures. Maintenance errors have their own `leaderboards.maintenance` readiness entry and do not terminate the Legend refresh loop.

## Weekly Ranked trophy reset

On Monday between 16:30 and 17:00 UTC only, maintenance resets nonzero `basic_player.trophies` for league IDs 105000000 through 105000035, including Unranked. Legend I (105000036) and unknown leagues are untouched. Monday's Ranked closeout must have recorded successful completion first.

Each transaction changes at most 5,000 players and saves its last tag. A process restart resumes that cursor inside the same safe window. Completed jobs never repeat for that Monday; a missed or partial job is not caught up outside the window. Completion forces both player snapshots to refresh, regardless of the six-hour timer. No historical trophy records are changed, no Legend season reset is performed, and normal profile writers remain free to record later observations.

The maintenance session holds a PostgreSQL advisory lock to prevent overlapping scheduled processes from running it simultaneously. There is deliberately no cross-writer stale-response coordination.

## Rollout order

Apply DevKit migration 020, deploy this Tracking change, verify both player views are populated, then deploy the API snapshot reader. The views are created WITH NO DATA so migration itself does not perform the population query. Deploying the API before the first refresh will fail those board reads. No production migration or deployment is performed by this PR.

## What it deliberately does not do

- It does not produce live events.
- It does not calculate player activity.
- It is not a separately selected script in `main.go`; `scheduled` starts it.
