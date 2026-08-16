# Leaderboards (inside `scheduled`)

## What this workload is for

Leaderboards fetch current global/location rankings and maintain the typed current/history tables and lightweight API cache. It is documented separately because it makes many paging and replacement decisions even though it runs inside `scheduled`.

## When it runs

The current leaderboard loop runs every `leaderboards.interval_seconds` at `leaderboards.requests_per_second`. Historical/season jobs run when the scheduled calendar says a season can be completed.

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
  -> refresh API cache metadata
```

## Data written

- `player_rankings_current` and `clan_rankings_current`.
- Typed leaderboard history tables by game mode.
- `legend_history` for completed Legend seasons.
- `ranked_league_group_members`.
- Changed `basic_player` facts learned from ranked players.

Leaderboard writes to `basic_player` are accepted because volume is bounded and the shared SQL upsert writes only changed columns.

## Configuration

- `leaderboards.requests_per_second`
- `leaderboards.interval_seconds`
- `leaderboards.limit`
- `leaderboards.null_asset_url`
- SQL/proxy settings

## Failure behavior

A board is replaced only with a complete accepted result. API errors are recorded per request. Availability pauses the entire fetch path, and a later scheduled pass retries naturally.

## What it deliberately does not do

- It does not produce live events.
- It does not calculate player activity.
- It is not a separately selected script in `main.go`; `scheduled` starts it.
