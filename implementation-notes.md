# Implementation Notes

## Choices

- Shared basic-player SQL upserts moved from `scripts` to `internal/utils`.
- `bot_players.go` no longer uses `CycleRunner`; it owns Run/do/store directly.
- `giveaways.go`, `reddit.go`, and `scheduled.go` now use SQL-only stores.
- `events.go` now streams Valkey Stream entries over the existing gRPC service.
- Published events now go only to Valkey Streams, not the old in-memory bus.
- Valkey Stream retention is time-based and configured with
  `events.retention_seconds`.
- R2 endpoint, bucket, prefix, and credentials are env-only deployment values.
- `scheduled.go` no longer stores CWL wars; the wars pipeline owns that data.
- Python scripts moved out of `scripts` and into root `old-python`.
- Go test files moved out of `scripts` and into root `tests`.
- Go test files moved out of `internal` and into root `tests`.
- Single-owner helpers from `helpers.go`, `json_helpers.go`, and `tracker.go`
  moved into their owning scripts.

## Design Decisions

- Bot-player targets come from `basic_clan.member_tags` plus
  `tracked_player_targets`.
- Bot-player target cursor state is stored in Valkey under
  `botplayers:cursor:targets`.
- Bot-player snapshots stay in Valkey and are updated only after SQL and event
  writes succeed.
- Giveaway status changes set `event_pending` before publishing to Valkey
  Streams.
- Giveaway pending markers are cleared only after the event append succeeds.
- Reddit dedupe moved from an in-memory map to idempotent `reddit_posts`
  inserts.
- Event stream consumers acknowledge entries only after a gRPC send succeeds.
- Filtered-out event entries are acknowledged to avoid permanently pending
  stream messages.

## Deviations

- The current API still reads several Mongo collections, so SQL-only writes need
  a matching API migration before deployment.
- `events.go` uses one Valkey consumer group meant for the Discord bot, not many
  independent filtered subscribers.
- Mock and dry-run script stores are intentionally minimal and only exercise
  control flow.
- `bot_clans.go` remains Mongo/in-memory because this pass only required the
  named remaining scripts and it still owns legacy bot event behavior.
- Moved script-internal tests are behind the `script_internal_tests` build tag
  because Go tests outside `scripts` cannot access unexported script internals.
- Moved platform/internal utility tests are similarly build-tagged by package.

## Tradeoffs

- Valkey Streams were chosen over direct bot calls so each tracking script does
  not need to own gRPC delivery and replay logic.
- Valkey Streams were chosen over a SQL outbox to keep event delivery light and
  avoid adding polling load to Timescale.
- Stream replay survives script or bot downtime, but Valkey restart durability
  depends on the deployed Valkey AOF/RDB persistence settings.
- The bot-player SQL schema keeps profile changes append-only and season stats
  aggregated to keep reads simple.

## Schema Changes

- Added `tracked_player_targets`.
- Added `player_profile_changes`.
- Added `player_season_stats`.
- Expanded `giveaways` to match the current API giveaway document shape.
- Added `reddit_posts`.
- Added `leaderboard_snapshots`.

## Client Changes

- Added `Player.ClanCapitalContributions` in local `clashy.go`.
- This supports bot-player capital donation stat increments from the Clash API.

## Open Questions

- Confirm whether the Discord bot will always consume all event topics from one
  stream connection.
- Confirm whether old Mongo giveaway/player stat API routes should be migrated
  in the same rollout before deploying SQL-only writes.
