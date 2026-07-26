# Implementation Notes

## Choices

- Shared basic-player SQL upserts moved from `scripts` to `internal/utils`.
- `bot_players.go` no longer uses `CycleRunner`; it owns Run/do/store directly.
- `giveaways.go` and `scheduled.go` use Timescale-only stores; Reddit persists
  its replay dedupe keys in Valkey and publishes through the event stream.
- `events.go` now streams Valkey Stream entries over the existing gRPC service.
- Published events now go only to Valkey Streams, not the old in-memory bus.
- Valkey Stream retention is time-based and configured with
  `events.retention_seconds`.
- R2 endpoint, bucket, prefix, and credentials are env-only deployment values.
- `scheduled.go` no longer stores CWL wars; the wars pipeline owns that data.
- Python scripts moved out of `scripts` and into root `old-python`.
- Single-owner helpers from `helpers.go`, `json_helpers.go`, and `tracker.go`
  moved into their owning scripts.
- The active Go runtime has no Mongo client, configuration, or repository
  wiring. The archival `old-python` tree is unchanged and is not built or run.

## Design Decisions

- Bot-player targets are expanded from `basic_clan.members` JSONB and unioned
  with `tracked_player_targets`.
- Bot-player targets use a process-local SQL keyset cursor and safely restart
  from the beginning because their writes are idempotent.
- Bot-player snapshots stay compressed in Valkey, fall back to memory for
  mock/dry-run execution, and update only after SQL and event writes succeed.
- Giveaway status changes set `event_pending` before publishing to Valkey
  Streams.
- Giveaway pending markers are cleared only after the event append succeeds.
- Reddit dedupe uses 30-day Valkey `SET NX EX` keys, so process restarts do not
  replay recent posts and no unsupported Timescale table is required.
- Event stream consumers acknowledge entries only after a gRPC send succeeds.
- Filtered-out event entries are acknowledged to avoid permanently pending
  stream messages.
- Request limiters cap request starts at the configured rate, while worker and
  HTTP connection capacity allow up to three seconds of normal request latency
  without lowering the configured throughput ceiling.
- Scheduled snapshots run once per day. The configured interval is 86,400
  seconds and same-day rows are intentionally idempotent.
- Async SQL writers retain failed batches, apply queue backpressure while
  retrying, and drain queued work for up to ten seconds during shutdown.
- Bot-clan reminder configuration is cached for one minute. Failed reminder
  event appends are rescheduled one minute later instead of being discarded.

## Deviations

- `events.go` uses one Valkey consumer group meant for the Discord bot, not many
  independent filtered subscribers.
- Mock and dry-run script stores are intentionally minimal and only exercise
  control flow.
- `bot_clans.go` reads clan targets and reminder configuration from Timescale,
  writes capital raid cache rows to Timescale, and compares snapshots through
  Valkey or the in-memory fallback.
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

## Client Changes

- Added `Player.ClanCapitalContributions` in local `clashy.go`.
- This supports bot-player capital donation stat increments from the Clash API.

## Open Questions

- Confirm whether the Discord bot will always consume all event topics from one
  stream connection.
- Mobile push replays its own pending entries and uses `XAUTOCLAIM` to recover
  stale entries left by replaced consumers.
- Individual APNS/FCM token failures are logged and skipped so one invalid
  device cannot poison the stream entry for every subscriber.
- Giveaway state and event publication remain an at-least-once sequence: a
  crash after event append but before clearing `event_pending` can republish.
- Regular wars are stored with two attacks per member and CWL wars with one.
  The current `clashy.go` model does not expose the API's exact
  `attacksPerMember` value, so unusual friendly-war rules need a client-model
  field before they can be represented exactly.
- `go.mod` still replaces `clashy.go` with the sibling checkout. The branch
  depends on uncommitted client pagination, transport, and limiter work there,
  so the release workflow is not self-contained until those client changes are
  published and the replace is removed or the build checks out that revision.
