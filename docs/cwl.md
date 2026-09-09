# Clan War League tracking (`cwl`)

## Discovery and refresh

Discovery pages through all `basic_clan` rows during the existing 1st–15th UTC recovery window, without requiring public war logs or a cached league. Each pass drains its pages immediately behind the configured request limiter. Clans belonging to an already-stored current-season group are excluded; discovering one group therefore removes all its sibling clans from subsequent discovery pages. Empty pages finish the pass. The next sweep starts after `cwl.sync_seconds` (default 1800).

An independent refresh sweep runs immediately on startup and every 30 minutes after a completed sweep, sharing the group-request limiter. It reads one representative per current-season group whose state is not `ended`. It does not join war schedules or try to predict whether the group needs refreshing. Refresh continues throughout the month, independently of the discovery calendar window.

Season selection uses `season >= '2026-09' AND season < '2026-10'` (bounds calculated in UTC), so exact stored values such as `2026-09-01`, `2026-09-02`, and `2026-09-03` are included without applying a function to the indexed column.

## Checker-owned war-tag handoff

```text
GET group
  -> compare fetched war tags with that group's stored rounds
  -> enqueue only added tags into Valkey tracking:cwl:war-tags
  -> persist the new group snapshot, members, rounds and state

war-archiver consumes queued tags
  -> already scheduled/stored? acknowledge without another API request
  -> otherwise GET war by tag
       ended: allocate ID and save final payload in one SQL transaction
       future: create war_schedule + player_timers, next_run_at = end + 30 minutes
  -> acknowledge only after SQL succeeds
```

The archiver does **not** scan group rounds to discover work. The checker compares API and database values on every refresh, including the final response that changes a group to `ended`.

The handoff uses a deduplicated Valkey sorted set with no expiry. Enqueue happens before the new group snapshot is committed: if enqueue fails, the snapshot remains unchanged and the next comparison retries the new tags. If SQL fails afterward, replaying the enqueue is safe. Failed worker jobs retry after five minutes; successful scheduling/finalization removes the job. A job seen before its group commit is retried. Existing pending schedules continue independently after a group ends. Deploy both processes with the same persistent Valkey instance; do not flush this queue. Valkey data loss is not repaired by scanning unchanged group snapshots.

An already-ended war is stored from its first response, without scheduling a second HTTP fetch. Future wars receive their final fetch 30 minutes after end time; a still-unfinished response then retries after five minutes. The archiver stores observed war size back on the group. CWL league remains group metadata, never a new war column.

## League resolution and configuration

### War-worker throughput and size assignment

When a group first exposes real tags, the checker marks one job `setSize: true`; other jobs explicitly carry `false`. The designated job fills `war_size` only when it is null, using its fetched or already-stored war size. Temporary failures retain the flag. A permanently unavailable designated tag hands responsibility to the next stored round tag, when available. Legacy queued jobs without a flag remain supported: one job per group per process attempts the conditional update, so existing values are never rewritten.

The archiver continuously feeds up to 256 in-flight hydration jobs from a bounded buffer. Known war tags are checked in batches. Each free slot refills without waiting for the slowest request; polling waits only when there are no additional jobs due. Hydration and finalization still share the configured request-start limiter. Run one archiver consumer: its in-flight set prevents duplicate dispatch within that process, and restart replay remains safe through SQL deduplication.

The existing one-time league resolution is retained: reuse the group's saved league, otherwise use sibling `basic_clan` league data or the configured clan-profile lookup. A failed league lookup does not prevent storing the group; a later refresh can resolve it.

- `cwl.requests_per_second`: shared discovery/refresh group and optional profile budget.
- `cwl.sync_seconds`: default 1800, applied between complete sweeps, not individual pages.
- `cwl.resolve_league_from_clan_profile`: optional first-discovery league lookup.
- `war_archiver.requests_per_second`: shared war-tag hydration and finalization budget.
- Existing `cwl.war_requests_per_second` remains accepted for scoped tooling; the running checker no longer fetches war payloads.
- Both processes require the existing Timescale, Valkey and proxy configuration.

No schema migration is required. Discovery cursors are process-local; after restart, discovery begins again but excludes already-collected sibling clans. Newly signed-up clans missed earlier are revisited on later discovery sweeps. HTTP 404 for a clan without a CWL group remains an expected discovery outcome.
