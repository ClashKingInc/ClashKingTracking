# Isolated tracking test report

## Scope

Testing was performed on 2026-08-17 against the dedicated tracking server, its Clash API proxy, Timescale/PostgreSQL, Valkey, Mongo source, and Elasticsearch. Each runtime was started by itself with a 500 MiB memory limit. The complete tracking stack was deliberately not started together; that remains a separate user-approved phase.

Mongo was read only for every migration test. Test rows created in SQL or Valkey were removed afterward.

Results below distinguish three kinds of proof:

- **Live runtime:** the compiled process used the real proxy and test database.
- **Deterministic integration:** controlled API/SQL/Valkey/FCM substitutes exercised a state that the live game could not provide on demand.
- **Not live-tested:** an external credential, device, or calendar state was unavailable.

## Runtime summary

| Runtime | Result | Highest observed memory | Main observation |
| --- | --- | ---: | --- |
| `basicplayers` | Passed, 49 seconds | 16 MiB | Held 29.49 requests/second against a configured 30. SQL batches averaged about 10.5 ms. |
| `battlelogs` | Passed, 184 seconds | 76 MiB | The bounded pool remained below the cap. Two upstream 504s were logged and isolated to their player jobs. |
| `capital` | Passed, 123 seconds | 26 MiB | Sustained about 201–203 requests/second. An unchanged raid snapshot now causes no SQL/cache rewrite. |
| `cwl` | Passed off-season runtime, 80 seconds | 6.6 MiB | Slept cleanly outside the CWL window. Current/preparation behavior was proved separately with deterministic league responses. |
| `events` | Passed, 67 seconds | 6.3 MiB | Consumer-group transport stayed idle and bounded. Contract tests cover structured v2 values and reclaim behavior. |
| `globalclans` | Passed, 56 seconds | 72.5 MiB steady state | Sustained 1,039.61 combined requests/second against a 1,050 target. The shutdown flush no longer reports a false cancellation. |
| `notifications` | Passed, 63 seconds | 7.9 MiB | Event consumer and stored FCM sender stayed bounded. Exact FCM request construction was verified with a controlled transport. |
| `reminders` | Passed, 151 seconds | 17.1 MiB | A live config event created and then removed the correct active-war clock. Startup recovery and due-clock publication also passed. |
| `scheduled` | Passed, 243 seconds | 198.2 MiB during refresh | Fresh materialized views now receive an ordinary first refresh; populated views continue using concurrent refreshes. |
| `trackedclans` | Passed, 122 seconds | 38.1 MiB | Live clan, war, CWL, and Capital loops stayed bounded; event-interest state did not change target tracking. |
| `trackedplayers` | Passed, 123 seconds | 139 MiB during first snapshot writes | Reached 912–919 requests/second against a 950 target after removing page barriers and unnecessary profile conversion. |
| `war-discovery` and finalizer | Passed, 91-second discovery plus focused finalizer runs | 20 MiB | Regular wars schedule durably, exact finished wars persist, and private/unavailable wars use the bounded grace policy. |

Every isolated component exited with code zero and without an OOM. CPU use followed the configured work: idle consumers stayed near zero, `globalclans` used about one core at roughly 1,040 requests/second, and the higher-throughput player/battle workers used multiple cores while remaining memory bounded.

## Fixes found by testing

### Global clan write pressure

The first hydration populated missing `basic_player` rows for all clan members and temporarily reached about 343.5 MiB. That is intentional one-time hydration, because complete clan members are required for search and downstream tracking.

Steady-state batches still read many profiles but conditionally changed only about ten percent of rows: 1,155,936 requested rows produced 116,377 actual changes across 236 measured batches. The store spent about 55.4 seconds total over that run and the queue depth stayed at three. The original low throughput was caused by page barriers and database backpressure rather than the proxy; bounded asynchronous storage and separate priority/non-priority pools restored the configured rate.

### Tracked player throughput

The tracker previously waited for the slowest retry at every SQL page and decoded complete profiles into generic maps before checking whether the raw snapshot had changed. It now collects one target cycle into memory, runs one continuous bounded worker pool, and performs cheap first-snapshot/raw-equality checks before expensive comparison work. This raised observed throughput from roughly 654–689 to 912–919 requests/second while keeping memory below 140 MiB.

Snapshots use a 30-day TTL. An active snapshot refreshes only when fewer than 23 days remain, using `GETEX` so the payload is not uploaded again. After a complete target pass, a player removed from the union receives a one-day TTL; active but quiet players keep their comparison history.

### Capital write amplification

Unchanged Raid Weekend responses were rewriting every participant timer on every poll, producing more than 6.9 million unnecessary updates. The tracker now compares the compressed snapshot first. An unchanged response performs no SQL/cache write; a changed response inserts only newly observed participants, and the fixed weekend end makes existing timers immutable. During the two-minute verification run, the update counter did not move and only 35 genuinely new timer rows were inserted.

### Reminder reconciliation

The Valkey consumer previously acknowledged configuration entries even when SQL reconciliation failed. It now acknowledges only after successful work and performs one complete active-war reconciliation at startup. Discord changes remain event-driven; the five-minute safety pass is mobile-only.

Deletion now applies the same normal/CWL type filter as insertion. A live test added an 86-minute Discord reminder after the worker was already running, published `reminder_config`, observed the shared SQL job appear, removed the configuration, published again, and observed the job disappear. No synthetic reminder rows remained.

Reminder payloads are clean v2 contracts. War clocks publish numeric `minutes_remaining` and a nested war object under `data`. Discord member reminders use `clan`, `reminder`, `members`, and optional `raid`; old `_data` names and stringified JSON are not accepted or published.

### CWL live tracking

CWL can expose today's battle and tomorrow's preparation at the same time. The live tracker now holds both tagged wars independently, stores separate schedules/snapshots/player timers, and fetches each war tag once per cycle. Both roles publish normal v2 war events with explicit `war_type: "cwl"` and `war_role`.

Only the current battle has `panel_target: true`. Round-one preparation is the panel target before a battle exists; overlapping preparation still publishes discovery and lineup changes without replacing the battle panel. Deterministic integration tests cover two simultaneous wars, one panel, preparation lineup changes, idempotent schedules, the signup cutoff, and mobile ignoring preparation as a live battle-start push.

The global target query checks unknown clans individually during the 1st–3rd signup window. Once a group response stores all members, only one eligible representative remains in the refresh pool, including during discovery. Its page now runs through bounded concurrent workers rather than serial network calls; a deterministic delayed-server test proves group requests overlap. The live state does not mark a clan as no-spin until after the entire 3rd has elapsed, and negative pre-signup lookups back off to 15 minutes instead of running every fast cycle.

### Scheduled materialized views

PostgreSQL rejects `REFRESH MATERIALIZED VIEW CONCURRENTLY` for a newly created, unpopulated view. The scheduled process now recognizes that exact SQL state/message and performs a one-time ordinary refresh, then returns to concurrent refreshes. The real refresh completed in about 70.8 seconds.

## Reminders, notifications, and maintenance

War schedules and reminder jobs are durable SQL rows. When a clock fires, the process waits for API availability, rechecks that maintenance has not shifted `run_at`, fetches the current war once, publishes if the war is still active, and removes the shared clock. Discord and mobile recipients can share that fetch while retaining separate configurations.

Mobile war reminders group every verified account for one user in the same war, total remaining attacks, and create one notification. Raid reminders group verified accounts by user and clan and use the same single-notification rule. The FCM test verified the Google endpoint, bearer authorization, token, title, body, and data map without contacting a real device.

The availability controller correctly separates two cases:

- An official Clash maintenance response closes the shared request gate. Recovery shifted `war_schedule`, related `player_timers`, and `war_reminder_jobs` by exactly the measured 45-second test window.
- Proxy-only downtime closes the gate but does not move game clocks. Tracking processes waited and recovered instead of crashing or independently retrying at full rate.

There is no maintenance ledger table. One controller owns the current Valkey state, and SQL time shifts happen once on official recovery.

## War and CWL storage

Regular-war identity uses the sorted pair of clan tags plus preparation time. CWL additionally retains its Clash war tag. Both live and global discovery paths upsert the same schedule identity, so seeing a war from either side does not duplicate it.

`player_timers` permits several simultaneous timers per player. Its uniqueness boundary is `(player_tag, event_type, event_key)`: a regular war, friendly war, CWL war, and Raid Weekend can coexist. War `event_key` is the schedule key; Raid Weekend uses the clan tag. Expiry is indexed for cleanup.

The finalizer tries both clan perspectives for a regular war, validates that the response still matches the scheduled identity, persists exact complete wars, and removes their schedules/timers. A private or unavailable finished war receives a six-hour recovery grace before only its schedule/timers are removed.

## Mongo migrations

The following migrations passed with Mongo read-only:

- bans and strikes;
- basic clans;
- bot server settings;
- clan change history and clan records;
- bounded clan wars and attacks;
- bounded CWL groups plus CWL league history;
- join/leave history;
- leaderboard history;
- Legend history;
- one-guild player links;
- player online events;
- rosters;
- server clans and server settings.

Wars/attacks and CWL groups were intentionally sampled rather than copied in full. Runtime v2 events have no legacy compatibility, but migration parsers still understand the old Mongo field formats because reading historical source data is their only job.

## Player profile analytics

The JSONB profile detail table held 804,011 sampled players with valid hero, equipment, and achievement arrays. The average combined payload was about 3.6 KiB. It has only the player primary key and Town Hall index; no broad JSON GIN index was added because these global aggregations run weekly.

A read-only equipment rollup across the sample completed in about 9.7 seconds. If the sample grows by an order of magnitude, the next step should be a weekly materialized aggregate or a higher `work_mem` for that job, not billions of normalized item rows.

## Elasticsearch and PGSync

Elasticsearch contains roughly 49 million players and stayed responsive. `Town Hall 18 + "magic"` found 1,286 matching documents; returning the top ten took 6.3 ms p50 and 7.4 ms p95 after warmup. A normal API request would return only its requested page, not transfer all 1,286 hits.

PGSync is currently stopped. Both logical slots report `wal_status = lost` and `invalidation_reason = wal_removed`. They remained inactive while migrations and hydration generated more than the configured `max_slot_wal_keep_size` of 5,120 MiB, so a checkpoint recycled WAL they still required. Recreating the slots at the current database position preserves the 49-million-document index and future changes but cannot prove that no changes were missed during the gap. A full reconciliation proves consistency but must read the complete source set and will take hours. No full rebuild or destructive Elasticsearch reset was started without approval.

## External limits still open

- Today is outside an active CWL window, so the live `cwl` process could only prove its off-season path. Controlled HTTP/SQL tests proved current-plus-preparation behavior, scheduling, lineup changes, and panel selection.
- No real mobile token/device was available. SQL delivery state, grouping, retries, invalid-token handling, and the exact outbound FCM request were tested with a controlled transport.
- Reddit credentials were unavailable. Post filtering, deduplication, retry, normalized payloads, and a real Valkey stream write passed; the live Reddit stream login remains untested.
- PGSync recovery needs an explicit choice between full reconciliation and accepting the current database position as the new starting point.
- The complete stack has not been run together, by request.

## Before combined testing

1. Choose the PGSync recovery mode.
2. Provide Reddit test credentials only if a live Reddit connection must be proved before deployment.
3. Review this isolated report and approve the combined run.
4. During the combined run, watch aggregate proxy rate, Timescale write latency/queue depth, per-process CPU and memory, availability-gate recovery, reminder clock accuracy, and Elasticsearch propagation.
