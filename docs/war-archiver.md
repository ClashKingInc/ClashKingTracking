# Finished-war finalization and archive (`war-archiver`)

## What this process is for

The war archiver owns the durable end-time queue for regular and CWL wars, then moves completed payloads from PostgreSQL into compact, immutable R2 packs. The `war-discovery` and `cwl` processes only discover and schedule wars, so either process can restart without stopping finalization of rows already in `war_schedule`.

## When it runs

It checks due `war_schedule` rows every 15 seconds and archive candidates every 30 seconds. Packing does nothing until exactly 10,000 unclaimed completed wars are ready; there is no time-based partial flush. `run_once` performs one archive check and exits.

It also consumes checker-produced CWL war-tag jobs from `tracking:cwl:war-tags` in Valkey every five seconds, sharing the finalization request budget. It never discovers tags by scanning group rounds. Already-ended payloads are stored from that first fetch; future CWL wars are scheduled at end time plus 30 minutes. See [CWL handoff](cwl.md) for comparison, deduplication, and retry behavior.

## Final-war decision flow

```text
war_schedule.next_run_at is due
  -> regular war: fetch the source clan and require the exact two tags + preparation start
  -> private, missing, partial/cancelled, or newer war: try the opponent clan
  -> neither perspective exposes the scheduled war: remove the schedule immediately
  -> visible matching war not ended: keep retrying at the response cache expiry
  -> ended: store the war row, pending archive JSON, and player history, then remove the schedule
```

## Decision flow

```text
Find an unfinished live pack
  -> none exists and fewer than 10,000 pending wars? wait
  -> none exists and 10,000 are ready? reserve the next sequential pack ID
  -> claim those 10,000 pending rows in PostgreSQL
  -> serialize each canonical JSON frame
  -> compress each frame independently with Zstd level 3 + the embedded dictionary
  -> append the frames to packs/XXXXXX.pack
  -> upload the immutable object to R2
  -> GET byte 0 through wars.clashk.ing; Cloudflare primes the pack while returning only one byte
  -> in one SQL transaction:
       write every war's pack/offset/length locator
       store additive pack statistics
       delete only those pending JSON rows
       mark the pack uploaded
```

An individual frame is compressed independently, so the API uses one HTTP byte-range request to fetch one war without downloading or decompressing the whole pack.

## Failure and restart behavior

The database claim is committed before upload. If R2 is unavailable, the pack remains `building` and its pending payloads remain intact. A restart finds that same pack, rebuilds the same ordered bytes, and retries the same immutable key. PostgreSQL locators and pending-row deletion happen together only after the upload succeeds, so the API never points at a frame that was not uploaded.

## Data read and written

Reads `war_archive_pending`. Writes:

- `war_archive_packs`: sequential identity, counts, sizes, time bounds, and additive JSON statistics.
- `wars.archive_pack_id`, `archive_offset`, and `archive_compressed_bytes`.
- R2 objects named `packs/XXXXXX.pack` in the configured bucket.

It deletes a pending payload only after its corresponding locator is durable. It does not change `player_war_history`; war discovery creates that mapping immediately when the war ends.

## Pack statistics

Each pack stores additive statistics by day: wars by type, total and missed attacks, wars by size, and regular-war details. Regular-war details include Town Hall matchup hit rates with separate zero-, one-, two-, and three-star outcomes, destruction and duration where applicable, plus Town Hall distribution, total stars, wins, losses, and ties by war size. These values let broad statistics endpoints merge small pack rows instead of reading thousands of archived wars.

## Configuration

- `war_archiver.scan_seconds`: delay between archive passes.
- `war_archiver.requests_per_second`: dedicated Clash API budget for due final-war fetches.
- `war_archiver.pack_size`: completed wars claimed for one immutable object.
- `TIMESCALE_*`: PostgreSQL connection.
- `R2_ACCOUNT_ID`, or explicit `WAR_ARCHIVE_S3_ENDPOINT`.
- `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`.
- `WAR_ARCHIVE_BUCKET`, default `clashking-wars`.
- `WAR_ARCHIVE_ORIGIN`, default `https://wars.clashk.ing`, used for best-effort cache warming.
- `run_once` for one bounded pass.

The durable write always uses the authenticated S3-compatible R2 endpoint. Cache warming is best-effort: a failed public GET is logged, while the successfully uploaded pack continues through SQL finalization and remains available for an ordinary API cache miss.

The archiver is queue-driven rather than a finite crawl. Operational metrics therefore report the current `war_archive_pending` depth, each archive-pass duration, archived wars as writes, readiness, and the latest error; they deliberately omit target totals and completion percentage.

## What it deliberately does not do

- It does not archive partial packs on a timer.
- It does not store a manifest, ETag, dictionary ID, or format version.
- It does not store integer war IDs inside compressed frames; PostgreSQL owns identity and location.
- It does not create per-attack SQL rows.
