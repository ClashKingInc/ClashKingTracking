# Battle-log tracking (`battlelogs`)

## What this process is for

Battle-log tracking stores newly observed multiplayer battles for players whose history is worth polling. It no longer exists as a side effect of a mutable column on `basic_player`, and it does not send mobile Legend notifications.

## When it runs

It runs continuously as `battlelogs`. Two independent target loops share the total configured request budget:

- Legend targets use `battlelogs.priority_requests_per_second`.
- Standard tracked-player targets use the remaining `battlelogs.requests_per_second` budget.

Each group has its own progress statistics. Its target tags are loaded once per complete pass and kept as a small in-memory slice; this avoids re-expanding configured-clan member JSON for every page.

## How a player becomes a target

Legend targets are players whose stored league is Legend League. Standard SQL targets are Town Hall 9 or higher members of recently active configured server clans, matching `trackedplayers`. Active verified app accounts are loaded from the seven-day Valkey target set on each pass without a Town Hall filter and use the same standard request budget. Verified players already in Legend League are removed from the standard pool so the two workers cannot publish the same newly observed battle twice.

Bookmarks and war participation do not create battle-log targets. There is no `battlelogs_tracking_ttl`.

## Decision flow

```text
Load and deduplicate the current target set once
  -> split it into checkpoint batches
  -> load each batch's checkpoints with one Valkey MGET
  -> stream player jobs through the fixed worker pool
  -> GET the player's battle log
  -> identify stable battle identities not seen before
  -> reject newly discovered rows older than the first-seen lookback
  -> insert new battles in SQL
  -> advance the checkpoint only after storage succeeds
```

Pseudocode:

```text
targets = SQL target set UNION active verified accounts
for target_batch in targets:
  checkpoints = Valkey MGET for target_batch
  stream targets through bounded workers:
  log = GET /players/{tag}/battlelog
  checkpoint = checkpoints[target]
  for battle in log:
    if battle identity is already checkpointed: skip
    if no checkpoint and battle time is older than 14 days: skip
    normalize armies, result, trophies, town halls, and opponent
    queue SQL insert
  commit inserts
  save new checkpoint with its TTL
```

Checkpoint batches feed one long-lived pool rather than waiting for every retry at a batch boundary, so one slow 504 does not idle the rest of the request budget. Loading the roughly 100,000 current tags once per pass uses only a few megabytes and removes repeated JSON expansion from Postgres. The response-worker count, SQL batch, and pending-write queue are capped separately, which keeps the first-start history seed bounded. The 14-day rule applies only when deciding whether a battle is newly discoverable; the stored battle itself keeps its real API timestamp.

## Clash API used

- `GET /v1/players/{playerTag}/battlelog`.

## Data read and written

Reads target tables and `basic_player.league_id`. Writes normalized rows to `battlelogs`. A row includes the player/opponent identity, attack direction, battle type, result, trophy change, destruction, stars, town halls, armies, and the API battle time.

Valkey checkpoint keys remember the newest battle time seen for each player. `battlelogs.checkpoint_ttl_days` controls their lifetime. The checkpoint is comparison state, not target membership.

## Events and interaction

No mobile Legend event is emitted. Battle-log storage remains available to API statistics and analytics. `trackedplayers` decides who belongs to the standard fast-player set; wars do not write player rows to opt people in.

```mermaid
flowchart LR
  L[(Legend players)] --> B[battlelogs]
  T[(tracked targets)] --> B
  V[verified target cache] --> B
  B --> API[Battle-log endpoint]
  B --> C[Valkey checkpoints]
  B --> SQL[(battlelogs)]
  SQL --> Stats[API analytics]
```

## Configuration

- `battlelogs.requests_per_second`: total request-start budget.
- `battlelogs.priority_requests_per_second`: portion reserved for Legend targets.
- `battlelogs.checkpoint_ttl_days`: comparison-checkpoint retention.
- `battlelogs.first_seen_lookback_days`: `14` in the supplied config.
- `target_page_multiplier`, SQL, Valkey, and proxy settings.

## Outages and restarts

Requests pause at the availability gate. A checkpoint is stored only after SQL succeeds, so a restart or failed transaction replays the same candidate instead of losing it. Duplicate SQL identities make that replay safe.

## What it deliberately does not do

- No bookmarked-player or Legend mobile notifications.
- No writes to general `basic_player` targeting state.
- No war-derived TTL.
- No search through every player's bookmarked accounts per attack.
