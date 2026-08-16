# Lightweight bot automations (`bot-automations`)

## What this runtime is for

`bot-automations` starts Reddit, giveaways, and roster automation together. They are low-volume workloads that can share process startup, SQL/Valkey connections, logging, metrics, and shutdown without merging their internal logic.

## Child workloads

- Reddit polls configured feeds at `reddit.poll_seconds`, stores/publishes unseen posts, and advances its per-feed state.
- Giveaways scans at `giveaways.scan_seconds`, closes or advances due giveaway state, and publishes required bot work.
- Roster automation scans exact `scheduled_at` rules as described in [roster-automations.md](roster-automations.md).

## Runtime behavior

Each child runs in its own goroutine. If one returns an unexpected error, the composite cancels the other children and exits so supervision can restart one coherent unit. Graceful shutdown waits for all children.

## Data, endpoints, and events

Reddit calls the configured Reddit API credentials. Giveaways and rosters primarily use PostgreSQL. All bot-facing work is published through the shared event transport; this tracking repository does not send Discord messages.

## Why one runtime

The workloads are small enough that three containers would add connections and operational surfaces without giving useful scaling control. Keeping separate source modules preserves clarity and tests while one entrypoint reduces idle RAM and duplicate clients.

## What it deliberately does not do

- It does not combine heavy clan/player/war trackers.
- It does not own bot webhook execution.
- It does not require replicas or cross-instance claims beyond its durable rules.
