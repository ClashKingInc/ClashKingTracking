# Discord gateway and delivery

## Gateway (`discord-gateway`)

The gateway is the only long-running Discord socket. Disgo opens Discord's recommended shard count and subscribes to guild, member, guild-message, and message-content intents. Message content can be disabled with `DISCORD_MESSAGE_CREATE_ENABLED=false` when link parsing is not needed.

Discord library diagnostics are emitted through a payload-safe handler: both third-party messages and attributes are replaced with a fixed diagnostic before reaching logs, while parse failures produce only a bounded generic error category for Sentry. The snowflake decoder accepts Discord's occasional unquoted integer representation and parses it directly as `uint64`, preserving values larger than JavaScript's exact-integer range.

It acquires one PostgreSQL advisory ownership lock per Discord application before opening the gateway. READY rotates each shard's generation and fences older cache coverage; RESUMED keeps the generation. Every mutation captures its shard sequence before entering one ordered, backpressured writer queue. Fifteen-second heartbeats pass through that writer, and API readers fail closed when the matching shard generation is unhealthy or stale.

Shard startup is supervised together with the SQL writer, advisory-lock monitor, activity reconciler, and event-stream consumer. A failure in any of those components cancels startup immediately instead of waiting for all shard Identify attempts to finish or allowing the mutation queue to stall.

Guild startup snapshots replace guild metadata, channels, and roles transactionally. Servers active within 90 days receive a full member chunk; inactive servers retain basic metadata without the bulk member cache. The in-memory Disgo cache does not retain members independently. A generation-scoped synchronization token rejects older chunk completions, while member joins, updates, and leaves received during the request are replayed after the accepted snapshot. A chunk that does not finish within two minutes clears its synchronization state so reconciliation can retry it. Guild unavailability marks that guild incomplete without deleting retained state; an actual guild leave deletes it.

READY establishes an atomic inventory baseline before a shard becomes healthy: every announced guild ID is moved into the new generation as unavailable and metadata-incomplete, and its stale child cache is cleared. Each subsequent GUILD_CREATE promotes only that guild to available and metadata-complete. A guild that Discord cannot make available therefore remains individually fail-closed without holding every ready guild on the shard behind an incomplete event count.

Every five minutes the gateway reconciles current `servers.last_command_at` activity. A newly active guild starts a fenced member chunk, while a newly inactive guild clears its retained members and marks member coverage incomplete. The API can also publish an authenticated `discord_guild_activity` signal after an activation commit. The gateway rechecks that the guild is active, present in its current healthy generation, and not already complete or syncing before starting the same fenced chunk; the five-minute database pass remains the fallback if that best-effort signal is lost.

Message-create events from non-bot users are filtered before publication, so only official Clash share URLs enter the shared event stream. Message deletion, presence, and voice events are not published or persisted. READY, RESUMED, guild availability, writer health, and shard sequence are persisted only as cache-readiness state.

## Delivery (`discord-delivery`)

The delivery process is REST-only: it does not open another gateway connection. It consumes the shared Valkey stream in its own `discord-delivery` consumer group and uses Disgo to send the Discord message. Delivery is best effort: each applicable destination is attempted independently, and processed, irrelevant, invalid, expired, or failed events are acknowledged without a later send retry. Provider pacing and cooldowns are still handled by Disgo during the one attempt. A permanent Discord unknown-channel, unknown-webhook, missing-access, or missing-permission response disables only the exact configuration revision that was attempted and stores a sanitized reason; a concurrent repair therefore cannot be disabled by an older failure.

Reminder events resolve their destination from the typed reminder payload or current PostgreSQL configuration. Player tags resolve through `player_links`, and the Discord member cache prevents mentions for users who are no longer in the target server. Allowed mentions are explicitly limited to those resolved users.

Clan, player, war, capital, and Reddit events map to configured `server_logs` types. The process resolves the configured webhook to its channel and sends with the bot identity, so it does not need to share in-memory webhook or channel objects with the gateway. Giveaway start/end events use their configured channel. Official Clash share links load the server's link-parse settings and player/clan data through the ClashKing API, then send a compact response without deleting the original message.

## Configuration

Both processes use `DISCORD_BOT_TOKEN` (with `BOT_TOKEN` as a migration fallback) and the Timescale settings. The gateway uses `discord_gateway.queue_size`; delivery uses `discord_delivery.batch_size` plus the common event stream and Valkey settings. When link parsing is enabled, delivery also requires `CLASHKING_API_ORIGIN` and `CLASHKING_API_TOKEN`. Dev and production run as separate processes with separate application tokens against the intended environment database.

Neither process registers commands. Command registration and interaction handling belong to the TypeScript Worker.
