# Reddit feed automation

The `bot-automations` process runs the Reddit domain. It reads new posts and comments from `r/ClashOfClansRecruit` through graw. The request interval is `reddit.poll_seconds`; startup requires the configured Reddit credentials, Valkey, and the event stream.

Posts with the `Searching` flair become `reddit` events. Comments become `redditcomment` events inside the same `reddit` stream topic and include the original submission author. Valkey keeps post/comment seen IDs for 30 days, with separate namespaces. A failed event publish clears that seen ID so the next observation can retry.

`discord-delivery` reads enabled `server_logs` rows of type `reddit_feed`. All configured feeds receive Searching posts. A comment is sent only when its submission author matches a stored `reddit_accounts` value, case-insensitively. The commenter is not the subscription target. The optional `reddit_role_id` is the only role allowed to be mentioned; arbitrary Reddit text cannot enable automatic mentions.

Destinations use the same owned webhook and optional thread as other logs. The producer does not create destinations. The DevKit `reddit_feeds.go` repair preserves legacy top-level channel settings, role mentions, and watched accounts. Deleted or inaccessible destinations retain their records for review. Delivery remains best-effort; there is no occurrence ledger.

Validation: `cmd/verify-reddit-logs` runs the actual SQL/Valkey/Discord delivery service against a loopback provider. It checks posts, subscribed and unrelated comments, role allowlists, and forum threads. Live Reddit polling and public Discord sends are not simulated by that check.
