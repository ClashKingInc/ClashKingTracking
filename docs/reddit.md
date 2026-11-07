# Reddit feeds

`bot-automations` reads posts and comments from r/ClashOfClansRecruit. Post events include author and Searching-flair status. Valkey keeps post/comment IDs for 30 days; failed publication clears the ID for the next observation.

`discord-delivery` applies each enabled destination independently:

- `reddit_feed`: Searching posts only. `reddit_min_townhall` is optional. A resolved player at or above the minimum OR an explicit qualifying Town Hall in the title permits delivery. Body text can supply player tags but never a title-level match. TH10+ means 10. Lists/ranges and case/spacing variations are accepted.
- `reddit_user_feed`: every post by one of `reddit_accounts` (one or two usernames, case-insensitive), regardless of flair. `reddit_include_comments` optionally forwards comments on those users' posts; the submission author is matched, not the commenter.

Both use owned webhooks and optional threads, preserve explicit role-only mentions, and include a Post Link with the Reddit emoji. Valid player tags add a Player Profile button; the Bot opens it privately using the current viewer's context. Player lookups are bounded and shared across destinations. Missing profiles do not prevent title-based matches.

Migration 069 copies existing watched users into the separate feed at the same destination with comments enabled. New feeds default to comments off and no minimum. Application rollback must preserve the new settings columns/rows; destructive schema downgrade is blocked.

Delivery stays best effort. No occurrence ledger or retry table is introduced. `cmd/verify-reddit-logs` exercises the actual SQL/Valkey/delivery path with loopback providers; it does not send real Discord messages.
