# Privacy Compliance Notes

ClashKing Tracking is a background data collection and processing service. It exposes no user or administrator API, but it reads and writes data that can be personal when linked to a Discord account, device, roster, or Clash of Clans player tag. User privacy routes belong to ClashKingAPI; the separate Go administrator API and its revocable Discord sessions belong to ClashKingAdminPanel. Tracking must never accept either API's bearer tokens.

## Data handled here

- Clash of Clans player tags, clan tags, names, rankings, war/battle logs, raid data, activity signals, and leaderboard/history snapshots.
- Server configuration, reminders, user settings, bookmarked players/clans, and account-link dependent processing.
- Reddit community metadata used by tracking jobs, including author names and avatars where displayed.
- Operational traces and event streams controlled by `retention_seconds`.

## Global privacy rules

- Treat Discord IDs, player-account links, bookmarks, reminders, roster membership, push tokens, and support/ticket references as personal data.
- Treat public game history as product data only after it is no longer linked to a specific ClashKing or Discord user account.
- Do not collect advertising IDs, precise location, contacts, health, biometric, or other sensitive categories in this service.
- Do not log bot tokens, Clash of Clans developer credentials, OAuth tokens, API tokens, push tokens, or raw authorization headers.
- Keep OpenTelemetry/event-stream retention bounded and aggregate operational telemetry where possible.

## Deletion and export coordination

Verified access and deletion requests are initiated through the API/dashboard/bot surfaces. Tracking jobs must honor those requests by:

1. Removing Discord-user keyed preferences from `user_settings`.
2. Removing or anonymizing Discord-linked player links, reminders, roster references, open tickets, and support records owned by the requester.
3. Coordinating removal of mobile push/device records from the canonical API data store without exposing user or administrator routes from the worker.
4. Preserving public Clash of Clans history only when it is no longer tied to the requester.
5. Keeping only minimal audit/security evidence required for abuse prevention or legal obligations.

Recommended defaults:

- Push tokens: delete immediately on opt-out, logout, account deletion, or invalid-token feedback.
- Recent searches and event streams: keep short retention windows.
- Security/audit records: retain for the operational/legal limitation period and mask IP/user-agent fields in exports unless required for investigation.
