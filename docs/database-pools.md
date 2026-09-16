# Database connection limits

`config.json` owns `database_pools`. Platform startup applies the selected
script's limit to the connection string used by its operational stores. Metrics
get a separate connection string with `stats_max_conns`. Both URL and keyword
connection strings are supported; JSON settings override pool options in the DSN.

These are **per-pool limits**, not a global PostgreSQL connection ceiling.
War discovery and CWL each create two operational pools, so their configured
maximum of 2 permits 4 operational connections per process. Scheduled creates
its scheduled and leaderboard pools; notifications creates event and campaign
pools; bot automations creates giveaway and roster pools. Each receives the
selected composite script's limit, not a separate child override.

Other script overrides are globalclans 6, battlelogs 6, war-archiver 3,
basicplayers/scheduled/notifications/bot-automations/availability 1. Remaining
scripts default to 2 per pool. Metrics permit 1 additional connection per process.
Gateway and reminders each also use one dedicated LISTEN session, outside pools.

Minimum connections are 0, idle lifetime is 5 minutes, maximum lifetime is
30 minutes with up to 5 minutes of jitter, and connection establishment timeout
is 10 seconds. The connect timeout is **not** a pool-acquisition or SQL query
timeout. Retry/error-isolation and query deadlines are separate work.

The limits take effect on a future process start; editing this file does not
reconfigure or restart deployed services. Count every replica and deployment
overlap separately. API Hyperdrive, Admin, maintenance, and non-Tracking clients
are outside this configuration. Do not describe these values as a hard whole-
server budget, especially because Hyperdrive's origin limit is soft.

Dedicated LISTEN connections parse the pool DSN through pgxpool first and use
only the underlying pgx connection configuration, so pool parameters are never
sent to PostgreSQL as session settings.
