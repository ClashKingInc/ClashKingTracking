# Error reporting

Sentry error reporting is opt-in. An empty `SENTRY_DSN` leaves it disabled. When enabled, `SENTRY_ENVIRONMENT` and `SENTRY_RELEASE` identify the runtime and release; the selected script and failing domain are attached as tags.

Only unexpected errors that terminate a process are captured. The integration does not forward stdout logs, info events, expected provider failures, breadcrumbs, performance traces, request data, user data, or arbitrary context. URLs, account tags, email addresses, long identifiers, and token-shaped strings are removed from error text before transmission. Repeated instances of one scrubbed error signature are suppressed for five minutes, with a bounded 512-signature in-memory cache.

Shutdown flushes the private Sentry client through the application's existing bounded shutdown context. Invalid explicit configuration fails startup; no provider call is made when the DSN is empty.
