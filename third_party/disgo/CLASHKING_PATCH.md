# ClashKing gateway lifecycle patch

This directory is an exact source copy of `github.com/disgoorg/disgo` tag
`v0.19.6` (`de15e6a`) plus the narrow changes in `gateway/gateway.go` and
`gateway/gateway_config.go`. The upstream Apache-2.0 license is retained.

The released gateway has several independent asynchronous reconnect initiators
(read failure, heartbeat timeout/send failure, opcode 7, and invalid session),
while its connection, heartbeat, and resumable-session fields are shared. It
also calls graceful `Close` when a pre-READY/RESUMED attempt fails, clearing the
session ID, sequence, and resume URL that should be retried. Upstream issue #494
independently demonstrates gateway lifecycle races and remains open in v0.19.6.

The local patch:

- serializes every reconnect through one owner per gateway instance;
- protects lifecycle, session, and heartbeat state and avoids the status/connection lock inversion;
- preserves resumable state across ordinary reconnect failures and clears it only when Discord invalidates it;
- bounds READY/RESUMED waits without blocking a late listener result and uses
  exponential reconnect delay with jitter;
- retains the existing shared Identify rate limiter and terminal close-code classification.

`scripts/discord_gateway_lifecycle_test.go` runs the patched gateway against a
local fake WebSocket server and asserts exact Identify, Resume, connection, and
dial-attempt counts for the supported lifecycle cases. The focused suite is
also run with Go's race detector.
