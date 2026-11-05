# API availability and maintenance (`availability`)

The controller probes `GET locations?limit=1` every 15 seconds. Each Clash-using
process watches a 45-second Valkey heartbeat and pauses its local request gate
on unavailable responses. A newer healthy heartbeat reopens that gate.

## Durable current state

`tracking_availability` has exactly one current-state row, not an outage history.
The controller locks this row for each probe and stores the last probe time, the
current availability state, a revision, and the first confirmed official
maintenance time. An older overlapping probe cannot replace a newer one.

Proxy-only outages never move game clocks. A proxy failure after a confirmed
official-maintenance response preserves the known official interval until a
successful probe. As before, duration ends at the first successful probe; an
unobservable proxy tail can therefore make the estimate longer than the actual
game maintenance. This is not independent measurement of the game's clock.

## Official recovery

In one PostgreSQL transaction, recovery extends schedules whose old end is after
the official interval began, matching war participant timers, and reminder jobs.
The same transaction clears the current official interval and increments the
state revision. Wars whose old end elapsed during maintenance are included.
Preparation time and schedule identity remain unchanged.

Only after SQL commits does the controller publish the healthy Valkey heartbeat.
If publishing fails or the process restarts in that gap, the next probe reads the
already-cleared SQL interval and does not shift clocks again. The Lua heartbeat
write rejects an older revision that arrives after a newer one.

Every probe refreshes the heartbeat, including identical unavailable states.
Heartbeat expiry no longer deletes the original official interval: SQL retains
it for a restarted controller. War participant cleanup retains timers while their
war_schedule exists, so recovery can extend them after their old expiry. Normal
finalization/abandonment retires that participation state.

## Configuration and release

The 15-second probe and 45-second TTL are code constants. `--once` records and
publishes one probe, then exits; the normal controller runs continuously.

Apply DevKit migration 053 before deploying these writers. Roll out all profile
writers together so older code cannot bypass their new observation-time guards.
Do not deploy the first durable controller during an already-running legacy
maintenance interval: legacy Valkey-only state is not automatically imported.
No delivery ledger, occurrence lease, or notification retry policy is added.
