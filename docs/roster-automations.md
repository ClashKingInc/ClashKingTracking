# Roster automation (inside `bot-automations`)

## What this workload is for

Roster automation turns an absolute time or a roster-event offset into durable webhook/message work. It schedules only; the bot owns Discord execution.

## When and how it runs

The scanner runs every `roster_automations.scan_seconds` inside `bot-automations` and claims up to `roster_automations.batch_size` due rules.

```text
Find enabled rules with a due time <= now
  -> expand each targeted roster into a durable execution
  -> retire obsolete pending or expired-lease executions after a time change
  -> claim a bounded batch of pending or expired processing executions
  -> publish exact action and target to the event stream
  -> mark rule/execution state according to handoff result
```

When `event_offset_days` is null, `scheduled_at` is the absolute due time and the rule runs once. When it is set, each targeted roster's due time is `roster.event_start_time + event_offset_days` days; negative offsets run before the event and positive offsets after it. A roster without an event start has no due event-relative execution. The scanner does not derive due times from signup-open/close booleans.

## Data and events

Reads `roster_automation_rules` and roster identity. Writes `roster_automation_executions` with attempts, status, next attempt, claim/completion time, and errors. Publishes bot work only after durable execution state exists.

Changing an event time, offset, or rule from relative to absolute retires obsolete pending executions and expired processing leases before they can be reclaimed. An active processing lease is not cancelled because publication may already be in flight. If a moved event returns to an earlier due time, an execution missed specifically because of that schedule change may be re-armed; completed executions are not replayed. Changing an absolute rule's exact time resets its executed state so the new occurrence can be claimed. Execution identity prevents a restart from silently creating unrelated duplicate work.

## Configuration

- `roster_automations.scan_seconds`
- `roster_automations.batch_size`
- SQL and event-stream settings

## Boundaries

This repository does not edit a Discord message or call a webhook. The bot is deliberately untouched. There is no generic scheduling framework shared with war jobs; roster and war schedules have different identities and failure rules.
