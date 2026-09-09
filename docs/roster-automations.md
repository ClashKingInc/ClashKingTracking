# Roster automation (inside `bot-automations`)

## What this workload is for

Roster automation turns an exact user-selected time into durable webhook/message work. It schedules only; the bot owns Discord execution.

## When and how it runs

The scanner runs every `roster_automations.scan_seconds` inside `bot-automations` and claims up to `roster_automations.batch_size` due rules.

```text
Find enabled, unexecuted rules with scheduled_at <= now
  -> claim a bounded batch
  -> create/upsert roster_automation_executions
  -> publish exact action and target to the event stream
  -> mark rule/execution state according to handoff result
```

`scheduled_at` is an absolute timestamp. It is not derived from signup-open/close booleans, event-start offsets, or a relative reusable schedule.

## Data and events

Reads `roster_automation_rules` and roster identity. Writes `roster_automation_executions` with attempts, status, next attempt, claim/completion time, and errors. Publishes bot work only after durable execution state exists.

Updating a rule's exact time resets its executed state so the new occurrence can be claimed. Execution identity prevents a restart from silently creating unrelated duplicate work.

## Configuration

- `roster_automations.scan_seconds`
- `roster_automations.batch_size`
- SQL and event-stream settings

## Boundaries

This repository does not edit a Discord message or call a webhook. The bot is deliberately untouched. There is no generic scheduling framework shared with war jobs; roster and war schedules have different identities and failure rules.
