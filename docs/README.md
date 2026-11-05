# Tracking dossiers

Each dossier answers the same questions: what the process owns, how it selects targets, which Clash endpoint it calls, what it writes, what it emits, and what it deliberately leaves to another process.

The processes remain separate so each workload can be enabled, stopped, and observed independently. They share PostgreSQL contracts, a Valkey event stream, and the availability gate; they do not share in-process state.

| Process | Dossier |
| --- | --- |
| Global clan crawl | [globalclans.md](globalclans.md) |
| Live clan and war tracking | [trackedclans.md](trackedclans.md) |
| Global war discovery | [war-discovery.md](war-discovery.md) |
| Finished-war R2 archiver | [war-archiver.md](war-archiver.md) |
| CWL | [cwl.md](cwl.md) |
| Tracked players | [trackedplayers.md](trackedplayers.md) |
| Basic players | [basicplayers.md](basicplayers.md) |
| Battle logs | [battlelogs.md](battlelogs.md) |
| Raid Weekend | [capital.md](capital.md) |
| Reminders | [reminders.md](reminders.md) |
| Discord gateway and delivery | [discord-runtime.md](discord-runtime.md) |
| Mobile and Discord delivery | [notifications.md](notifications.md) |
| Valkey event transport | [events.md](events.md) |
| Availability and maintenance | [availability.md](availability.md) |
| Fixed scheduled work | [scheduled.md](scheduled.md) |
| Leaderboards inside scheduled | [leaderboards.md](leaderboards.md) |
| Bot-side lightweight automation | [bot-automations.md](bot-automations.md) |
| Reddit feed automation | [reddit.md](reddit.md) |
| Giveaway automation | [giveaways.md](giveaways.md) |
| Roster automation | [roster-automations.md](roster-automations.md) |
| Error reporting | [observability.md](observability.md) |

All Clash requests pass through the shared request gate. A short proxy outage pauses work without changing game clocks. Confirmed Clash maintenance pauses work and, at recovery, shifts the durable war clocks by the measured duration.

## Runtime ordering verification

After DevKit migration 053, run the real-domain local checks with:

```sh
/path/to/clashking_schemas/scripts/with-test-integration.sh --profile retained-api -- \
  go run -race ./cmd/verify-tracking-state
```

The command requires the disposable fixture markers and a loopback database. It
uses real Tracking domains, PostgreSQL, Valkey and local HTTP responses, not unit
or mocked helper tests. It covers maintenance recovery after a post-commit
heartbeat failure, expired clocks, proxy classification, and reversed profile
and clan-member response order. It never contacts Discord or the public Clash API.
