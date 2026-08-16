# Tracking dossiers

Each dossier answers the same questions: what the process owns, how it selects targets, which Clash endpoint it calls, what it writes, what it emits, and what it deliberately leaves to another process.

The processes remain separate so each workload can be enabled, stopped, and observed independently. They share PostgreSQL contracts, a Valkey event stream, and the availability gate; they do not share in-process state.

| Process | Dossier |
| --- | --- |
| Global clan crawl | [globalclans.md](globalclans.md) |
| Live clan and war tracking | [trackedclans.md](trackedclans.md) |
| Global war discovery | [war-discovery.md](war-discovery.md) |
| CWL | [cwl.md](cwl.md) |
| Tracked players | [trackedplayers.md](trackedplayers.md) |
| Basic players | [basicplayers.md](basicplayers.md) |
| Battle logs | [battlelogs.md](battlelogs.md) |
| Raid Weekend | [capital.md](capital.md) |
| Reminders | [reminders.md](reminders.md) |
| Mobile and Discord delivery | [notifications.md](notifications.md) |
| Event transport | [events.md](events.md) |
| Availability and maintenance | [availability.md](availability.md) |
| Fixed scheduled work | [scheduled.md](scheduled.md) |
| Leaderboards inside scheduled | [leaderboards.md](leaderboards.md) |
| Bot-side lightweight automation | [bot-automations.md](bot-automations.md) |
| Reddit feed automation | [reddit.md](reddit.md) |
| Giveaway automation | [giveaways.md](giveaways.md) |
| Roster automation | [roster-automations.md](roster-automations.md) |

All Clash requests pass through the shared request gate. A short proxy outage pauses work without changing game clocks. Confirmed Clash maintenance pauses work and, at recovery, shifts the durable war clocks by the measured duration.
