# GlobalClans incident evidence

`globalclans_observer.py` observes only the configured Coolify resource. It does not start, stop, restart, or modify containers. Run it on the Docker host so evidence survives application container removal.

It records lifecycle events, exit codes, OOM state, restart policy/count, recent redacted diagnostic log fields, periodic resource usage, host memory, and disk headroom. It never records container environment values or full inspect payloads. Files are private and rotate at 16 MiB, with three older files retained.

Install the reviewed Python file at `/usr/local/lib/clashking/globalclans_observer.py`, create `/var/log/clashking-globalclans` with mode `0700`, and install `clashking-globalclans-observer.service` in `/etc/systemd/system`. The unit pins resource `hk4ykclekid9tkax4j7kf6fj`; update that explicit target if the application is replaced. Enable the unit after `systemctl daemon-reload`.

Read evidence from `/var/log/clashking-globalclans/events.jsonl`. OOM events differ from explicit stop/kill events and application exits. Docker does not identify which person initiated a stop; use Coolify's audit records when that attribution is required. This observer cannot reconstruct incidents that happened before it was installed.

Run `python3 ops/verify_globalclans_observer.py` for the real local Docker check. It creates one uniquely labelled fixture container, verifies exit/OOM evidence and redaction, then removes only that fixture.
