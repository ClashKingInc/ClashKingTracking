#!/usr/bin/env python3
"""Exercise the observer with a real local container that exits unexpectedly."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid


def run(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout.strip()


def main():
    endpoint = os.environ.get("DOCKER_HOST") or run("docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}")
    if not endpoint.startswith("unix://"):
        raise SystemExit("Observer verification requires a local Docker socket")
    resource = uuid.uuid4().hex[:24]
    name = "ck-observer-e2e-"+resource
    with tempfile.TemporaryDirectory(prefix="ck-observer-e2e-") as directory:
        observer = subprocess.Popen([sys.executable, str(Path(__file__).with_name("globalclans_observer.py")), "--uuid", resource, "--output", directory, "--interval", "1", "--duration", "8"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            target = Path(directory)/"events.jsonl"
            deadline = time.monotonic()+5
            while not target.exists() and time.monotonic()<deadline: time.sleep(.05)
            if not target.exists(): raise RuntimeError("observer did not start")
            run("docker", "run", "-d", "--name", name, "--label", "coolify.name="+resource, "busybox:1.36", "sh", "-c", 'echo \'{"level":"error","msg":"password=observer-fixture-secret","token":"observer-fixture-secret"}\'; sleep 2; exit 7')
            if run("docker", "wait", name)!="7": raise RuntimeError("fixture did not exit as requested")
            _, error = observer.communicate(timeout=15)
            if observer.returncode: raise RuntimeError(error)
            text=target.read_text(); rows=[json.loads(line) for line in text.splitlines()]
            deaths=[r for r in rows if r.get("kind")=="lifecycle" and r.get("action")=="die"]
            if not deaths or deaths[-1].get("state",{}).get("exit_code")!=7: raise RuntimeError("exit evidence not retained")
            if deaths[-1]["state"].get("oom_killed") is not False: raise RuntimeError("OOM state not captured")
            if "observer-fixture-secret" in text: raise RuntimeError("credential-shaped log content leaked")
            if "[REDACTED]" not in text: raise RuntimeError("recent redacted logs missing")
            if not any(r.get("kind")=="sample" for r in rows): raise RuntimeError("periodic sample missing")
            print("PASS actual Docker observer: external exit evidence, exit code, OOM flag, recent redacted logs, periodic samples")
        finally:
            if observer.poll() is None: observer.terminate();observer.wait(timeout=10)
            subprocess.run(["docker","rm","-f",name],capture_output=True)


if __name__=="__main__": main()
