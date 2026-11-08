#!/usr/bin/env python3
"""Retain read-only container lifecycle evidence outside a disposable container."""
import argparse
import datetime
import json
import logging
import logging.handlers
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import threading
import time


def docker(*args, timeout=15):
    result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"docker {args[0]} failed ({result.returncode})")
    return result.stdout


def redact(text):
    text = re.sub(r"(?i)(/webhooks/\d+/)[^\s/?\"']+", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)(authorization[\"'\s]*[:=][\"'\s]*)(?:bearer\s+)?[^,;\s\"'}]+", r"\1[REDACTED]", text)
    return re.sub(r"(?i)((?:password|token|secret|api[_-]?key)[\"'\s]*[:=][\"'\s]*)[^,;\s\"'}]+", r"\1[REDACTED]", text)[:2000]


def inspect(container):
    value = json.loads(docker("inspect", container))[0]
    state = value.get("State", {})
    policy = value.get("HostConfig", {})
    return {
        "container": value["Id"], "name": value.get("Name", "").lstrip("/"),
        "image": value.get("Config", {}).get("Image"),
        "status": state.get("Status"), "exit_code": state.get("ExitCode"),
        "oom_killed": state.get("OOMKilled"), "error": redact(state.get("Error", "")),
        "started_at": state.get("StartedAt"), "finished_at": state.get("FinishedAt"),
        "restart_count": value.get("RestartCount"), "restart_policy": policy.get("RestartPolicy"),
        "memory_limit_bytes": policy.get("Memory"), "memory_swap_bytes": policy.get("MemorySwap"),
    }


def recent_logs(container):
    # Never retain Config.Env or the full docker inspect payload. Keep only
    # diagnostic fields from logs, with credential-shaped values redacted.
    result = subprocess.run(["docker", "logs", "--tail", "80", "--timestamps", container], capture_output=True, text=True, timeout=15)
    rows = []
    for line in (result.stdout + result.stderr).splitlines()[-80:]:
        _, _, payload = line.partition(" ")
        try:
            item = json.loads(payload)
        except (ValueError, TypeError):
            if any(marker in line.lower() for marker in ("panic:", "fatal error", "signal:", "out of memory")):
                rows.append(redact(line))
            continue
        if isinstance(item, dict):
            rows.append({k: redact(str(item[k])) for k in ("time", "level", "msg", "err", "error", "script", "domain") if k in item})
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uuid", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval", type=float, default=60)
    parser.add_argument("--duration", type=float, default=0)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9]{24}", args.uuid) or args.interval < 1:
        parser.error("a 24-character resource UUID and interval >= 1 are required")
    os.umask(0o077)
    directory = Path(args.output)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    logger = logging.getLogger("globalclans-observer")
    logger.setLevel(logging.INFO)
    handler = logging.handlers.RotatingFileHandler(directory / "events.jsonl", maxBytes=16*1024*1024, backupCount=3)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    stop = threading.Event()
    def record(kind, **fields):
        logger.info(json.dumps({"time": datetime.datetime.now(datetime.timezone.utc).isoformat(), "kind": kind, "resource": args.uuid, **fields}, separators=(",", ":")))
        handler.flush()
    def halt(*_):
        stop.set()
    signal.signal(signal.SIGTERM, halt)
    signal.signal(signal.SIGINT, halt)
    since = datetime.datetime.now(datetime.timezone.utc).isoformat()
    events = subprocess.Popen(["docker", "events", "--since", since, "--filter", "type=container", "--filter", "label=coolify.name="+args.uuid, "--format", "{{json .}}"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    def read_events():
        for line in events.stdout:
            if stop.is_set(): break
            try:
                item = json.loads(line)
                action = item.get("Action", item.get("status", ""))
                if action not in ("start", "die", "stop", "kill", "oom", "destroy", "restart") and not action.startswith("health_status"):
                    continue
                actor = item.get("Actor", {})
                attrs = actor.get("Attributes", {})
                container = actor.get("ID", item.get("id", ""))
                evidence = {"action": action, "container": container, "exit_code": attrs.get("exitCode"), "signal": attrs.get("signal"), "docker_time_nano": item.get("timeNano")}
                try: evidence["state"] = inspect(container)
                except Exception as error: evidence["inspect_error"] = str(error)
                if action in ("die", "oom", "kill", "stop"):
                    try: evidence["recent_logs"] = recent_logs(container)
                    except Exception as error: evidence["log_error"] = str(error)
                record("lifecycle", **evidence)
            except Exception as error:
                record("observer_error", error=redact(str(error)))
    reader = threading.Thread(target=read_events, daemon=True)
    reader.start()
    deadline = time.monotonic() + args.duration if args.duration else None
    record("observer_started")
    try:
        while not stop.is_set() and (deadline is None or time.monotonic() < deadline):
            try:
                containers = docker("ps", "--filter", "label=coolify.name="+args.uuid, "--format", "{{.ID}}").split()
                if not containers: record("sample", running=False)
                for container in containers:
                    usage = json.loads(docker("stats", "--no-stream", "--format", "{{json .}}", container))
                    memory = {}
                    if Path("/proc/meminfo").exists():
                        for line in Path("/proc/meminfo").read_text().splitlines():
                            key, _, val = line.partition(":")
                            if key in ("MemAvailable", "MemTotal"): memory[key+"_kb"] = int(val.split()[0])
                    record("sample", running=True, state=inspect(container), usage={k: usage.get(k) for k in ("CPUPerc", "MemUsage", "MemPerc", "PIDs", "BlockIO")}, host_memory=memory, disk_free_bytes=shutil.disk_usage(directory).free)
            except Exception as error:
                record("observer_error", error=redact(str(error)))
            if events.poll() is not None:
                raise RuntimeError("Docker event stream ended; supervisor must restart observer")
            stop.wait(args.interval)
    finally:
        stop.set()
        events.terminate()
        try: events.wait(timeout=5)
        except subprocess.TimeoutExpired: events.kill()
        reader.join(timeout=5)
        record("observer_stopped")


if __name__ == "__main__":
    main()
