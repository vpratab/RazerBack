from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd


ENGINE_PYTHON = Path(r"C:\Python311\python.exe")
ENGINE_SCRIPT = Path(r"C:\Users\saanvi\Documents\GitHub\RazerBack\scripts\live_six_sleeve_engine.py")
ENGINE_WORKDIR = Path(r"C:\Users\saanvi\Documents\GitHub\RazerBack")
LIVE_ROOT = Path(r"C:\fx_data\live_six_sleeve_clean_oos")
PID_FILE = LIVE_ROOT / "launcher_pid.txt"
HEARTBEAT_FILE = LIVE_ROOT / "engine_heartbeat.json"
STATE_FILE = LIVE_ROOT / "watchdog_state.json"
LOG_FILE = LIVE_ROOT / "watchdog.log"
STDOUT_FILE = LIVE_ROOT / "launcher_stdout.log"
STDERR_FILE = LIVE_ROOT / "launcher_stderr.log"


def now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def append_log(event: str, **payload: Any) -> None:
    LIVE_ROOT.mkdir(parents=True, exist_ok=True)
    row = {"timestamp": now_utc().isoformat(), "event": event, **payload}
    with LOG_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def save_state(payload: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def read_pid() -> int | None:
    if not PID_FILE.exists():
        return None
    raw = PID_FILE.read_text(encoding="ascii", errors="ignore").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def is_pid_running(pid: int) -> bool:
    result = subprocess.run(
        ["tasklist", "/FI", f"PID eq {pid}"],
        capture_output=True,
        text=True,
        check=False,
    )
    return str(pid) in result.stdout


def kill_pid(pid: int) -> None:
    subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True, text=True, check=False)


def heartbeat_age_seconds() -> float | None:
    if not HEARTBEAT_FILE.exists():
        return None
    try:
        payload = json.loads(HEARTBEAT_FILE.read_text(encoding="utf-8"))
        ts = pd.Timestamp(payload["timestamp"])
        return float((now_utc() - ts).total_seconds())
    except Exception:
        return None


def start_engine() -> int:
    LIVE_ROOT.mkdir(parents=True, exist_ok=True)
    stdout = STDOUT_FILE.open("a", encoding="utf-8")
    stderr = STDERR_FILE.open("a", encoding="utf-8")
    process = subprocess.Popen(  # noqa: S603
        [str(ENGINE_PYTHON), str(ENGINE_SCRIPT), "--live-root", str(LIVE_ROOT)],
        cwd=str(ENGINE_WORKDIR),
        stdout=stdout,
        stderr=stderr,
        creationflags=getattr(subprocess, "DETACHED_PROCESS", 0) | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
    )
    PID_FILE.write_text(str(process.pid), encoding="ascii")
    append_log("engine_started", pid=process.pid)
    save_state({"last_action": "engine_started", "pid": process.pid, "timestamp": now_utc().isoformat()})
    return process.pid


def check_once(max_heartbeat_age_seconds: int) -> None:
    pid = read_pid()
    heartbeat_age = heartbeat_age_seconds()
    heartbeat_stale = heartbeat_age is None or heartbeat_age > max_heartbeat_age_seconds
    pid_running = bool(pid and is_pid_running(pid))

    if pid_running and not heartbeat_stale:
        save_state(
            {
                "last_action": "healthy",
                "pid": pid,
                "heartbeat_age_seconds": heartbeat_age,
                "timestamp": now_utc().isoformat(),
            }
        )
        return

    if pid_running and heartbeat_stale:
        append_log("engine_restart_stale_heartbeat", pid=pid, heartbeat_age_seconds=heartbeat_age)
        kill_pid(pid)
        time.sleep(2)

    if not pid_running:
        append_log("engine_missing_or_dead", pid=pid, heartbeat_age_seconds=heartbeat_age)

    start_engine()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watchdog for the six-sleeve live engine.")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--max-heartbeat-age-seconds", type=int, default=300)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    append_log(
        "watchdog_started",
        once=args.once,
        interval_seconds=args.interval_seconds,
        max_heartbeat_age_seconds=args.max_heartbeat_age_seconds,
    )
    if args.once:
        check_once(args.max_heartbeat_age_seconds)
        return
    while True:
        try:
            check_once(args.max_heartbeat_age_seconds)
        except Exception as exc:  # pragma: no cover
            append_log("watchdog_error", error=str(exc))
        time.sleep(max(5, int(args.interval_seconds)))


if __name__ == "__main__":
    sys.exit(main())
