from __future__ import annotations

import argparse
import json
import os
import smtplib
import sys
from email.message import EmailMessage
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from live_reporting import ensure_live_paths
from oanda_client import OandaClient, OandaCredentials


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check the health of the live trading system.")
    parser.add_argument("--live-root", default="C:/fx_data/live")
    parser.add_argument("--stale-minutes", type=int, default=15)
    return parser.parse_args()


def send_alert(subject: str, body: str) -> None:
    host = os.getenv("SMTP_HOST", "").strip()
    username = os.getenv("SMTP_USERNAME", "").strip()
    password = os.getenv("SMTP_PASSWORD", "").strip()
    sender = os.getenv("ALERT_FROM", "").strip()
    recipient = os.getenv("ALERT_TO", "").strip()
    port = int(os.getenv("SMTP_PORT", "587"))
    if not all([host, username, password, sender, recipient]):
        return

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = sender
    message["To"] = recipient
    message.set_content(body)

    with smtplib.SMTP(host, port, timeout=30) as server:
        server.starttls()
        server.login(username, password)
        server.send_message(message)


def main() -> None:
    args = parse_args()
    live_paths = ensure_live_paths(Path(args.live_root))
    issues: list[str] = []

    try:
        client = OandaClient(OandaCredentials.from_env())
        summary = client.account_summary()
        account_id = str(summary.get("id", client.account_id))
    except Exception as exc:  # pragma: no cover - network dependent
        issues.append(f"OANDA connectivity failed: {exc}")
        account_id = ""

    heartbeat_payload = {}
    if live_paths.heartbeat_path.exists():
        heartbeat_payload = json.loads(live_paths.heartbeat_path.read_text(encoding="utf-8"))
        heartbeat_ts = pd.Timestamp(heartbeat_payload["timestamp"])
        age_minutes = (pd.Timestamp.now(tz="UTC") - heartbeat_ts.tz_convert("UTC")).total_seconds() / 60.0
        if age_minutes > args.stale_minutes:
            issues.append(f"Live engine heartbeat is stale ({age_minutes:.1f} minutes old).")
    else:
        issues.append("Live engine heartbeat file is missing.")

    if live_paths.db_path.exists():
        db_age_minutes = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(live_paths.db_path.stat().st_mtime, unit="s", tz="UTC")).total_seconds() / 60.0
        if db_age_minutes > 24 * 60:
            issues.append(f"Trade database has not been updated in {db_age_minutes:.1f} minutes.")
    else:
        issues.append("Trade database is missing.")

    status = {
        "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
        "account_id": account_id,
        "heartbeat": heartbeat_payload,
        "issues": issues,
        "ok": not issues,
    }
    status_path = live_paths.root / "health_status.json"
    status_path.write_text(json.dumps(status, indent=2, sort_keys=True), encoding="utf-8")

    if issues:
        send_alert("RazerBack live health check failed", "\n".join(issues))
        raise SystemExit("\n".join(issues))
    print(status_path)


if __name__ == "__main__":
    main()
