from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests


REST_BASE = {
    "practice": "https://api-fxpractice.oanda.com",
    "live": "https://api-fxtrade.oanda.com",
}
MAX_CANDLES = 5000


def parse_ts(value: str) -> datetime:
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return datetime.fromisoformat(value).astimezone(timezone.utc)


def chunk_minutes(start: datetime, end: datetime, minutes: int = MAX_CANDLES) -> list[tuple[datetime, datetime]]:
    chunks: list[tuple[datetime, datetime]] = []
    cursor = start
    step = timedelta(minutes=minutes)
    while cursor < end:
        nxt = min(end, cursor + step)
        chunks.append((cursor, nxt))
        cursor = nxt
    return chunks


def request_with_retry(session: requests.Session, url: str, params: dict[str, str], retries: int = 6) -> dict:
    delay = 1.0
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=60)
            if response.status_code in (429, 500, 502, 503, 504):
                if attempt == retries - 1:
                    response.raise_for_status()
                time.sleep(delay)
                delay *= 2.0
                continue
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(delay)
            delay *= 2.0
            continue
    raise RuntimeError("unreachable")


def fetch_bid_ask_candles(
    token: str,
    instrument: str,
    start: datetime,
    end: datetime,
    environment: str,
) -> pd.DataFrame:
    base_url = REST_BASE[environment]
    url = f"{base_url}/v3/instruments/{instrument}/candles"
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    rows: list[dict[str, object]] = []
    for chunk_start, chunk_end in chunk_minutes(start, end):
        payload = request_with_retry(
            session,
            url,
            {
                "price": "BA",
                "granularity": "M1",
                "from": chunk_start.isoformat().replace("+00:00", "Z"),
                "to": chunk_end.isoformat().replace("+00:00", "Z"),
            },
        )
        candles = payload.get("candles", [])
        for candle in candles:
            if not candle.get("complete", False):
                continue
            bid = candle.get("bid")
            ask = candle.get("ask")
            if not bid or not ask:
                continue
            rows.append(
                {
                    "timestamp": pd.Timestamp(candle["time"]),
                    "open_bid": float(bid["o"]),
                    "high_bid": float(bid["h"]),
                    "low_bid": float(bid["l"]),
                    "close_bid": float(bid["c"]),
                    "open_ask": float(ask["o"]),
                    "high_ask": float(ask["h"]),
                    "low_ask": float(ask["l"]),
                    "close_ask": float(ask["c"]),
                }
            )
        time.sleep(0.12)

    if not rows:
        raise SystemExit(f"No candles returned for {instrument} {start} -> {end}")

    frame = pd.DataFrame(rows).drop_duplicates("timestamp").sort_values("timestamp").reset_index(drop=True)
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch OANDA M1 bid/ask candles to parquet.")
    parser.add_argument("--instrument", required=True, help="OANDA instrument name, e.g. EUR_USD")
    parser.add_argument("--from", dest="start", required=True)
    parser.add_argument("--to", dest="end", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--token")
    parser.add_argument("--token-env", default="OANDA_API_TOKEN")
    parser.add_argument("--environment", choices=("practice", "live"), default="practice")
    args = parser.parse_args()

    token = args.token
    if not token:
        import os
        token = os.getenv(args.token_env)
    if not token:
        raise SystemExit(f"Missing OANDA token. Pass --token or set {args.token_env}.")

    start = parse_ts(args.start)
    end = parse_ts(args.end)
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    existing = None
    if output.exists():
        existing = pd.read_parquet(output)
        existing["timestamp"] = pd.to_datetime(existing["timestamp"], utc=True)
        if not existing.empty:
            resume_from = existing["timestamp"].max().to_pydatetime() + timedelta(minutes=1)
            if resume_from > start:
                start = resume_from

    frame = fetch_bid_ask_candles(token, args.instrument, start, end, args.environment)
    if existing is not None and not existing.empty:
        frame = (
            pd.concat([existing, frame], ignore_index=True)
            .drop_duplicates("timestamp")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
    frame.to_parquet(output, index=False)
    print(output)


if __name__ == "__main__":
    main()
