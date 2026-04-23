from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


DEFAULT_INSTRUMENTS = [
    "eurusd",
    "gbpusd",
    "usdjpy",
    "audusd",
    "usdcad",
    "usdchf",
    "eurjpy",
    "eurgbp",
    "eurchf",
    "audjpy",
    "gbpjpy",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a date-filtered FX research snapshot with the same filenames as the main M1 surface.")
    parser.add_argument("--source-dir", default="C:/fx_data/m1")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2025-12-31")
    parser.add_argument("--instruments", nargs="+", default=DEFAULT_INSTRUMENTS, choices=DEFAULT_INSTRUMENTS)
    return parser.parse_args()


def filter_frame(path: Path, start_ts: pd.Timestamp, end_ts: pd.Timestamp) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    mask = (frame["timestamp"] >= start_ts) & (frame["timestamp"] < end_ts)
    filtered = frame.loc[mask].copy()
    return filtered.sort_values("timestamp").reset_index(drop=True)


def main() -> None:
    args = parse_args()
    source_dir = Path(args.source_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    start_ts = pd.Timestamp(args.start_date, tz="UTC")
    end_ts = pd.Timestamp(args.end_date, tz="UTC") + pd.Timedelta(days=1)

    rows: list[dict[str, object]] = []
    for instrument in args.instruments:
        for suffix in ("_5yr_m1.parquet", "_5yr_m1_bid_ask.parquet"):
            source_path = source_dir / f"{instrument}{suffix}"
            if not source_path.exists():
                raise SystemExit(f"Missing required source file: {source_path}")

            filtered = filter_frame(source_path, start_ts, end_ts)
            if filtered.empty:
                raise SystemExit(f"{source_path.name} produced an empty filtered frame for {args.start_date} -> {args.end_date}")

            target_path = output_dir / source_path.name
            filtered.to_parquet(target_path, index=False)

            rows.append(
                {
                    "instrument": instrument,
                    "file": source_path.name,
                    "rows": int(len(filtered)),
                    "start": filtered["timestamp"].min().isoformat(),
                    "end": filtered["timestamp"].max().isoformat(),
                    "days": int(filtered["timestamp"].dt.date.nunique()),
                }
            )

    summary = pd.DataFrame(rows).sort_values(["instrument", "file"]).reset_index(drop=True)
    summary.to_csv(output_dir / "snapshot_manifest.csv", index=False)
    print(output_dir)


if __name__ == "__main__":
    main()
