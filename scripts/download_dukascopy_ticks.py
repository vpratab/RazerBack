from __future__ import annotations

import argparse
import json
import logging
import lzma
import os
import struct
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from pathlib import Path

import polars as pl
import requests

from fx_pipeline_config import ALL_PAIRS, DOWNLOAD_STATE_ROOT, LOG_ROOT, PRICE_DIVISORS, TICK_ROOT, ensure_pipeline_dirs


URL_TEMPLATE = "https://datafeed.dukascopy.com/datafeed/{pair}/{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
TICK_SCHEMA = {
    "timestamp": pl.Datetime(time_unit="us", time_zone="UTC"),
    "bid": pl.Float64,
    "ask": pl.Float64,
    "bid_volume": pl.Float64,
    "ask_volume": pl.Float64,
    "pair": pl.Utf8,
}


@dataclass(frozen=True)
class DownloadResult:
    pair: str
    day: str
    output_path: str
    rows: int
    hours_with_ticks: int
    empty_day: bool
    skipped_existing: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download resumable Dukascopy tick data into daily parquet files.")
    parser.add_argument("--pairs", nargs="+", default=ALL_PAIRS, choices=ALL_PAIRS)
    parser.add_argument("--start-date", default="2011-01-01")
    parser.add_argument("--end-date", default="2026-04-24")
    parser.add_argument("--date-order", choices=("ascending", "descending"), default="ascending")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--workers", type=int, default=max(2, min(8, os.cpu_count() or 4)))
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    parser.add_argument("--retries", type=int, default=6)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def configure_logging() -> None:
    ensure_pipeline_dirs()
    log_path = LOG_ROOT / "dukascopy_tick_download.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(stream=sys.stdout),
        ],
    )


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_dates(start_date: date, end_date: date) -> list[date]:
    days: list[date] = []
    cursor = start_date
    while cursor <= end_date:
        days.append(cursor)
        cursor += timedelta(days=1)
    return days


def daily_tick_output_path(pair: str, day: date) -> Path:
    return TICK_ROOT / pair / f"{day.year:04d}" / f"{day.isoformat()}.parquet"


def daily_metadata_path(pair: str, day: date) -> Path:
    return DOWNLOAD_STATE_ROOT / pair / f"{day.isoformat()}.json"


def completion_marker_path(pair: str, start_date: date | None = None, end_date: date | None = None) -> Path:
    if start_date is not None and end_date is not None:
        return DOWNLOAD_STATE_ROOT / pair / f"_complete_{start_date.isoformat()}_{end_date.isoformat()}.json"
    return DOWNLOAD_STATE_ROOT / pair / "_complete.json"


def build_url(pair: str, day: date, hour: int) -> str:
    return URL_TEMPLATE.format(
        pair=pair,
        year=day.year,
        month=day.month - 1,
        day=day.day,
        hour=hour,
    )


def fetch_hour_ticks(
    session: requests.Session,
    pair: str,
    day: date,
    hour: int,
    timeout_seconds: float,
    retries: int,
) -> list[tuple[datetime, float, float, float, float, str]]:
    url = build_url(pair, day, hour)
    divisor = PRICE_DIVISORS[pair]
    base_dt = datetime.combine(day, dt_time(hour=hour), tzinfo=timezone.utc)

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout_seconds)
            if response.status_code == 404:
                return []
            response.raise_for_status()
            if not response.content:
                return []

            raw = lzma.decompress(response.content)
            if not raw:
                return []

            rows: list[tuple[datetime, float, float, float, float, str]] = []
            for time_ms, ask_i, bid_i, ask_volume, bid_volume in struct.iter_unpack("!IIIff", raw):
                rows.append(
                    (
                        base_dt + timedelta(milliseconds=int(time_ms)),
                        float(bid_i) / divisor,
                        float(ask_i) / divisor,
                        float(bid_volume),
                        float(ask_volume),
                        pair,
                    )
                )
            return rows
        except (requests.RequestException, lzma.LZMAError, struct.error) as exc:
            if attempt == retries:
                raise RuntimeError(f"{pair} {day.isoformat()} {hour:02d}h failed after {retries} attempts") from exc
            sleep_seconds = min(30.0, 1.5**attempt)
            logging.warning("%s %s %02dh retry %s/%s after %s", pair, day.isoformat(), hour, attempt, retries, exc)
            time.sleep(sleep_seconds)

    raise RuntimeError("unreachable")


def write_parquet_atomic(frame: pl.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=destination.stem, suffix=".tmp.parquet", delete=False, dir=destination.parent) as tmp:
        temp_path = Path(tmp.name)
    try:
        frame.write_parquet(temp_path, compression="zstd")
        os.replace(temp_path, destination)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def write_metadata_atomic(payload: dict[str, object], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temp_path, destination)


def write_completion_marker(pair: str, start_date: date, end_date: date) -> None:
    days = iter_dates(start_date, end_date)
    completed_days = 0
    total_rows = 0
    empty_days = 0
    for day in days:
        metadata_path = daily_metadata_path(pair, day)
        output_path = daily_tick_output_path(pair, day)
        if not metadata_path.exists() or not output_path.exists():
            raise RuntimeError(f"Cannot mark {pair} complete because {day.isoformat()} is missing output or metadata.")
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        completed_days += 1
        total_rows += int(payload.get("rows", 0))
        empty_days += 1 if payload.get("empty_day", False) else 0

    payload = {
        "pair": pair,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "expected_days": len(days),
        "completed_days": completed_days,
        "empty_days": empty_days,
        "total_rows": total_rows,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_atomic(payload, completion_marker_path(pair, start_date, end_date))
    write_metadata_atomic(payload, completion_marker_path(pair))


def download_one_day(
    pair: str,
    day: date,
    timeout_seconds: float,
    retries: int,
    force: bool,
) -> DownloadResult:
    output_path = daily_tick_output_path(pair, day)
    metadata_path = daily_metadata_path(pair, day)
    if output_path.exists() and metadata_path.exists() and not force:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        return DownloadResult(
            pair=pair,
            day=day.isoformat(),
            output_path=str(output_path),
            rows=int(payload.get("rows", 0)),
            hours_with_ticks=int(payload.get("hours_with_ticks", 0)),
            empty_day=bool(payload.get("empty_day", False)),
            skipped_existing=True,
        )

    session = requests.Session()
    session.headers.update({"User-Agent": "RazerBackTickPipeline/1.0"})
    all_rows: list[tuple[datetime, float, float, float, float, str]] = []
    hours_with_ticks = 0
    for hour in range(24):
        hour_rows = fetch_hour_ticks(session, pair, day, hour, timeout_seconds=timeout_seconds, retries=retries)
        if hour_rows:
            hours_with_ticks += 1
            all_rows.extend(hour_rows)

    if all_rows:
        frame = (
            pl.DataFrame(all_rows, schema=["timestamp", "bid", "ask", "bid_volume", "ask_volume", "pair"], orient="row")
            .sort("timestamp")
            .cast(TICK_SCHEMA)
        )
    else:
        frame = pl.DataFrame(schema=TICK_SCHEMA)
    write_parquet_atomic(frame, output_path)
    payload = {
        "pair": pair,
        "day": day.isoformat(),
        "output_path": str(output_path),
        "rows": frame.height,
        "hours_with_ticks": hours_with_ticks,
        "empty_day": frame.height == 0,
        "source": "dukascopy_hourly_bi5",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    write_metadata_atomic(payload, metadata_path)
    return DownloadResult(
        pair=pair,
        day=day.isoformat(),
        output_path=str(output_path),
        rows=frame.height,
        hours_with_ticks=hours_with_ticks,
        empty_day=frame.height == 0,
        skipped_existing=False,
    )


def main() -> None:
    args = parse_args()
    configure_logging()
    ensure_pipeline_dirs()

    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)
    if args.start_year is not None:
        start_date = date(args.start_year, 1, 1)
    if args.end_year is not None:
        end_date = date(args.end_year, 1, 1)
    if end_date < start_date:
        raise SystemExit("--end-date must be on or after --start-date")

    ordered_days = iter_dates(start_date, end_date)
    if args.date_order == "descending":
        ordered_days = list(reversed(ordered_days))

    tasks = [(pair, day) for pair in args.pairs for day in ordered_days]
    first_task = tasks[0] if tasks else None
    last_task = tasks[-1] if tasks else None
    logging.info(
        "Submitting %s pair/day downloads across %s workers order=%s first_task=%s last_task=%s",
        len(tasks),
        args.workers,
        args.date_order,
        first_task,
        last_task,
    )

    completed = 0
    total_rows = 0
    failures_by_pair: dict[str, list[str]] = {pair: [] for pair in args.pairs}
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(
                download_one_day,
                pair,
                day,
                timeout_seconds=args.timeout_seconds,
                retries=args.retries,
                force=args.force,
            ): (pair, day)
            for pair, day in tasks
        }
        for future in as_completed(future_map):
            pair, day = future_map[future]
            completed += 1
            try:
                result = future.result()
            except Exception as exc:  # pragma: no cover - defensive batch orchestration
                failures_by_pair[pair].append(day.isoformat())
                logging.error(
                    "FAIL %s %s error=%s (%s/%s)",
                    pair,
                    day.isoformat(),
                    exc,
                    completed,
                    len(tasks),
                )
                continue

            total_rows += result.rows
            status = "SKIP" if result.skipped_existing else "DONE"
            logging.info(
                "%s %s %s rows=%s hours=%s path=%s (%s/%s)",
                status,
                pair,
                day.isoformat(),
                result.rows,
                result.hours_with_ticks,
                result.output_path,
                completed,
                len(tasks),
            )

    for pair in args.pairs:
        if failures_by_pair[pair]:
            logging.error(
                "Did not mark %s complete; failed_days=%s",
                pair,
                ",".join(failures_by_pair[pair]),
            )
            continue
        write_completion_marker(pair, start_date, end_date)
        logging.info("Marked %s complete for %s -> %s", pair, start_date.isoformat(), end_date.isoformat())

    logging.info("Finished tick download batch. total_rows=%s tasks=%s", total_rows, len(tasks))
    if any(failures_by_pair.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
