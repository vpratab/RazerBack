from __future__ import annotations

import argparse
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from fx_pipeline_config import ALL_PAIRS, LOG_ROOT, M1_ROOT, TICK_ROOT, ensure_pipeline_dirs


M1_FRAGMENT_ROOT = M1_ROOT / "_daily_bid_ask"
BID_ASK_COLUMNS = [
    "timestamp",
    "pair",
    "open_bid",
    "high_bid",
    "low_bid",
    "close_bid",
    "open_ask",
    "high_ask",
    "low_ask",
    "close_ask",
    "volume_bid",
    "volume_ask",
    "tick_count",
]
MIDPOINT_RAW_COLUMNS = [
    "timestamp",
    "pair",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "tick_count",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate daily Dukascopy tick parquet files into M1 bid/ask and midpoint parquet files.")
    parser.add_argument("--pairs", nargs="+", default=ALL_PAIRS, choices=ALL_PAIRS)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def configure_logging() -> None:
    ensure_pipeline_dirs()
    log_path = LOG_ROOT / "aggregate_ticks_to_m1.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def fragment_path(pair: str, day_stem: str) -> Path:
    return M1_FRAGMENT_ROOT / pair / f"{day_stem}.parquet"


def bid_ask_output_path(pair: str) -> Path:
    return M1_ROOT / f"{pair.lower()}_5yr_m1_bid_ask.parquet"


def midpoint_output_path(pair: str) -> Path:
    return M1_ROOT / f"{pair.lower()}_5yr_m1.parquet"


def write_lazy_frame_atomic(frame: pl.LazyFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=destination.stem, suffix=".tmp.parquet", delete=False, dir=destination.parent) as tmp:
        temp_path = Path(tmp.name)
    try:
        frame.sink_parquet(str(temp_path), compression="zstd")
        os.replace(temp_path, destination)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def aggregate_daily_ticks(tick_path: Path, pair: str) -> pl.DataFrame:
    tick_frame = pl.read_parquet(tick_path)
    if tick_frame.is_empty():
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime(time_unit="us", time_zone="UTC"),
                "pair": pl.Utf8,
                "open_bid": pl.Float64,
                "high_bid": pl.Float64,
                "low_bid": pl.Float64,
                "close_bid": pl.Float64,
                "open_ask": pl.Float64,
                "high_ask": pl.Float64,
                "low_ask": pl.Float64,
                "close_ask": pl.Float64,
                "open_mid": pl.Float64,
                "high_mid": pl.Float64,
                "low_mid": pl.Float64,
                "close_mid": pl.Float64,
                "volume_bid": pl.Float64,
                "volume_ask": pl.Float64,
                "tick_count": pl.UInt32,
            }
        )

    return (
        tick_frame
        .sort("timestamp")
        .with_columns(
            [
                ((pl.col("bid") + pl.col("ask")) / 2.0).alias("mid"),
                pl.lit(pair).alias("pair"),
            ]
        )
        .group_by_dynamic("timestamp", every="1m", period="1m", closed="left", label="left")
        .agg(
            [
                pl.col("pair").first().alias("pair"),
                pl.col("bid").first().alias("open_bid"),
                pl.col("bid").max().alias("high_bid"),
                pl.col("bid").min().alias("low_bid"),
                pl.col("bid").last().alias("close_bid"),
                pl.col("ask").first().alias("open_ask"),
                pl.col("ask").max().alias("high_ask"),
                pl.col("ask").min().alias("low_ask"),
                pl.col("ask").last().alias("close_ask"),
                pl.col("mid").first().alias("open_mid"),
                pl.col("mid").max().alias("high_mid"),
                pl.col("mid").min().alias("low_mid"),
                pl.col("mid").last().alias("close_mid"),
                pl.col("bid_volume").sum().alias("volume_bid"),
                pl.col("ask_volume").sum().alias("volume_ask"),
                pl.len().cast(pl.UInt32).alias("tick_count"),
            ]
        )
        .sort("timestamp")
    )


def rebuild_if_needed(pair: str, destination: Path, source_paths: list[Path], frame_builder: callable, force: bool) -> bool:
    if not source_paths:
        return False
    latest_source = max(path.stat().st_mtime for path in source_paths)
    if destination.exists() and destination.stat().st_mtime >= latest_source and not force:
        return False
    write_lazy_frame_atomic(frame_builder(source_paths, destination), destination)
    return True


def build_bid_ask_frame(source_paths: list[Path], destination: Path) -> pl.LazyFrame:
    return (
        pl.scan_parquet([str(path) for path in source_paths])
        .select(BID_ASK_COLUMNS)
        .sort("timestamp")
    )


def build_midpoint_frame(source_paths: list[Path], destination: Path) -> pl.LazyFrame:
    raw_frame = (
        pl.scan_parquet([str(path) for path in source_paths])
        .select(
            [
                "timestamp",
                "pair",
                pl.col("open_mid").alias("open"),
                pl.col("high_mid").alias("high"),
                pl.col("low_mid").alias("low"),
                pl.col("close_mid").alias("close"),
                (pl.col("volume_bid") + pl.col("volume_ask")).alias("volume"),
                "tick_count",
            ]
        )
        .sort("timestamp")
    )
    if not destination.exists():
        return raw_frame

    existing_columns = pq.read_schema(destination).names
    extra_columns = [column for column in existing_columns if column not in MIDPOINT_RAW_COLUMNS]
    if not extra_columns:
        return raw_frame

    existing_extras = (
        pl.scan_parquet(str(destination))
        .select(["timestamp", *extra_columns])
        .sort("timestamp")
        .unique(subset=["timestamp"], keep="last")
    )
    return raw_frame.join(existing_extras, on="timestamp", how="left")


def main() -> None:
    args = parse_args()
    configure_logging()
    ensure_pipeline_dirs()

    for pair in args.pairs:
        pair_tick_paths = sorted((TICK_ROOT / pair).glob("*/*.parquet"))
        if not pair_tick_paths:
            logging.info("No tick parquet files yet for %s", pair)
            continue

        built_fragments = 0
        fragment_paths: list[Path] = []
        for tick_path in pair_tick_paths:
            day_stem = tick_path.stem
            target = fragment_path(pair, day_stem)
            fragment_paths.append(target)
            if target.exists() and target.stat().st_mtime >= tick_path.stat().st_mtime and not args.force:
                continue
            daily_bars = aggregate_daily_ticks(tick_path, pair)
            target.parent.mkdir(parents=True, exist_ok=True)
            daily_bars.write_parquet(target, compression="zstd")
            built_fragments += 1

        rebuilt_bid_ask = rebuild_if_needed(
            pair,
            bid_ask_output_path(pair),
            fragment_paths,
            build_bid_ask_frame,
            args.force,
        )
        rebuilt_midpoint = rebuild_if_needed(
            pair,
            midpoint_output_path(pair),
            fragment_paths,
            build_midpoint_frame,
            args.force,
        )
        logging.info(
            "%s fragments_built=%s bid_ask_rebuilt=%s midpoint_rebuilt=%s source_days=%s",
            pair,
            built_fragments,
            rebuilt_bid_ask,
            rebuilt_midpoint,
            len(pair_tick_paths),
        )

    logging.info("Finished tick -> M1 aggregation at %s", datetime.now(timezone.utc).isoformat())


if __name__ == "__main__":
    main()
