from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from fx_pipeline_config import ALL_PAIRS, M1_ROOT, MODELS_ROOT, PIP_SIZES, ensure_pipeline_dirs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an hour-of-week spread and liquidity lookup from M1 bid/ask parquet files.")
    parser.add_argument("--pairs", nargs="+", default=ALL_PAIRS, choices=ALL_PAIRS)
    parser.add_argument("--output", default=str(MODELS_ROOT / "spread_lookup.parquet"))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ensure_pipeline_dirs()

    frames: list[pl.LazyFrame] = []
    for pair in args.pairs:
        path = M1_ROOT / f"{pair.lower()}_5yr_m1_bid_ask.parquet"
        if not path.exists():
            continue
        pip_size = PIP_SIZES[pair]
        frames.append(
            pl.scan_parquet(str(path)).select(
                [
                    "timestamp",
                    pl.lit(pair.lower()).alias("pair"),
                    ((pl.col("close_ask") - pl.col("close_bid"))).alias("spread_price"),
                    ((pl.col("close_ask") - pl.col("close_bid")) / pip_size).alias("spread_pips"),
                    "volume_bid",
                    "volume_ask",
                    "tick_count",
                ]
            )
        )

    if not frames:
        raise SystemExit("No M1 bid/ask parquet files found under C:/fx_data/m1")

    model = (
        pl.concat(frames)
        .with_columns(
            [
                pl.col("timestamp").dt.hour().alias("hour_utc"),
                pl.col("timestamp").dt.weekday().alias("weekday"),
            ]
        )
        .group_by(["pair", "hour_utc", "weekday"])
        .agg(
            [
                pl.col("spread_price").median().alias("median_spread_price"),
                pl.col("spread_price").quantile(0.90).alias("p90_spread_price"),
                pl.col("spread_pips").median().alias("median_spread_pips"),
                pl.col("spread_pips").quantile(0.90).alias("p90_spread_pips"),
                pl.col("volume_bid").median().alias("median_bid_volume"),
                pl.col("volume_ask").median().alias("median_ask_volume"),
                pl.col("tick_count").median().alias("median_tick_count"),
                pl.len().alias("sample_minutes"),
            ]
        )
        .sort(["pair", "weekday", "hour_utc"])
        .collect()
    )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    model.write_parquet(output_path, compression="zstd")
    print(f"Saved spread model to {output_path}")


if __name__ == "__main__":
    main()
