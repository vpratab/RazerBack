from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path

import pandas as pd


DEFAULT_PAIRS = [
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "AUDUSD",
    "USDCAD",
    "USDCHF",
    "EURJPY",
    "EURGBP",
    "EURCHF",
    "AUDJPY",
    "GBPJPY",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit tick and M1 completeness for the local FX data surface.")
    parser.add_argument("--tick-root", default="C:/fx_data/tick")
    parser.add_argument("--m1-root", default="C:/fx_data/m1")
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent.parent / "output" / "data_audit"),
    )
    parser.add_argument("--pairs", nargs="+", default=DEFAULT_PAIRS, choices=DEFAULT_PAIRS)
    return parser.parse_args()


def expected_days(start: pd.Timestamp, end: pd.Timestamp) -> int:
    return max((end.normalize() - start.normalize()).days + 1, 1)


def audit_tick_pair(tick_root: Path, pair: str) -> dict[str, object]:
    files = sorted((tick_root / pair).glob("*/*.parquet"))
    if not files:
        return {
            "pair": pair,
            "tick_days": 0,
            "tick_start": "",
            "tick_end": "",
            "tick_expected_days": 0,
            "tick_day_coverage_pct": 0.0,
            "tick_years": "",
        }

    dates = pd.to_datetime([path.stem for path in files], utc=True)
    start = dates.min()
    end = dates.max()
    year_counts = pd.Series(dates.year).value_counts().sort_index()
    return {
        "pair": pair,
        "tick_days": int(len(files)),
        "tick_start": start.isoformat(),
        "tick_end": end.isoformat(),
        "tick_expected_days": expected_days(start, end),
        "tick_day_coverage_pct": float(len(files) / expected_days(start, end) * 100.0),
        "tick_years": ", ".join(f"{int(year)}:{int(count)}" for year, count in year_counts.items()),
    }


def audit_m1_pair(m1_root: Path, pair: str) -> dict[str, object]:
    path = m1_root / f"{pair.lower()}_5yr_m1_bid_ask.parquet"
    if not path.exists():
        return {
            "pair": pair,
            "m1_rows": 0,
            "m1_start": "",
            "m1_end": "",
            "m1_unique_days": 0,
            "m1_expected_days": 0,
            "m1_day_coverage_pct": 0.0,
            "m1_rows_per_present_day": 0.0,
            "m1_rows_per_span_day": 0.0,
            "m1_max_gap_minutes": 0.0,
        }

    ts = pd.to_datetime(pd.read_parquet(path, columns=["timestamp"])["timestamp"], utc=True).sort_values().reset_index(drop=True)
    start = ts.min()
    end = ts.max()
    unique_days = int(ts.dt.date.nunique())
    gaps = ts.diff().dt.total_seconds().div(60.0).fillna(1.0)
    return {
        "pair": pair,
        "m1_rows": int(len(ts)),
        "m1_start": start.isoformat(),
        "m1_end": end.isoformat(),
        "m1_unique_days": unique_days,
        "m1_expected_days": expected_days(start, end),
        "m1_day_coverage_pct": float(unique_days / expected_days(start, end) * 100.0),
        "m1_rows_per_present_day": float(len(ts) / max(unique_days, 1)),
        "m1_rows_per_span_day": float(len(ts) / max(expected_days(start, end), 1)),
        "m1_max_gap_minutes": float(gaps.max()),
    }


def overlap_summary(tick_rows: list[dict[str, object]], tick_root: Path) -> dict[str, object]:
    day_sets = {}
    for row in tick_rows:
        pair = str(row["pair"])
        start = row["tick_start"]
        end = row["tick_end"]
        if not start or not end:
            continue
        files = sorted((tick_root / pair).glob("*/*.parquet"))
        day_sets[pair] = {path.stem for path in files}

    if not day_sets:
        return {
            "all_pair_overlap_days": 0,
            "core_10_pair_overlap_days": 0,
        }

    pair_names = sorted(day_sets)
    overlap = set.intersection(*(day_sets[pair] for pair in pair_names))
    core_pairs = [pair for pair in pair_names if pair != "GBPJPY"]
    core_overlap = set.intersection(*(day_sets[pair] for pair in core_pairs))
    return {
        "all_pair_overlap_days": len(overlap),
        "core_10_pair_overlap_days": len(core_overlap),
    }


def main() -> None:
    args = parse_args()
    tick_root = Path(args.tick_root).expanduser().resolve()
    m1_root = Path(args.m1_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    tick_rows = [audit_tick_pair(tick_root, pair) for pair in args.pairs]
    m1_rows = [audit_m1_pair(m1_root, pair) for pair in args.pairs]

    tick_df = pd.DataFrame(tick_rows).sort_values("pair").reset_index(drop=True)
    m1_df = pd.DataFrame(m1_rows).sort_values("pair").reset_index(drop=True)
    combined = tick_df.merge(m1_df, on="pair", how="outer")
    summary = overlap_summary(tick_rows, tick_root)

    tick_df.to_csv(output_dir / "tick_coverage.csv", index=False)
    m1_df.to_csv(output_dir / "m1_coverage.csv", index=False)
    combined.to_csv(output_dir / "combined_coverage.csv", index=False)

    report_lines = [
        "# Data Completeness Audit",
        "",
        "## Overlap Summary",
        "",
        f"- All 11 pairs overlap days: `{summary['all_pair_overlap_days']}`",
        f"- Core 10-pair overlap days: `{summary['core_10_pair_overlap_days']}`",
        "",
        "## Tick Coverage",
        "",
        combined[
            [
                "pair",
                "tick_days",
                "tick_day_coverage_pct",
                "tick_start",
                "tick_end",
                "m1_rows",
                "m1_unique_days",
                "m1_day_coverage_pct",
                "m1_rows_per_present_day",
                "m1_max_gap_minutes",
            ]
        ].to_string(index=False),
        "",
    ]
    (output_dir / "audit_report.md").write_text("\n".join(report_lines), encoding="utf-8")
    print(output_dir)


if __name__ == "__main__":
    main()
