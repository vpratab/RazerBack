from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from continuation_core import REQUIRED_ENRICHED_COLUMNS
from fx_pipeline_config import AGGREGATION_STATE_ROOT, ALL_PAIRS, DOWNLOAD_STATE_ROOT, M1_ROOT


DEFAULT_START_DATE = "2011-01-01"
DEFAULT_END_DATE = "2026-01-01"
DEFAULT_SCENARIOS = ("base", "conservative", "hard")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report machine-readable status for the nonstop FX pipeline.")
    parser.add_argument("--pairs", nargs="+", default=ALL_PAIRS, choices=ALL_PAIRS)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--end-date", default=DEFAULT_END_DATE)
    parser.add_argument("--repo-root", default=r"C:\Users\saanvi\Documents\GitHub\RazerBack")
    return parser.parse_args()


def completion_marker_path(pair: str, start_date: str | None = None, end_date: str | None = None) -> Path:
    if start_date is not None and end_date is not None:
        return DOWNLOAD_STATE_ROOT / pair / f"_complete_{start_date}_{end_date}.json"
    return DOWNLOAD_STATE_ROOT / pair / "_complete.json"


def bid_ask_path(pair: str) -> Path:
    return M1_ROOT / f"{pair.lower()}_5yr_m1_bid_ask.parquet"


def midpoint_path(pair: str) -> Path:
    return M1_ROOT / f"{pair.lower()}_5yr_m1.parquet"


def read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def latest_tick_mtime(pair: str) -> float | None:
    tick_files = list((Path(r"C:\fx_data\tick") / pair).glob("*/*.parquet"))
    if not tick_files:
        return None
    return max(file.stat().st_mtime for file in tick_files)


def is_download_complete(pair: str, start_date: str, end_date: str) -> bool:
    candidate_paths = [
        completion_marker_path(pair, start_date, end_date),
        completion_marker_path(pair),
    ]
    for marker_path in candidate_paths:
        if not marker_path.exists():
            continue
        payload = read_json(marker_path)
        if (
            payload.get("start_date") == start_date
            and payload.get("end_date") == end_date
            and int(payload.get("expected_days", -1)) == int(payload.get("completed_days", -2))
        ):
            return True
    return False


def m1_is_up_to_date(pair: str) -> bool:
    bid_ask = bid_ask_path(pair)
    midpoint = midpoint_path(pair)
    latest_tick = latest_tick_mtime(pair)
    if latest_tick is None or not bid_ask.exists() or not midpoint.exists():
        return False
    latest_output = min(bid_ask.stat().st_mtime, midpoint.stat().st_mtime)
    return latest_output >= latest_tick


def enrichment_complete(pair: str) -> bool:
    path = midpoint_path(pair)
    if not path.exists():
        return False
    schema = pq.read_schema(path)
    names = set(schema.names)
    return all(column in names for column in REQUIRED_ENRICHED_COLUMNS)


def scenario_output_complete(repo_root: Path, scenario: str) -> bool:
    output_dir = repo_root / "output" / f"full_15yr_{scenario}"
    required = ["summary.csv", "trade_stats.csv", "run_config.json", "portfolio_report.md"]
    return all((output_dir / filename).exists() for filename in required)


def report_complete(repo_root: Path) -> bool:
    base_output = repo_root / "output" / "full_15yr_base"
    return (base_output / "forensic_report.md").exists() and (base_output / "realism_matrix.md").exists()


def current_phase(summary: dict[str, object]) -> str:
    if not summary["download_complete"]:
        return "download"
    if not summary["m1_complete"]:
        return "aggregate"
    if not summary["enrichment_complete"]:
        return "enrich"
    if not summary["scenarios_complete"]:
        return "simulate"
    if not summary["report_complete"]:
        return "report"
    return "complete"


def build_summary(args: argparse.Namespace) -> dict[str, object]:
    repo_root = Path(args.repo_root).resolve()
    pair_rows: list[dict[str, object]] = []
    for pair in args.pairs:
        pair_rows.append(
            {
                "pair": pair,
                "download_complete": is_download_complete(pair, args.start_date, args.end_date),
                "m1_up_to_date": m1_is_up_to_date(pair),
                "enrichment_complete": enrichment_complete(pair),
                "latest_tick_mtime": latest_tick_mtime(pair),
            }
        )

    download_complete = all(row["download_complete"] for row in pair_rows)
    m1_complete = all(row["m1_up_to_date"] for row in pair_rows)
    enrichment_done = all(row["enrichment_complete"] for row in pair_rows)
    scenarios_complete = all(scenario_output_complete(repo_root, scenario) for scenario in DEFAULT_SCENARIOS)
    summary = {
        "generated_at_utc": datetime.utcnow().isoformat() + "Z",
        "pairs": pair_rows,
        "download_complete": download_complete,
        "m1_complete": m1_complete,
        "enrichment_complete": enrichment_done,
        "scenarios_complete": scenarios_complete,
        "report_complete": report_complete(repo_root),
        "repo_root": str(repo_root),
    }
    summary["phase"] = current_phase(summary)
    return summary


def main() -> None:
    args = parse_args()
    print(json.dumps(build_summary(args), indent=2))


if __name__ == "__main__":
    main()
