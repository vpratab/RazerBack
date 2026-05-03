from __future__ import annotations

import argparse
from pathlib import Path
import sys

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.fx_pipeline_config import ALL_PAIRS
from scripts.pipeline_utilities import LOCKED_SPLITS, summarize_split_coverage


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate the locked discovery/validation/final-test data split against M1 parquet files.")
    parser.add_argument("--data-dir", default="C:/fx_data/m1")
    parser.add_argument("--bid-ask", action="store_true")
    parser.add_argument("--instruments", nargs="+", default=[pair.lower() for pair in ALL_PAIRS])
    parser.add_argument("--output-dir")
    return parser.parse_args()


def build_markdown_report(summary: pd.DataFrame, data_dir: Path, bid_ask: bool) -> str:
    lines = [
        "# Data Split Validation",
        "",
        f"- Data directory: `{data_dir}`",
        f"- Surface: `{'bid_ask' if bid_ask else 'midpoint'}`",
        "- Locked splits:",
    ]
    for split_name, split in LOCKED_SPLITS.items():
        lines.append(f"  - `{split_name}`: `{split.start.isoformat()}` to `{split.end.isoformat()}`")
    lines.extend(["", "## Coverage Summary", "", summary.to_markdown(index=False), ""])
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir)
    summary = summarize_split_coverage(
        instruments=args.instruments,
        data_dir=data_dir,
        bid_ask=bool(args.bid_ask),
    )
    summary["all_locked_splits_present"] = summary[
        [f"{split_name}_nonempty" for split_name in LOCKED_SPLITS if split_name != "live_forward"]
    ].all(axis=1)

    print(summary.to_string(index=False))

    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        summary.to_csv(output_dir / "split_validation.csv", index=False)
        report = build_markdown_report(summary=summary, data_dir=data_dir, bid_ask=bool(args.bid_ask))
        (output_dir / "split_validation.md").write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
