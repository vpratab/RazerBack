from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "modern_window_research"
DEFAULT_BASELINE_CONFIG = REPO_ROOT / "configs" / "continuation_portfolio_total_v1.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the FX research stack on a date-filtered modern snapshot.")
    parser.add_argument("--data-dir", default="C:/fx_data/m1")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2025-12-31")
    parser.add_argument("--scenario", choices=("base", "conservative", "hard"), default="hard")
    parser.add_argument("--continuation-samples", type=int, default=160)
    parser.add_argument("--reversal-samples", type=int, default=64)
    parser.add_argument("--breakout-samples", type=int, default=96)
    parser.add_argument("--portfolio-top-portfolios", type=int, default=5)
    parser.add_argument("--portfolio-beam-width", type=int, default=20)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--baseline-config", default=str(DEFAULT_BASELINE_CONFIG))
    return parser.parse_args()


def run_step(name: str, command: list[str], cwd: Path) -> None:
    print(f"[run_modern_window_research] starting {name}")
    subprocess.run(command, cwd=cwd, check=True)
    print(f"[run_modern_window_research] finished {name}")


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_dir = output_dir / "snapshot"
    continuation_dir = output_dir / "continuation"
    multifamily_dir = output_dir / "multifamily"
    portfolio_dir = output_dir / "portfolio_factory"

    build_snapshot_script = REPO_ROOT / "scripts" / "build_date_filtered_snapshot.py"
    continuation_script = REPO_ROOT / "scripts" / "run_universe_continuation_research.py"
    multifamily_script = REPO_ROOT / "scripts" / "run_multifamily_fx_research.py"
    portfolio_script = REPO_ROOT / "scripts" / "run_portfolio_factory_research.py"

    run_step(
        "snapshot",
        [
            sys.executable,
            str(build_snapshot_script),
            "--source-dir",
            str(Path(args.data_dir).expanduser().resolve()),
            "--output-dir",
            str(snapshot_dir),
            "--start-date",
            args.start_date,
            "--end-date",
            args.end_date,
        ],
        REPO_ROOT,
    )

    run_step(
        "continuation",
        [
            sys.executable,
            str(continuation_script),
            "--data-dir",
            str(snapshot_dir),
            "--output-dir",
            str(continuation_dir),
            "--scenario",
            args.scenario,
            "--samples-per-pair-side",
            str(args.continuation_samples),
            "--workers",
            str(args.workers),
        ],
        REPO_ROOT,
    )

    run_step(
        "multifamily",
        [
            sys.executable,
            str(multifamily_script),
            "--data-dir",
            str(snapshot_dir),
            "--output-dir",
            str(multifamily_dir),
            "--reversal-samples-per-pair-side",
            str(args.reversal_samples),
            "--breakout-samples-per-pair-side",
            str(args.breakout_samples),
            "--workers",
            str(args.workers),
        ],
        REPO_ROOT,
    )

    portfolio_command = [
        sys.executable,
        str(portfolio_script),
        "--data-dir",
        str(snapshot_dir),
        "--output-dir",
        str(portfolio_dir),
        "--continuation-csv",
        str(continuation_dir / "all_candidates.csv"),
        "--multifamily-csv",
        str(multifamily_dir / "all_candidates.csv"),
        "--scenario",
        args.scenario,
        "--beam-width",
        str(args.portfolio_beam_width),
        "--top-portfolios",
        str(args.portfolio_top_portfolios),
    ]
    baseline_config = Path(args.baseline_config).expanduser().resolve()
    if baseline_config.exists():
        portfolio_command.extend(["--baseline-config", str(baseline_config)])
    run_step("portfolio_factory", portfolio_command, REPO_ROOT)

    summary_payload = {
        "start_date": args.start_date,
        "end_date": args.end_date,
        "scenario": args.scenario,
        "snapshot_dir": str(snapshot_dir),
        "continuation_dir": str(continuation_dir),
        "multifamily_dir": str(multifamily_dir),
        "portfolio_dir": str(portfolio_dir),
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    print(output_dir)


if __name__ == "__main__":
    main()
