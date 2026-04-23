from __future__ import annotations

import argparse
import json
from pathlib import Path

from locked_portfolio_runtime import run_locked_portfolio
from fxbacktest.strategies.v_sentinel import run_v_sentinel_portfolio


def default_config_path(algorithm: str) -> Path:
    base = Path(__file__).resolve().parent / "configs"
    if algorithm == "v_sentinel":
        return base / "v_sentinel_portfolio.json"
    return base / "continuation_portfolio_total_v1.json"


def resolve_algorithm_choice(config_path: Path, requested: str) -> str:
    if requested != "auto":
        return requested
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return "legacy_continuation"
    algorithm = str(raw.get("algorithm", "legacy_continuation")).strip().lower()
    if algorithm == "v_sentinel":
        return "v_sentinel"
    return "legacy_continuation"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the locked continuation portfolio and export the full artifact pack.")
    parser.add_argument("--config")
    parser.add_argument("--data-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--start-balance", type=float)
    parser.add_argument("--risk-pct", type=float)
    parser.add_argument("--max-leverage", type=float)
    parser.add_argument("--max-concurrent", type=int)
    parser.add_argument("--workers", type=int)
    parser.add_argument("--algorithm", choices=("auto", "v_sentinel", "legacy_continuation"), default="auto")
    parser.add_argument("--scenario", choices=("base", "conservative", "hard"), default="base")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = Path(args.config) if args.config else default_config_path("legacy_continuation" if args.algorithm == "auto" else args.algorithm)
    resolved_algorithm = resolve_algorithm_choice(config_path, args.algorithm)

    runner = run_v_sentinel_portfolio if resolved_algorithm == "v_sentinel" else run_locked_portfolio
    output_dir = runner(
        config_path=config_path,
        data_dir_override=Path(args.data_dir) if args.data_dir else None,
        output_dir_override=Path(args.output_dir) if args.output_dir else None,
        start_balance=args.start_balance,
        risk_pct=args.risk_pct,
        max_leverage=args.max_leverage,
        max_concurrent=args.max_concurrent,
        workers=args.workers,
        scenario=args.scenario,
    )
    print(output_dir)


if __name__ == "__main__":
    main()
