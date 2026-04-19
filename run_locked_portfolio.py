from __future__ import annotations

import argparse
from pathlib import Path

from locked_portfolio_runtime import run_locked_portfolio


def default_config_path() -> Path:
    return Path(__file__).resolve().parent / "configs" / "continuation_portfolio_total_v1.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the locked continuation portfolio and export the full artifact pack.")
    parser.add_argument("--config", default=str(default_config_path()))
    parser.add_argument("--data-dir")
    parser.add_argument("--output-dir")
    parser.add_argument("--start-balance", type=float)
    parser.add_argument("--risk-pct", type=float)
    parser.add_argument("--max-leverage", type=float)
    parser.add_argument("--max-concurrent", type=int)
    parser.add_argument("--workers", type=int)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = run_locked_portfolio(
        config_path=Path(args.config),
        data_dir_override=Path(args.data_dir) if args.data_dir else None,
        output_dir_override=Path(args.output_dir) if args.output_dir else None,
        start_balance=args.start_balance,
        risk_pct=args.risk_pct,
        max_leverage=args.max_leverage,
        max_concurrent=args.max_concurrent,
        workers=args.workers,
    )
    print(output_dir)


if __name__ == "__main__":
    main()
