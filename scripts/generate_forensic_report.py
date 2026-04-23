from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render a forensic markdown report from a RazerBack output directory.")
    parser.add_argument("--output-dir", default="output/full_15yr_run")
    parser.add_argument("--scenario-dir", action="append", default=[])
    return parser.parse_args()


def load_csv(output_dir: Path, filename: str) -> pd.DataFrame:
    path = output_dir / filename
    if not path.exists():
        raise SystemExit(f"Missing required artifact: {path}")
    return pd.read_csv(path)


def safe_markdown(frame: pd.DataFrame, columns: list[str]) -> str:
    available = [column for column in columns if column in frame.columns]
    if not available:
        return "_No rows available_"
    return frame[available].to_markdown(index=False)


def scenario_row(path: Path) -> dict[str, object]:
    if not path.exists():
        return {
            "scenario": path.name,
            "status": "not_run",
            "roi_pct": None,
            "sharpe": None,
            "max_dd_pct": None,
            "win_rate_pct": None,
            "profit_factor": None,
        }

    summary = pd.read_csv(path / "summary.csv").iloc[0]
    trade_stats = pd.read_csv(path / "trade_stats.csv").iloc[0]
    return {
        "scenario": path.name,
        "status": "complete",
        "roi_pct": float(summary["roi_pct"]),
        "sharpe": float(summary["calendar_weekly_sharpe_ann"]),
        "max_dd_pct": float(summary["max_drawdown_pct"]),
        "win_rate_pct": float(trade_stats["win_rate_pct"]),
        "profit_factor": float(trade_stats["profit_factor"]),
    }


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    report_path = output_dir / "forensic_report.md"

    summary = load_csv(output_dir, "summary.csv").iloc[0]
    yearly = load_csv(output_dir, "yearly_table.csv")
    module_table = load_csv(output_dir, "module_table.csv")
    trade_stats = load_csv(output_dir, "trade_stats.csv").iloc[0]
    run_config = pd.read_json(output_dir / "run_config.json", typ="series")
    run_name = str(run_config.get("name", output_dir.name))
    algorithm = str(run_config.get("algorithm", "legacy_continuation"))

    if len(yearly) > 0:
        first_year = int(yearly["year"].min())
        last_year = int(yearly["year"].max())
        span_years = max(1, last_year - first_year + 1)
    else:
        first_year = last_year = None
        span_years = 1

    cagr_pct = ((float(summary["ending_balance"]) / float(summary["start_balance"])) ** (1.0 / span_years) - 1.0) * 100.0
    scenarios = [output_dir, *[Path(path).resolve() for path in args.scenario_dir]]
    stress_df = pd.DataFrame([scenario_row(path) for path in scenarios])

    realism_lines = [
        "This run consumes quote-side bid/ask M1 parquet files generated from Dukascopy tick data.",
        "Midpoint parquet files are derived from the same bid/ask ticks and are used only for feature enrichment, not for execution-side fills.",
        "Execution in the current public RazerBack runtime is still an M1-bar simulator with next-bar quote-side entries and laddered exits.",
        "That means the run is materially more realistic than midpoint-only research, but it is not yet a full tick-path or order-book simulator.",
    ]

    lines = [
        f"# {run_name}: Forensic Audit",
        "",
        f"Source output directory: `{output_dir}`",
        "",
        "## Headline Performance",
        f"- Total ROI: `{summary['roi_pct']:.2f}%`",
        f"- CAGR: `{cagr_pct:.2f}%`",
        f"- Annualized weekly Sharpe: `{summary['calendar_weekly_sharpe_ann']:.3f}`",
        f"- Annualized weekly Sortino: `{summary['calendar_weekly_sortino_ann']:.3f}`",
        f"- Max drawdown: `{summary['max_drawdown_pct']:.2f}%`",
        f"- Win rate: `{trade_stats['win_rate_pct']:.2f}%`",
        f"- Profit factor: `{trade_stats['profit_factor']:.3f}`",
        f"- Trades: `{int(summary['portfolio_trades'])}`",
        "",
        "## Run Context",
        f"- Data dir: `{run_config['data_dir']}`",
        f"- Config path: `{run_config['config_path']}`",
        f"- Algorithm: `{algorithm}`",
        f"- Worker threads: `{run_config['workers']}`",
        "",
        "## Year-by-Year Returns",
        safe_markdown(yearly, ["year", "yearly_return_pct", "trades"]),
        "",
        "## Module Attribution",
        safe_markdown(
            module_table,
            ["module", "instrument", "side", "trades", "win_rate_pct", "total_pnl_dollars", "pnl_share_pct"],
        ),
        "",
        "## Realism Matrix",
        stress_df.to_markdown(index=False),
        "",
        "## Realism Statement",
    ]
    lines.extend([f"- {line}" for line in realism_lines])
    lines.append("")
    lines.append("## Notes")
    lines.append("- The realism matrix reports only completed scenario directories that already exist.")
    lines.append("- Execution scenarios currently change slippage, volatility sensitivity, entry delay, deterministic fill probability, and spread multiplier.")

    (output_dir / "realism_matrix.md").write_text(
        "# Realism Matrix\n\n" + stress_df.to_markdown(index=False) + "\n",
        encoding="utf-8",
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Saved forensic report to {report_path}")


if __name__ == "__main__":
    main()
