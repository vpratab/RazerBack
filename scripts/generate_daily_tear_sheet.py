from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from live_reporting import daily_pnl, ensure_live_paths, equity_curve, load_trades, performance_summary


def default_portfolio_output(repo_root: Path) -> Path:
    full_run = repo_root / "output" / "full_15yr_base"
    if (full_run / "summary.csv").exists():
        return full_run
    return repo_root / "output" / "sample_scenario_base"


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Generate the daily institutional tear sheet PDF.")
    parser.add_argument("--live-root", default="C:/fx_data/live")
    parser.add_argument("--portfolio-output", default=str(default_portfolio_output(repo_root)))
    parser.add_argument("--date", default=pd.Timestamp.now(tz="America/New_York").date().isoformat())
    return parser.parse_args()


def rolling_ratios(pnl_df: pd.DataFrame) -> pd.DataFrame:
    if pnl_df.empty:
        return pd.DataFrame(columns=["date", "rolling_sharpe", "rolling_sortino"])
    frame = pnl_df.copy()
    values = frame["pnl_dollars"].astype(float)
    rolling_mean = values.rolling(30).mean()
    rolling_std = values.rolling(30).std(ddof=0).replace(0.0, np.nan)
    downside_std = values.where(values < 0.0).rolling(30).std(ddof=0).replace(0.0, np.nan)
    frame["rolling_sharpe"] = (rolling_mean / rolling_std).fillna(0.0)
    frame["rolling_sortino"] = (rolling_mean / downside_std).fillna(0.0)
    return frame[["date", "rolling_sharpe", "rolling_sortino"]]


def safe_table(frame: pd.DataFrame, columns: list[str], max_rows: int = 10) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame([{column: "" for column in columns}])
    available = [column for column in columns if column in frame.columns]
    return frame[available].head(max_rows).copy()


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    live_paths = ensure_live_paths(Path(args.live_root))
    report_date = pd.Timestamp(args.date)
    output_path = live_paths.reports_daily / f"{report_date.date().isoformat()}_tear_sheet.pdf"
    trades = load_trades(live_paths.db_path)
    pnl_by_day = daily_pnl(trades)
    ratios = rolling_ratios(pnl_by_day)
    equity = equity_curve(trades)
    summary = performance_summary(trades)

    positions = []
    if live_paths.state_path.exists():
        state = json.loads(live_paths.state_path.read_text(encoding="utf-8"))
        positions = list(state.get("managed_trades", {}).values())

    today_trades = pd.DataFrame()
    if not trades.empty:
        today_trades = trades[
            pd.to_datetime(trades["exit_time"], utc=True).dt.tz_convert("America/New_York").dt.date == report_date.date()
        ]

    backtest_summary_path = Path(args.portfolio_output) / "summary.csv"
    backtest_summary = pd.read_csv(backtest_summary_path).iloc[0] if backtest_summary_path.exists() else None

    with PdfPages(output_path) as pdf:
        fig, axes = plt.subplots(2, 2, figsize=(14, 9))

        ax = axes[0, 0]
        ax.set_title("Equity Curve")
        if equity["timestamp"].notna().any():
            ax.plot(pd.to_datetime(equity["timestamp"], utc=True), equity["equity"], color="#1f77b4", linewidth=2.0)
        else:
            ax.text(0.5, 0.5, "No live trades yet", ha="center", va="center", transform=ax.transAxes)
        ax.grid(alpha=0.2)

        ax = axes[0, 1]
        ax.set_title("Daily P&L / Rolling 30D Ratios")
        if not pnl_by_day.empty:
            ax.bar(pd.to_datetime(pnl_by_day["date"]), pnl_by_day["pnl_dollars"], color="#4c9f70", alpha=0.7, label="Daily P&L")
            ax2 = ax.twinx()
            ax2.plot(pd.to_datetime(ratios["date"]), ratios["rolling_sharpe"], color="#c44e52", label="30D Sharpe")
            ax2.plot(pd.to_datetime(ratios["date"]), ratios["rolling_sortino"], color="#8172b2", label="30D Sortino")
            ax2.legend(loc="upper right")
        else:
            ax.text(0.5, 0.5, "No daily P&L yet", ha="center", va="center", transform=ax.transAxes)
        ax.grid(alpha=0.2)

        ax = axes[1, 0]
        ax.axis("off")
        headline = [
            f"Date: {report_date.date().isoformat()}",
            f"Trade count: {int(summary['trade_count'])}",
            f"Win rate: {summary['win_rate_pct']:.2f}%",
            f"Profit factor: {summary['profit_factor']:.3f}",
            f"Max drawdown: {summary['max_drawdown_pct']:.2f}%",
            f"Total P&L: ${summary['total_pnl_dollars']:.2f}",
        ]
        if backtest_summary is not None:
            headline.extend(
                [
                    "",
                    "Backtest anchor:",
                    f"ROI {backtest_summary['roi_pct']:.2f}%",
                    f"Sharpe {backtest_summary['calendar_weekly_sharpe_ann']:.3f}",
                    f"Max DD {backtest_summary['max_drawdown_pct']:.2f}%",
                ]
            )
        ax.text(0.01, 0.98, "\n".join(headline), va="top", family="monospace")

        ax = axes[1, 1]
        ax.axis("off")
        positions_df = pd.DataFrame(positions)
        positions_table = safe_table(positions_df, ["module", "instrument", "side", "entry_price", "remaining_units", "ttl_at"], max_rows=8)
        trades_table = safe_table(today_trades, ["instrument", "side", "pnl_dollars", "pnl_pips", "exit_time"], max_rows=8)
        text_lines = ["Open Positions", positions_table.to_string(index=False), "", "Today's Trades", trades_table.to_string(index=False)]
        ax.text(0.01, 0.98, "\n".join(text_lines), va="top", family="monospace")

        fig.suptitle("RazerBack Daily Tear Sheet", fontsize=16)
        fig.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

    print(output_path)


if __name__ == "__main__":
    main()
