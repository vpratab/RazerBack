from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from live_reporting import attribution_table, ensure_live_paths, load_trades, monthly_returns, performance_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the weekly/monthly investor report.")
    parser.add_argument("--live-root", default="C:/fx_data/live")
    parser.add_argument("--label", default=pd.Timestamp.now(tz="America/New_York").date().isoformat())
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    live_paths = ensure_live_paths(Path(args.live_root))
    trades = load_trades(live_paths.db_path)
    summary = performance_summary(trades)
    month_table = monthly_returns(trades)
    instrument_table = attribution_table(trades, "instrument")
    side_table = attribution_table(trades, "side")

    lines = [
        f"# RazerBack Investor Report ({args.label})",
        "",
        "## Performance Summary",
        f"- Trades: {int(summary['trade_count'])}",
        f"- Win rate: {summary['win_rate_pct']:.2f}%",
        f"- Profit factor: {summary['profit_factor']:.3f}",
        f"- Total P&L: ${summary['total_pnl_dollars']:.2f}",
        f"- Max drawdown: {summary['max_drawdown_pct']:.2f}%",
        f"- Sharpe-like: {summary['sharpe_like']:.3f}",
        f"- Sortino-like: {summary['sortino_like']:.3f}",
        "",
        "## Monthly Returns",
        month_table.to_markdown(index=False) if not month_table.empty else "_No monthly returns yet_",
        "",
        "## Instrument Attribution",
        instrument_table.to_markdown(index=False) if not instrument_table.empty else "_No instrument attribution yet_",
        "",
        "## Side Attribution",
        side_table.to_markdown(index=False) if not side_table.empty else "_No side attribution yet_",
    ]

    markdown_path = live_paths.reports_weekly / f"{args.label}_investor_report.md"
    pdf_path = live_paths.reports_weekly / f"{args.label}_investor_report.pdf"
    markdown_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with PdfPages(pdf_path) as pdf:
        fig = plt.figure(figsize=(11, 8.5))
        ax = fig.add_subplot(111)
        ax.axis("off")
        ax.text(0.01, 0.99, "\n".join(lines), va="top", family="monospace")
        fig.tight_layout()
        pdf.savefig(fig)
        plt.close(fig)

    print(markdown_path)
    print(pdf_path)


if __name__ == "__main__":
    main()
