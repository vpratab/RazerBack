from __future__ import annotations

import argparse
import shutil
from pathlib import Path


FILES_TO_COPY = [
    ".env.example",
    "ARCHITECTURE.md",
    "BENCHMARK.md",
    "continuation_core.py",
    "enrich_forex_research_data.py",
    "live_reporting.py",
    "locked_portfolio_runtime.py",
    "logging_utils.py",
    "native_acceleration.py",
    "oanda_client.py",
    "realistic_backtest.py",
    "requirements.txt",
    "run_locked_portfolio.py",
]

DIRS_TO_COPY = [
    "configs",
    "rust",
    "scripts",
    "tests",
]


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Assemble the RazerBack_Production delivery folder.")
    parser.add_argument("--output-dir", default=str(repo_root / "RazerBack_Production"))
    return parser.parse_args()


def write_readme(path: Path) -> None:
    body = """# RazerBack Production

This folder packages the production-facing RazerBack stack:

- historical Dukascopy tick-to-M1 pipeline
- enriched locked-portfolio backtester
- optional Rust acceleration hooks via `rust/fxbacktest_core`
- live OANDA trading engine
- SQLite trade ledger and institutional report generators
- Windows scheduled-task registration scripts

## Quick Deploy

1. Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

2. Optional Rust build:

```powershell
cd rust\\fxbacktest_core
maturin develop --release
```

3. Set credentials and live settings:

- copy `.env.example` into your preferred environment loader
- set `OANDA_API_TOKEN`
- set `OANDA_ENVIRONMENT=practice` or `live`
- optionally set `OANDA_ACCOUNT_ID`

4. Run the live engine:

```powershell
python scripts\\live_trading_engine.py --config configs\\continuation_portfolio_total_v1.json --data-dir C:\\fx_data\\m1 --live-root C:\\fx_data\\live
```

5. Register unattended Windows tasks:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\\register_production_tasks.ps1
```

## Core Outputs

- `C:\\fx_data\\live\\trades.db`: authoritative trade ledger
- `C:\\fx_data\\live\\reports\\daily\\*.pdf`: tear sheets
- `C:\\fx_data\\live\\reports\\investor\\*.md` and `*.pdf`: investor reports
- `output\\full_15yr_*`: scenario backtest outputs

## Interpretation

- `run_locked_portfolio.py` remains the primary offline validation entrypoint.
- `scripts\\live_trading_engine.py` uses the same locked module definitions for live signal generation.
- `native_acceleration.py` automatically falls back to Python when the Rust extension is unavailable.
- `BENCHMARK.md` records the current acceleration status and the benchmark procedure.
"""
    path.write_text(body, encoding="utf-8")


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    output_dir = Path(args.output_dir).resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for relative in FILES_TO_COPY:
        source = repo_root / relative
        if source.exists():
            destination = output_dir / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)

    for relative in DIRS_TO_COPY:
        source = repo_root / relative
        if source.exists():
            shutil.copytree(source, output_dir / relative, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))

    daily_reports = sorted((Path("C:/fx_data/live/reports/daily")).glob("*.pdf"), key=lambda path: path.stat().st_mtime, reverse=True)
    if daily_reports:
        shutil.copy2(daily_reports[0], output_dir / "tear_sheet.pdf")

    write_readme(output_dir / "README.md")
    print(output_dir)


if __name__ == "__main__":
    main()
