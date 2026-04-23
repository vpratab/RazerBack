# RazerBack

RazerBack is a clean standalone FX research and backtesting runtime for the locked continuation portfolio built on direct OANDA bid/ask M1 data.

This repo is structured for immediate use:

- fetch bid/ask candles from OANDA
- enrich midpoint data with Hawkes, GMM node, and local volatility features
- run the locked portfolio from one config file
- export a full artifact pack with trade ledger, NAV tables, and summary metrics

Maintainer:

- `Vinay Singh Shekhawat`

## What This Repo Contains

- `run_locked_portfolio.py`
  - single entrypoint for the productionized locked portfolio
- `configs/continuation_portfolio_total_v1.json`
  - locked strategy definition and runtime parameters
- `continuation_core.py`
  - signal generation, ladder exits, and portfolio accounting core
- `locked_portfolio_runtime.py`
  - config loading, artifact building, and export logic
- `fetch_oanda_bid_ask.py`
  - OANDA bid/ask M1 downloader
- `enrich_forex_research_data.py`
  - feature enrichment pipeline
- `realistic_backtest.py`
  - bid/ask data loader and shared execution-data helpers
- `reference_artifacts/`
  - current validated reference output for the locked portfolio
- `scripts/live_trading_engine.py`
  - OANDA practice/live execution engine with SQLite trade ledger output
- `scripts/generate_daily_tear_sheet.py`
  - daily institutional PDF tear sheet generator
- `scripts/generate_investor_report.py`
  - weekly/monthly investor report generator
- `scripts/register_production_tasks.ps1`
  - Windows scheduled-task registration for live trading and reporting
- `rust/fxbacktest_core`
  - optional PyO3/maturin Rust acceleration module

## Strategy Snapshot

The current locked portfolio is a four-module continuation book across:

- `GBP/USD`
- `USD/JPY`
- `GBP/JPY`

Reference result included in this repo:

- ROI: `185.48%`
- Max drawdown: `-8.83%`
- Win rate: `66.88%`
- Sharpe: `1.6778`
- Sortino: `1.3514`
- Trades: `154`

The locked configuration is in:

- [configs/continuation_portfolio_total_v1.json](configs/continuation_portfolio_total_v1.json)

The reference artifact pack is in:

- [reference_artifacts](reference_artifacts)

## Quick Start

### 1. Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

### 2. Fetch bid/ask data from OANDA

Example:

```bash
python3 fetch_oanda_bid_ask.py \
  --instrument GBP_USD \
  --from 2021-01-03T22:00:00Z \
  --to 2025-12-31T22:00:00Z \
  --output data/gbpusd_5yr_m1_bid_ask.parquet
```

Repeat for:

- `USD_JPY`
- `GBP_JPY`

If you also want the full research universe, fetch `EUR_USD` as well.

### 3. Put midpoint parquet files in `data/`

The runtime expects midpoint M1 parquet files named like:

- `gbpusd_5yr_m1.parquet`
- `usdjpy_5yr_m1.parquet`
- `gbpjpy_5yr_m1.parquet`

Those midpoint files are used for feature enrichment and signal generation.

### 4. Enrich the midpoint data

```bash
python3 enrich_forex_research_data.py --data-dir data
```

### 5. Run the locked portfolio

```bash
python3 run_locked_portfolio.py \
  --config configs/continuation_portfolio_total_v1.json \
  --data-dir data
```

Default output:

- `output/continuation_portfolio_total_v1`

## Common Commands

With `make`:

```bash
make install
make enrich DATA_DIR=data
make run DATA_DIR=data
```

## Output Pack

Each run exports:

- `summary.csv`
- `module_table.csv`
- `weekly_table.csv`
- `monthly_table.csv`
- `yearly_table.csv`
- `trade_stats.csv`
- `instrument_table.csv`
- `side_table.csv`
- `max_drawdown_point.csv`
- `trade_ledger.csv`
- `trade_ledger.parquet`
- `portfolio_report.md`

## Engineering Notes

- Signals are generated from enriched midpoint data.
- Entries execute at next-bar open on the correct side of the quote:
  - long on ask
  - short on bid
- Portfolio sizing uses realized balance at exit, not unrealized forward compounding.
- Weekly, monthly, and yearly returns are NAV-based.
- The runtime is clean and reproducible, but it is still an M1-bar simulator, not a full tick-level execution simulator.

## Production Add-On

The repo now includes a production-facing live stack for OANDA demo/live deployment:

- `scripts/live_trading_engine.py` for live signal generation and order placement
- `C:\fx_data\live\trades.db` as the trade-record ledger
- `scripts/generate_daily_tear_sheet.py` and `scripts/generate_investor_report.py` for institutional reporting
- `scripts/package_production_bundle.py` to assemble a single `RazerBack_Production` delivery folder

## Repo Hygiene

- `data/` is for local market data and is ignored by git.
- `output/` is for generated runs and is ignored by git.
- `reference_artifacts/` is tracked to preserve the validated locked-portfolio benchmark.

## Additional Docs

- [ARCHITECTURE.md](ARCHITECTURE.md)
- [reference_artifacts/README.md](reference_artifacts/README.md)
- [research/robustness_2026-04-23/README.md](research/robustness_2026-04-23/README.md)

## Research Workflows

The repo now includes a research-oriented robustness workflow on top of the execution/runtime stack:

- `scripts/run_multifamily_fx_research.py`
  - broad breakout/reversal search over the current bid/ask M1 surface
- `scripts/run_recent_weighted_portfolio_audit.py`
  - recent-window and weighted portfolio selection audit
- `scripts/run_breakout_ladder_exit_research.py`
  - exit-structure and laddering research for breakout modules
- `scripts/run_exact_tick_replay.py`
  - exact-tick replay of a selected module set against raw Dukascopy bid/ask quotes
- `scripts/run_pod_grade_audit.py`
  - metric/pod-style reproducibility audit using selected module, summary, and yearly inputs

The current clean research checkpoint is documented in:

- [research/robustness_2026-04-23/README.md](research/robustness_2026-04-23/README.md)
