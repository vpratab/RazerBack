# Architecture

## Purpose

This repo is the standalone runtime for the locked continuation portfolio, separated from the broader research workspace.

It is designed for:

- reproducible backtests
- clean handoff to another engineer or desk
- easy re-run against refreshed OANDA data

It is not designed as a full live execution stack.

## Flow

### 1. Raw data

`fetch_oanda_bid_ask.py` downloads OANDA M1 bid/ask candles and stores:

- `open_bid`, `high_bid`, `low_bid`, `close_bid`
- `open_ask`, `high_ask`, `low_ask`, `close_ask`

### 2. Enrichment

`enrich_forex_research_data.py` reads the midpoint M1 files and writes:

- `ask_lambda`
- `bid_lambda`
- `imbalance_ratio`
- `node_mean`
- `distance_to_node_pips`
- `local_sigma`

### 3. Strategy core

`continuation_core.py` contains:

- `ContinuationSpec`
- dataset loading
- module signal generation
- ladder exit simulation
- portfolio-level realized-balance accounting

### 4. Locked runtime

`locked_portfolio_runtime.py` contains:

- config loading
- locked module normalization
- artifact generation
- export of summary, NAV tables, and trade ledger

### 5. Entry point

`run_locked_portfolio.py` is the one-command runner used in normal operation.

## Locked Portfolio Contract

The current locked portfolio is defined entirely by:

- `configs/continuation_portfolio_total_v1.json`

That file controls:

- modules
- risk per trade
- max leverage
- max concurrency
- default data and output directories

## Data Contract

Required local files for the locked portfolio:

- `gbpusd_5yr_m1.parquet`
- `usdjpy_5yr_m1.parquet`
- `gbpjpy_5yr_m1.parquet`
- `gbpusd_5yr_m1_bid_ask.parquet`
- `usdjpy_5yr_m1_bid_ask.parquet`
- `gbpjpy_5yr_m1_bid_ask.parquet`

The midpoint parquet files must already contain the enriched feature columns.

## Output Contract

Each locked run exports:

- summary
- module contribution table
- weekly, monthly, yearly NAV tables
- trade stats
- instrument and side concentration tables
- max drawdown point
- full trade ledger as CSV and Parquet

## Realism Boundary

This runtime includes:

- direct bid/ask M1 data
- correct quote-side entries
- exit-based cash accounting
- NAV-based period returns

This runtime does not include:

- full tick-by-tick sequencing
- latency modeling
- queue-position modeling
- explicit financing or swap
- broker reject semantics

That means it is clean and serious research infrastructure, but not a full execution simulator.
