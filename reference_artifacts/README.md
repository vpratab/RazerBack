# Reference Artifacts

This directory contains the validated benchmark outputs for the locked portfolio in this repo.

These files are intentionally committed so another engineer can:

- compare a fresh run against the known-good result
- verify that the runtime still reproduces the same trade set and performance profile
- spot regressions after code changes

The current benchmark corresponds to the locked configuration in:

- `configs/continuation_portfolio_total_v1.json`

Headline benchmark numbers:

- ROI: `185.48%`
- Max drawdown: `-8.83%`
- Win rate: `66.88%`
- Sharpe: `1.6778`
- Sortino: `1.3514`
- Trades: `154`

Primary files:

- `summary.csv`
- `module_table.csv`
- `monthly_table.csv`
- `yearly_table.csv`
- `trade_stats.csv`
- `trade_ledger.csv`
- `trade_ledger.parquet`

These are reference outputs, not the default destination for new runs. New generated outputs should go under `output/`.
