# continuation_portfolio_total_v1

Locked continuation portfolio based on the corrected 185.48% ROI / -8.83% max drawdown frontier point.

## Summary

- ROI: `185.4824%`
- Ending balance: `$4282.24`
- Max drawdown: `-8.8284%`
- Win rate: `66.8831%`
- Profit factor: `2.0317`
- Annualized weekly Sharpe: `1.6778`
- Annualized weekly Sortino: `1.3514`
- Trades: `154`

## Run Config

- Config: `/Users/giansingh/Documents/GitHub/RazerBack/configs/continuation_portfolio_total_v1.json`
- Data dir: `/Users/giansingh/Documents/GitHub/razorback`
- Start balance: `$1500.00`
- Risk per trade: `2.90%`
- Max leverage: `35.00`
- Max concurrent: `2`
- Worker threads: `4`

## Artifacts

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

## Selected Modules

- `usdjpy_short_h12-15_rr1.25_imb2.5_node3_ret5_sig1_sl30_tp20-40-60_frac0.3-0.3-0.3_trail15_ttl180`
- `gbpusd_short_h12-15_rr1.25_imb4_node3_ret5_sig1_sl25_tp25-50-75_frac0.4-0.3-0.2_trail20_ttl180`
- `gbpjpy_short_h12-15_rr1.25_imb3.5_node3_ret10_sig1.05_sl30_tp25-50-75_frac0.4-0.3-0.2_trail20_ttl240`
- `gbpusd_long_h12-15_rr1.25_imb4_node2_ret20_sig1_sl20_tp20-40-60_frac0.3-0.3-0.3_trail15_ttl240`

## Notes

- This is the clean locked-portfolio runtime, not the broad candidate search.
- It uses the corrected cash accounting and NAV-based period returns from the continuation research engine.
