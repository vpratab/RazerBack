# RAZERBACK INSTITUTIONAL READINESS REPORT v2

Generated: 2026-05-03T03:19:34.468485+00:00

## Executive Summary

- Best combo that survived Discovery, Validation, and pre-Final-Test realism:
  - `eurgbp_long_bo_h8_rr1.25_dist5_wickmax5_retge20_z2_sl20_tp15_shield11-6_ttl60` at weight `2.5`
  - `usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z1.5_sl30_tp45_shield22-7_ttl60` at weight `0.05`
- This was the **final frozen candidate book**. An earlier 3-module Validation seed existed, but this 2-module EURGBP/USDJPY duo is the one that actually consumed the untouched Final Test.
- End result:
  - exact-tick Final Test ROI `-10.3461%`
  - exact-tick Final Test Sharpe `-1.1746`
  - exact-tick Final Test profit factor `0.4685`
  - positive years `1/3`
- Final verdict: **No**, not fundable

## Data Split

- Discovery: `2011-01-01` to `2019-12-31`
- Validation: `2020-01-01` to `2022-12-31`
- Final Test: `2023-01-01` to `2025-12-31`

## Snapshot IDs

- Discovery: `discovery_snapshot_20260501_112155`
- Validation: `validation_snapshot_20260501_165111`
- Final Test: `final_test_snapshot_20260501_165111`

## Final Frozen Modules

### eurgbp_a

- Module: `eurgbp_long_bo_h8_rr1.25_dist5_wickmax5_retge20_z2_sl20_tp15_shield11-6_ttl60`
- Weight: `2.5`
- Discovery walk-forward: `6/8` positive windows
- Discovery DSR: `0.8880`
- Validation Sharpe: `7.9022`
- Validation profit factor: `3.6646`
- Validation trades: `9`

### usdjpy_z15_t60

- Module: `usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z1.5_sl30_tp45_shield22-7_ttl60`
- Weight: `0.05`
- Discovery walk-forward: `6/8` positive windows
- Discovery DSR: `1.0000`
- Validation Sharpe: `1.9692`
- Validation profit factor: `1.3007`
- Validation trades: `51`

## Pre-Final-Test Validation Evidence

- Proxy realism book Sharpe: `2.7015`
- Proxy realism book PF: `2.6324`
- Proxy realism positive years: `3/3`
- Exact Validation Sharpe: `3.0194`
- Exact Validation PF: `2.9946`
- Exact Validation positive years: `3/3`

## Final Test Metrics

### M1 Weighted

- ROI: `-6.7224%`
- Sharpe: `-0.2486`
- Max drawdown: `-0.2758`
- Profit factor: `0.9353`
- Trades: `91`
- Positive years: `1/3`

### Exact Tick

- ROI: `-10.3461%`
- Sharpe: `-1.1746`
- Max drawdown: `-0.1511`
- Profit factor: `0.4685`
- Trades: `91`
- Positive years: `1/3`

### Exact Tick + 2s Delay

- ROI: `-7.0591%`
- Sharpe: `-0.7152`
- Max drawdown: `-0.1456`
- Profit factor: `0.6346`
- Trades: `91`
- Positive years: `1/3`

### Exact Tick + 0.3 Pip Spread

- ROI: `-10.9172%`
- Sharpe: `-1.2286`
- Max drawdown: `-0.1542`
- Profit factor: `0.4471`
- Trades: `91`
- Positive years: `1/3`

## Final Test Exact-Tick Calendar Years

- `2023`: trades `31`, PnL `$70.72`, pips `87.70`
- `2024`: trades `29`, PnL `$-4.56`, pips `-48.70`
- `2025`: trades `31`, PnL `$-221.35`, pips `-49.60`

## Live Demo Summary

- Not deployed to the OANDA practice account.
- Mission rules require stopping after a failed untouched Final Test rather than deploying or retuning this candidate.

## Platform Health Snapshot

- Data integrity: green
- Security / ops: green
- Research credibility: red after Final Test
- Portfolio quality: red after Final Test
- Execution realism: red after Final Test

## FUNDABILITY VERDICT

**No**

The frozen candidate package is not fundable. It failed the untouched Final Test on every institutional success criterion that matters: annualized Sharpe, profit factor, drawdown containment, and positive returns in each calendar year.

## Next Actions

- Do not retune or redeploy this Final-Test-spent candidate package.
- Archive this package as a failed holdout candidate with full lineage preserved.
- Start a new research cycle only with new Discovery/Validation work and a future untouched holdout, preferably emphasizing broader diversification families rather than the same EURGBP/USDJPY structure.
- Keep the security and data-lineage controls that are now in place; they worked correctly even though the alpha did not.
