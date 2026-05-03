# Slippage Scaling Bugfix Milestone

Generated: 2026-05-03T01:31:34.831999+00:00

## What was wrong

The first proxy execution-realism studies were overstating slippage pain for low-weight overlays because the portfolio scaler was shrinking `pnl_pips` along with `pnl_dollars`. That is not physically correct: position sizing changes dollar P&L and notional, but it does not change the underlying market move in pips. In the realism code path, this bug caused lower-weight modules to be charged too much slippage relative to their weighted notional.

## Fix

I corrected the scaling logic in:

- `locked_validation_usdjpy_followup_overlay_20260502.py`
- `locked_validation_realism_proxy_gauntlet_20260502.py`

The corrected rule is:

- scale `pnl_dollars`, `units`, and `usd_notional`
- preserve `pnl_pips` as a market-path quantity
- derive slippage dollars from weighted dollar-per-pip instead of shrinking the path itself

## Corrected result

After rerunning the slippage-aware reweight probe on the frozen EURGBP/USDJPY shortlist:

- structures tested: `38`
- evaluated weight rows: `46,016`
- feasible overlays at `0.5` pip per side: `268`

Best corrected proxy-execution-robust Validation overlay:

- Modules: `eurgbp_a|usdjpy_z15_t60`
- Weights: `eurgbp_a=2.5`, `usdjpy_z15_t60=0.05`
- Sharpe: `2.7015`
- Max drawdown: `-7.10%`
- Profit factor: `2.6324`
- Positive years: `3/3`
- Yearly PnL: `2020 +$243.57`, `2021 +$0.74`, `2022 +$30.20`

## Event proxy follow-through

I also checked the feasible set under the `0.5` pip + `1.5x` event-blowout proxy:

- passing overlays: `266`
- the same best simple duo still passes

## Interpretation

This materially changes the mission read.

The earlier execution-realism blocker was partly an artifact of the scaling bug. The corrected evidence now shows that the frozen EURGBP/USDJPY shortlist already contains a substantial set of slippage-aware Validation overlays that clear the proxy shape gate.

## Next action

Freeze the corrected execution-robust candidate book and move from proxy realism toward the stronger realism/tick-replay path, rather than reopening Discovery prematurely.
