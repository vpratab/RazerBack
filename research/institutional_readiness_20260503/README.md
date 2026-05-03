# RazerBack Institutionalization Mission Breakdown

## What This Is

This folder is the GitHub-ready record of the autonomous institutional-readiness mission that took the RazerBack FX research stack from a loose prototype to a statistically disciplined, execution-aware, holdout-governed platform.

It includes:

- the final institutional report
- the frozen candidate package that was eligible for Final Test
- the stronger Validation execution-realism checks
- the one-shot Final Test runner used to consume the untouched holdout exactly once
- the key milestone notes that explain how the result evolved

The outcome is simple and important:

**The platform infrastructure improved a lot. The final candidate did not pass the untouched Final Test. The correct fundability verdict is `No`.**

## Executive Summary

### Best combo that reached the Final Test

The final frozen candidate was a **2-module / 2-pair** book:

- `eurgbp_a`
  - `eurgbp_long_bo_h8_rr1.25_dist5_wickmax5_retge20_z2_sl20_tp15_shield11-6_ttl60`
  - weight `2.5`
- `usdjpy_z15_t60`
  - `usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z1.5_sl30_tp45_shield22-7_ttl60`
  - weight `0.05`

This is the combo that ultimately won the locked Validation and pre-Final-Test realism process.

Important distinction:

- an earlier **3-module** Validation seed existed
- the **final frozen candidate actually spent on the untouched Final Test** was this **2-module EURGBP + USDJPY duo**

### End result

That final combo **failed** the untouched Final Test.

Exact-tick Final Test result:

- ROI: `-10.3461%`
- Sharpe: `-1.1746`
- Max drawdown: `-15.11%`
- Profit factor: `0.4685`
- Positive years: `1/3`

So the final answer from this mission is:

- **best combo found:** the EURGBP / USDJPY duo above
- **end result:** not robust enough
- **fundability verdict:** `No`

## Mission Goal

The mission was to answer one institutional question honestly:

Can RazerBack produce a statistically robust, execution-aware, diversified FX portfolio that survives a locked Discovery / Validation / Final Test process and is strong enough to justify live deployment?

The answer from this cycle is:

- **Discovery:** yes, we found real candidate structure
- **Validation:** yes, we built a frozen candidate that looked strong
- **Final Test:** no, the frozen candidate collapsed on the untouched holdout

That means this cycle produced a better platform, but **not** a fundable strategy.

## The Locked Research Protocol

The mission enforced a strict three-way quarantine:

- `Discovery`: `2011-01-01` to `2019-12-31`
- `Validation`: `2020-01-01` to `2022-12-31`
- `Final Test`: `2023-01-01` to `2025-12-31`

Rules that governed the whole process:

- search and tuning only on Discovery
- Validation used once for filtering and portfolio assembly
- Final Test consumed exactly once at the end
- no retuning after the Final Test
- holdout failure ends the cycle

## What We Built

### 1. Security and operational hardening

We added the plumbing required to treat this like a real platform instead of a lab notebook:

- credential scan and env-var based secret handling
- `.env.template`
- stronger `.gitignore`
- live engine duplicate-process lock file
- safer engine startup behavior

Relevant code:

- [`scripts/rotate_and_secure_credentials.py`](../../scripts/rotate_and_secure_credentials.py)
- [`scripts/live_trading_engine.py`](../../scripts/live_trading_engine.py)
- [`.env.template`](../../.env.template)
- [`.gitignore`](../../.gitignore)

### 2. Locked data split and reproducible loading

We formalized split-aware loading and split validation instead of relying on ad hoc date filters:

- split definitions in pipeline utilities
- split validator
- split snapshot builder
- physically frozen Discovery / Validation / Final Test parquet surfaces

Relevant code:

- [`scripts/pipeline_utilities.py`](../../scripts/pipeline_utilities.py)
- [`scripts/validate_data_split.py`](../../scripts/validate_data_split.py)
- [`scripts/build_split_snapshot.py`](../../scripts/build_split_snapshot.py)

### 3. Statistical evaluation framework

We built a reusable evaluation layer that stopped the search from rewarding raw in-sample beauty:

- annualized Sharpe
- max drawdown
- profit factor
- monthly / yearly return views
- daily PnL correlations
- walk-forward analysis
- Deflated Sharpe Ratio (DSR)

Relevant code:

- [`scripts/evaluation_framework.py`](../../scripts/evaluation_framework.py)

### 4. Research engine and candidate discovery upgrades

We improved the research stack itself:

- fixed cross-pair USD conversion logic
- widened pair coverage in the research scripts
- added better discovery surfaces and follow-up probes
- built targeted diversification rescue probes
- built frozen-surface direct runners when the older miners were too brittle

Relevant code and core fixes:

- [`continuation_core.py`](../../continuation_core.py)
- [`scripts/run_multifamily_fx_research.py`](../../scripts/run_multifamily_fx_research.py)
- [`scripts/run_universe_continuation_research.py`](../../scripts/run_universe_continuation_research.py)

### 5. Validation overlay and execution realism tooling

We built a proper pre-Final-Test candidate path:

- Validation seed portfolio builder
- correlation-aware shortlist logic
- slippage-aware reweight search
- exact-tick Validation candidate check
- frozen candidate package creator
- one-shot Final Test runner

Relevant mission scripts copied into this bundle:

- [`scripts/locked_validation_usdjpy_followup_overlay_20260502.py`](scripts/locked_validation_usdjpy_followup_overlay_20260502.py)
- [`scripts/locked_validation_slippage_reweight_probe_20260502.py`](scripts/locked_validation_slippage_reweight_probe_20260502.py)
- [`scripts/locked_validation_exact_candidate_check_20260503.py`](scripts/locked_validation_exact_candidate_check_20260503.py)
- [`scripts/prepare_pre_final_test_candidate_package_20260503.py`](scripts/prepare_pre_final_test_candidate_package_20260503.py)
- [`scripts/locked_final_test_one_shot_runner_20260503.py`](scripts/locked_final_test_one_shot_runner_20260503.py)

## How We Did It

### Phase A: Freeze the surfaces

We validated the underlying tick and M1 archive, then created physical split-constrained research surfaces for Discovery, Validation, and Final Test. That eliminated silent leakage risk and gave us identifiable snapshot IDs.

### Phase B: Gate Discovery honestly

A broad first-pass Discovery sweep produced no robust survivors when all `176` candidates were forced through walk-forward and DSR gating. That was an important truth signal, not a tooling failure.

Then we ran targeted refinement:

- **EURGBP** emerged as the deepest valid Discovery basin
- **USDCHF** looked promising at first but ended up structurally blocked
- **USDJPY** eventually became the best non-EURGBP diversification lane

### Phase C: Filter on Validation

The targeted EURGBP basin produced:

- `151` Discovery survivors
- `74` Validation-profitable candidates
- `35` distinct profitable daily-PnL paths

That was the first sign of real depth, but correlation and currency concentration rules reduced the practical shortlist dramatically.

USDJPY follow-up probes then produced a real second-pair Discovery survivor that stayed profitable on Validation and had effectively zero daily-PnL correlation to the EURGBP legs.

### Phase D: Build the frozen candidate

The strongest frozen Validation candidate before Final Test was a **two-module / two-pair** book:

- `eurgbp_a`
  - `eurgbp_long_bo_h8_rr1.25_dist5_wickmax5_retge20_z2_sl20_tp15_shield11-6_ttl60`
  - weight `2.5`
- `usdjpy_z15_t60`
  - `usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z1.5_sl30_tp45_shield22-7_ttl60`
  - weight `0.05`

This was frozen into a lineage-rich package before the holdout was touched.

### Phase E: Stress Validation under execution realism

Before spending the Final Test, we put the frozen book through stronger Validation checks.

After fixing a real slippage-scaling bug in the overlay studies, the corrected Validation evidence looked strong:

- proxy slippage-aware Sharpe: `2.7015`
- proxy slippage-aware PF: `2.6324`
- positive Validation years: `3/3`

Then the stronger exact Validation candidate check also held up:

- exact-tick Validation ROI: `21.52%`
- exact-tick Validation Sharpe: `3.0194`
- exact-tick Validation PF: `2.9946`
- positive Validation years: `3/3`

It even remained strong under:

- `2s` entry delay
- `+0.3` pip spread

That was the point where the candidate became worth spending the Final Test on.

### Phase F: Consume the Final Test exactly once

We armed a one-shot runner that refused to touch the holdout until explicitly executed and that cannot be run twice once the marker file is written.

That runner consumed the Final Test exactly once.

## Final Result

The untouched Final Test failed decisively.

### Exact-tick Final Test

- ROI: `-10.3461%`
- Sharpe: `-1.1746`
- Max drawdown: `-15.11%`
- Profit factor: `0.4685`
- Trades: `91`
- Positive years: `1/3`

### Exact-tick yearly breakdown

- `2023`: positive
- `2024`: slightly negative
- `2025`: strongly negative

### M1 weighted Final Test

- ROI: `-6.7224%`
- Sharpe: `-0.2486`
- Max drawdown: `-27.58%`
- Profit factor: `0.9353`
- Positive years: `1/3`

### Friction sensitivity on Final Test

The exact-tick failure did not recover under harder execution variants:

- `2s` delay: still negative
- `+0.3` pip spread: still negative

## What The Result Means

This cycle produced two different kinds of output:

### What worked

- the platform is much more credible than before
- the split discipline worked
- the holdout controls worked
- the lineage and packaging worked
- the exact-tick and slippage validation tools worked
- the security and engine hardening work is useful and reusable

### What did not work

- the best candidate from this cycle was **not robust enough**
- strong Validation behavior did **not** transfer to the untouched Final Test
- this candidate should **not** be deployed
- this candidate should **not** be retuned after the spent Final Test

## Fundability Verdict

**No**

This specific frozen candidate is **not fundable**.

It failed the untouched Final Test on the exact metrics that matter to an allocator:

- Sharpe
- profit factor
- drawdown containment
- positive returns in each calendar year

## Most Important End Result

The most important thing we built is **not** a winning strategy. It is a platform and process that can now fail honestly.

That matters because before this mission the stack could produce attractive-looking research outputs without strong guarantees that:

- the data split was enforced end to end
- the execution assumptions were stress-tested correctly
- the final holdout would be consumed only once
- the resulting verdict would stop the process instead of drifting into post-hoc retuning

Now it can.

That is the real institutional improvement in this repo.

## What’s In This Folder

### Main write-ups

- [`artifacts/RAZERBACK_INSTITUTIONAL_READINESS_REPORT_v2_20260503.md`](artifacts/RAZERBACK_INSTITUTIONAL_READINESS_REPORT_v2_20260503.md)
- [`artifacts/final_test_report.md`](artifacts/final_test_report.md)
- [`artifacts/candidate_package.md`](artifacts/candidate_package.md)

### Key machine-readable artifacts

- [`artifacts/candidate_package.json`](artifacts/candidate_package.json)
- [`artifacts/final_test_summary.json`](artifacts/final_test_summary.json)
- [`artifacts/validation_exact_summary.json`](artifacts/validation_exact_summary.json)
- [`artifacts/validation_slippage_summary.json`](artifacts/validation_slippage_summary.json)
- [`artifacts/platform_health.json`](artifacts/platform_health.json)

### Important milestone notes

- [`artifacts/pre_final_test_freeze_package_milestone_20260503.md`](artifacts/pre_final_test_freeze_package_milestone_20260503.md)
- [`artifacts/slippage_scaling_bugfix_milestone_20260503.md`](artifacts/slippage_scaling_bugfix_milestone_20260503.md)
- [`artifacts/final_test_failure_milestone_20260503.md`](artifacts/final_test_failure_milestone_20260503.md)

### Research and execution scripts

- [`scripts/prepare_pre_final_test_candidate_package_20260503.py`](scripts/prepare_pre_final_test_candidate_package_20260503.py)
- [`scripts/locked_final_test_one_shot_runner_20260503.py`](scripts/locked_final_test_one_shot_runner_20260503.py)
- [`scripts/locked_validation_exact_candidate_check_20260503.py`](scripts/locked_validation_exact_candidate_check_20260503.py)
- [`scripts/locked_validation_slippage_reweight_probe_20260502.py`](scripts/locked_validation_slippage_reweight_probe_20260502.py)
- [`scripts/locked_validation_usdjpy_followup_overlay_20260502.py`](scripts/locked_validation_usdjpy_followup_overlay_20260502.py)

## Practical Next Step

If RazerBack continues from here, the next cycle should **not** retune this spent candidate. It should start a new research cycle with the improved platform controls intact and a fresh search emphasis on:

- broader diversification families
- less dependence on the EURGBP / USDJPY structure that just failed
- the same strict split, DSR, walk-forward, execution-realism, and one-shot holdout process
