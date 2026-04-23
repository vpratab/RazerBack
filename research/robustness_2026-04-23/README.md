# Robustness Checkpoint: 2026-04-23

This folder captures the current honest research checkpoint for the live FX robustness work.

## Scope

- Data source: Dukascopy bid/ask quote ticks aggregated to bid/ask M1 plus enriched midpoint features.
- Current fully tested years in the honesty pack: `2011-2013` and `2020-2025`.
- Missing continuous tested years: `2014-2019`.
- Meaning: this is a strong launch-research checkpoint, not yet a full continuous 2011-2025 pod-grade claim.

## Current Safest Launch Candidate

Label:

- `balanced_plus_usdcad_h13`
- overlay: `annual6_month4`

Module set:

- `audjpy_long_bo_h15_rr1.15_dist2.5_wickmaxna_retge20_zna_sl35_tp105_shield79-26_ttl60`
- `eurjpy_short_bo_h5_rr1.25_dist2.5_wickmaxna_retle-2.5_z2_sl25_tp50_shield38-0_ttl120`
- `eurusd_short_bo_h12_rr1.6_dist5_wickmaxna_retle-2.5_z1_sl35_tp26_shield13-3_ttl60`
- `eurusd_short_bo_h15_rr1_dist5_wickmax5_retle-20_z2_sl35_tp52_shield35-12_ttl180`
- `usdjpy_long_bo_h8_rr1.25_dist5_wickmaxna_retna_z1_sl30_tp30_shield20-4_ttl120`
- `usdjpy_long_bo_h9_rr1.15_dist5_wickmax10_retna_z1_sl30_tp22_shield11-2_ttl60`
- `usdcad_short_bo_h13_rr1.4_dist1_wickmaxna_retle-20_z2_sl40_tp40_shield16-3_ttl60`

Key metrics:

| scope | roi_pct | sharpe_ann | max_drawdown_pct | win_rate_pct | profit_factor | trades |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2025 M1 | 20.7957 | 1.9903 | -3.7649 | 70.5882 | 2.9950 | 34 |
| 2025 exact tick | 22.0243 | 2.1027 | -3.6925 | 70.5882 | 3.1766 | 34 |
| 2022-2025 avg | 37.7825 | 2.3923 | n/a | n/a | n/a | n/a |
| worst tested year | -4.1535 | n/a | -8.4216 | n/a | n/a | n/a |

Interpretation:

- This remains the cleanest launch candidate because it keeps the older tested tail inside a tolerable range.
- The baseline exact-tick replay passed without missing windows and slightly improved versus the M1 path model.

## Strongest Add-On Candidate

Candidate:

- baseline above plus `eurjpy_short_bo_h10_rr1.15_dist2.5_wickmax10_retle-2.5_z1.5_sl20_tp20_shield8-3_ttl180`

Key metrics:

| scope | roi_pct | sharpe_ann | max_drawdown_pct | win_rate_pct | profit_factor | trades |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2025 M1 | 29.0186 | 2.3761 | -3.7649 | 73.6842 | 3.6427 | 38 |
| 2025 exact tick | 30.3309 | 2.4765 | -3.6925 | 73.6842 | 3.8459 | 38 |
| 2022-2025 avg | 35.4703 | 2.4137 | n/a | n/a | n/a | n/a |
| worst tested year | -5.3058 | n/a | -8.4216 | n/a | n/a | n/a |

Interpretation:

- This is the strongest incremental add-on candidate so far.
- It survived exact-tick replay cleanly.
- It improves 2025 materially, but it still worsens the older tested tail versus the protected baseline, so it is not yet a clean replacement.

## Multifamily All-10 Sweep

The best finished all-10-pair multifamily sweep produced a diversified book that was useful as an idea source but not good enough to replace the launch baseline.

Key metrics:

| portfolio | tested_window | roi_pct | sharpe_ann | max_drawdown_pct | win_rate_pct | profit_factor | trades |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| diversified | 2020-2025 | 239.8796 | 1.6992 | -8.3107 | 59.1837 | 2.0413 | 833 |
| best-by-pair | 2020-2025 | 234.9961 | 1.5298 | -10.6554 | 56.1798 | 1.6411 | 1246 |

Why it was rejected as the main launch book:

- win rate stayed below the desired launch zone
- drawdown sat too close to or above the risk ceiling
- the sweep was a strong research lead, not a safer production replacement

## Pod-Grade Interpretation

Current status:

- metric gate: `PASS`
- final pod gate: `FAIL`

Why final pod gate still fails:

- continuous `2011-2025` yearly testing is incomplete because `2014-2019` are not yet in the validated tested sample
- Dukascopy harvest is still in progress, so selected instruments are not yet complete across the full requested range
- full exact-tick replay has only been completed for the current 2025 launch window, not every filled trade across the full requested horizon
- the current launch book was chosen with 2025 visible, so it is a 2026 launch candidate rather than a pristine untouched-2025 walk-forward claim

## Reproduction Scripts

These scripts are the repo-clean versions of the local robustness work:

- `scripts/run_exact_tick_replay.py`
- `scripts/run_pod_grade_audit.py`

Example exact-tick replay for the strongest add-on candidate:

```bash
python scripts/run_exact_tick_replay.py ^
  --data-dir C:\fx_data\m1 ^
  --tick-root C:\fx_data\tick ^
  --overlay annual6_month4 ^
  --annual-stop -6 ^
  --monthly-stop -4 ^
  --year 2025 ^
  --module audjpy_long_bo_h15_rr1.15_dist2.5_wickmaxna_retge20_zna_sl35_tp105_shield79-26_ttl60 ^
  --module eurjpy_short_bo_h5_rr1.25_dist2.5_wickmaxna_retle-2.5_z2_sl25_tp50_shield38-0_ttl120 ^
  --module eurusd_short_bo_h12_rr1.6_dist5_wickmaxna_retle-2.5_z1_sl35_tp26_shield13-3_ttl60 ^
  --module eurusd_short_bo_h15_rr1_dist5_wickmax5_retle-20_z2_sl35_tp52_shield35-12_ttl180 ^
  --module usdjpy_long_bo_h8_rr1.25_dist5_wickmaxna_retna_z1_sl30_tp30_shield20-4_ttl120 ^
  --module usdjpy_long_bo_h9_rr1.15_dist5_wickmax10_retna_z1_sl30_tp22_shield11-2_ttl60 ^
  --module usdcad_short_bo_h13_rr1.4_dist1_wickmaxna_retle-20_z2_sl40_tp40_shield16-3_ttl60 ^
  --module eurjpy_short_bo_h10_rr1.15_dist2.5_wickmax10_retle-2.5_z1.5_sl20_tp20_shield8-3_ttl180 ^
  --output-dir output/research/exact_tick_add_on_candidate
```

Example pod-grade audit using curated module/yearly/summary inputs:

```bash
python scripts/run_pod_grade_audit.py ^
  --modules-file research/robustness_2026-04-23/selected_modules_baseline.csv ^
  --summary-csv research/robustness_2026-04-23/selected_metrics_baseline.csv ^
  --yearly-csv research/robustness_2026-04-23/selected_yearly_baseline.csv ^
  --exact-replay-report output/research/exact_tick_baseline/exact_tick_replay_report.md ^
  --output-dir output/research/pod_grade_audit_baseline
```
