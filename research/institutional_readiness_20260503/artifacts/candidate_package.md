# Frozen Pre-Final-Test Candidate Package

Generated: 2026-05-03T02:45:43.045949+00:00
Candidate package id: `locked_pre_final_test_candidate_package_20260503`

## Frozen Book

- Candidate book: `C:\Users\saanvi\Documents\Codex\2026-04-20-what-i-need-from-the-new\locked_validation_slippage_reweight_probe_20260502\best_execution_robust_candidate_book.json`
- Validation snapshot: `validation_snapshot_20260501_165111`
- Final Test snapshot: `final_test_snapshot_20260501_165111`
- Final Test runner: `C:\Users\saanvi\Documents\Codex\2026-04-20-what-i-need-from-the-new\locked_final_test_one_shot_runner_20260503.py`
- Holdout status: `untouched`

## Modules

### eurgbp_a

- Module: `eurgbp_long_bo_h8_rr1.25_dist5_wickmax5_retge20_z2_sl20_tp15_shield11-6_ttl60`
- Weight: `2.5`
- Discovery gate: `6/8` windows, DSR `0.8880282761280041`
- Validation: Sharpe `7.902155509722087`, PF `3.6646413698002367`, trades `9`

### usdjpy_z15_t60

- Module: `usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z1.5_sl30_tp45_shield22-7_ttl60`
- Weight: `0.05`
- Discovery gate: `6/8` windows, DSR `0.9999999463508012`
- Validation: Sharpe `1.9691825623216388`, PF `1.3006649567489723`, trades `51`

## Validation Execution Evidence

- Proxy realism Sharpe: `2.7014770003508564`
- Proxy realism PF: `2.632372488790235`
- Proxy realism positive years: `3/3`
- Exact Validation Sharpe: `3.01941642634328`
- Exact Validation PF: `2.9946414095430853`
- Exact Validation positive years: `3/3`
- Exact +2s delay PF: `2.367455262853688`
- Exact +0.3 pip spread PF: `2.847763046836414`

## Final Test Control

- The Final Test snapshot remains untouched.
- The one-shot runner is armed but requires `--execute-final-test` and will refuse any second run once the marker file exists.
