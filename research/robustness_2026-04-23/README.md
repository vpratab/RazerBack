# Public Robustness Checkpoint: 2026-04-23

This folder captures the current public-facing honesty checkpoint for RazerBack.

The goal of this document is to show what the system can currently defend, what it cannot yet defend, and why. It is intentionally more transparent about the validation state than a normal GitHub teaser, but less explicit about module recipes than a private research notebook.

## Scope

- Data source: Dukascopy bid/ask quote ticks aggregated to bid/ask M1, plus enriched midpoint features.
- Current validated yearly set in this public checkpoint: `2011-2013` and `2020-2025`.
- Missing continuous validated years: `2014-2019`.
- Interpretation: this is a serious launch-research checkpoint, not yet a full continuous `2011-2025` allocator-grade claim.

## Current Public Launch Candidate

Public description:

- protected multi-module breakout book
- concentrated in the instruments that have shown the cleanest recent robustness in the current finished sample
- annual/monthly loss overlay enabled

Key metrics:

| scope | roi_pct | sharpe_ann | max_drawdown_pct | win_rate_pct | profit_factor | trades |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2025 M1 | 20.7957 | 1.9903 | -3.7649 | 70.5882 | 2.9950 | 34 |
| 2025 exact tick | 22.0243 | 2.1027 | -3.6925 | 70.5882 | 3.1766 | 34 |
| 2022-2025 avg | 37.7825 | 2.3923 | n/a | n/a | n/a | n/a |
| worst validated year | -4.1535 | n/a | -8.4216 | n/a | n/a | n/a |

Interpretation:

- This remains the cleanest public launch candidate because its older validated tail is still tolerable.
- The exact-tick replay slightly improved the modeled 2025 result rather than breaking it.
- The result is promising enough to keep researching forward, but not strong enough to market as finished institutional proof.

## Strongest Public Add-On Lead

Public description:

- same protected baseline as above
- plus one additional short breakout component on a recent high-conviction JPY-cross sleeve

Key metrics:

| scope | roi_pct | sharpe_ann | max_drawdown_pct | win_rate_pct | profit_factor | trades |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2025 M1 | 29.0186 | 2.3761 | -3.7649 | 73.6842 | 3.6427 | 38 |
| 2025 exact tick | 30.3309 | 2.4765 | -3.6925 | 73.6842 | 3.8459 | 38 |
| 2022-2025 avg | 35.4703 | 2.4137 | n/a | n/a | n/a | n/a |
| worst validated year | -5.3058 | n/a | -8.4216 | n/a | n/a | n/a |

Interpretation:

- This is the strongest incremental public lead so far.
- It survived exact-tick replay cleanly.
- It materially improves the modern window, but it still worsens the older validated tail relative to the cleaner baseline, so it is not yet the default public replacement.

## Multifamily All-10 Sweep

The best finished all-10-pair multifamily sweep produced a diversified book that was valuable as an idea source, but not good enough to replace the safer launch baseline.

| portfolio | tested_window | roi_pct | sharpe_ann | max_drawdown_pct | win_rate_pct | profit_factor | trades |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| diversified | 2020-2025 | 239.8796 | 1.6992 | -8.3107 | 59.1837 | 2.0413 | 833 |
| best-by-pair | 2020-2025 | 234.9961 | 1.5298 | -10.6554 | 56.1798 | 1.6411 | 1246 |

Why it was not promoted to the public launch book:

- win rate stayed below the desired launch zone
- drawdown sat too close to or above the risk ceiling
- the sweep improved trade density, but not enough of the risk-quality profile

## Pod-Style Readout

Current public audit status:

- metric gate: `PASS`
- final pod gate: `FAIL`

Why final pod gate still fails:

- continuous `2011-2025` yearly validation is incomplete because `2014-2019` are not yet in the validated audited set
- the Dukascopy harvest is still in progress, so selected instruments are not yet complete across the full requested range
- full exact-tick replay has only been completed for the current launch window, not every filled trade across the full requested horizon
- the current public launch book was chosen with `2025` visible, so it is a launch-oriented research book, not a pristine untouched historical reveal

## What This Checkpoint Is Meant To Communicate

It is meant to show:

- the public engine can produce strong modern-window results under quote-aware execution assumptions
- exact-tick replay is real and can confirm a selected book rather than only the bar-level simulation
- the research process is being held to a realism standard instead of just a return target

It is not meant to imply:

- that the repo already contains a finished allocator-ready `2011-2025` institutional packet
- that the public docs expose every private portfolio recipe or curation rule
- that the current public book is the final private launch configuration

## Public Reproducibility Notes

The generic public audit helpers are:

- [scripts/run_exact_tick_replay.py](../../scripts/run_exact_tick_replay.py)
- [scripts/run_pod_grade_audit.py](../../scripts/run_pod_grade_audit.py)

The public repo includes the machinery used for exact-tick replay and pod-style audit. The front-facing checkpoint intentionally summarizes the book composition rather than publishing every active module manifest inline.
