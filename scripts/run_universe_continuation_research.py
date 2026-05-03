from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from continuation_core import ContinuationSpec, load_datasets, resolve_execution_profile, simulate_module, simulate_portfolio
from locked_portfolio_runtime import build_trade_stats


DEFAULT_INSTRUMENTS = [
    "eurusd",
    "gbpusd",
    "usdjpy",
    "audusd",
    "usdcad",
    "usdchf",
    "eurjpy",
    "eurgbp",
    "eurchf",
    "audjpy",
    "gbpjpy",
]

WINDOW_OPTIONS = [
    (0, 3),
    (2, 5),
    (3, 6),
    (6, 9),
    (7, 10),
    (8, 11),
    (9, 12),
    (10, 13),
    (12, 15),
    (13, 16),
    (14, 17),
    (18, 21),
    (20, 23),
]
RANGE_RATIO_OPTIONS = [0.85, 1.0, 1.15, 1.25, 1.4, 1.6]
IMBALANCE_OPTIONS = [1.0, 1.25, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0]
NODE_DISTANCE_OPTIONS = [1.5, 2.0, 2.5, 3.0, 4.0, 5.0]
RET60_OPTIONS = [2.5, 5.0, 7.5, 10.0, 15.0, 20.0, 25.0]
LOCAL_SIGMA_OPTIONS = [0.75, 0.85, 0.9, 1.0, 1.05, 1.1]
STOP_LOSS_OPTIONS = [10, 15, 20, 25, 30, 35, 40]
TTL_OPTIONS = [60, 120, 180, 240, 360]
FRACTION_PATTERNS = [
    (0.3, 0.3, 0.3),
    (0.4, 0.3, 0.2),
    (0.5, 0.25, 0.15),
]
LADDER_MULTIPLIER_PATTERNS = [
    (0.6, 1.2, 1.8),
    (0.75, 1.5, 2.25),
    (1.0, 2.0, 3.0),
]
TRAIL_RATIOS = [0.5, 0.6, 0.67, 0.75]
REQUIRED_ENRICHED_COLUMNS = [
    "ask_lambda",
    "bid_lambda",
    "imbalance_ratio",
    "node_mean",
    "distance_to_node_pips",
    "local_sigma",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a bounded continuation-module sweep across the 11-pair FX universe.")
    parser.add_argument("--data-dir", default="C:/fx_data/m1")
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent.parent / "output" / "universe_continuation_research"),
    )
    parser.add_argument("--scenario", choices=("base", "conservative", "hard"), default="conservative")
    parser.add_argument("--samples-per-pair-side", type=int, default=96)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-balance", type=float, default=1500.0)
    parser.add_argument("--risk-pct", type=float, default=2.9)
    parser.add_argument("--max-leverage", type=float, default=35.0)
    parser.add_argument("--max-concurrent", type=int, default=2)
    parser.add_argument("--max-modules-per-instrument", type=int, default=2)
    parser.add_argument("--target-trades-per-week", type=float, default=5.0)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) // 2))
    parser.add_argument("--instruments", nargs="+", default=DEFAULT_INSTRUMENTS, choices=DEFAULT_INSTRUMENTS)
    return parser.parse_args()


def make_rng(seed: int, instrument: str, side: str) -> np.random.Generator:
    digest = hashlib.sha256(f"{seed}|{instrument}|{side}".encode("utf-8")).digest()
    return np.random.default_rng(int.from_bytes(digest[:8], "big"))


def build_ladder(stop_loss_pips: int, rng: np.random.Generator) -> tuple[tuple[int, int, int], int]:
    multipliers = LADDER_MULTIPLIER_PATTERNS[int(rng.integers(0, len(LADDER_MULTIPLIER_PATTERNS)))]
    ladder = tuple(max(1, int(round(stop_loss_pips * value))) for value in multipliers)
    ladder = tuple(sorted(set(ladder)))
    if len(ladder) != 3:
        ladder = (stop_loss_pips, stop_loss_pips * 2, stop_loss_pips * 3)
    trail_stop_pips = max(1, int(round(stop_loss_pips * TRAIL_RATIOS[int(rng.integers(0, len(TRAIL_RATIOS)))])))
    return (int(ladder[0]), int(ladder[1]), int(ladder[2])), trail_stop_pips


def sample_spec(seed: int, instrument: str, side: str, draw_idx: int) -> ContinuationSpec:
    rng = make_rng(seed + draw_idx * 100_003, instrument, side)
    hour_start, hour_end = WINDOW_OPTIONS[int(rng.integers(0, len(WINDOW_OPTIONS)))]
    stop_loss_pips = int(rng.choice(STOP_LOSS_OPTIONS))
    ladder_pips, trail_stop_pips = build_ladder(stop_loss_pips, rng)
    return ContinuationSpec(
        instrument=instrument,
        side=side,
        hour_start=int(hour_start),
        hour_end=int(hour_end),
        range_ratio_min=float(rng.choice(RANGE_RATIO_OPTIONS)),
        imbalance_min=float(rng.choice(IMBALANCE_OPTIONS)),
        node_distance_max=float(rng.choice(NODE_DISTANCE_OPTIONS)),
        ret60_min_abs=float(rng.choice(RET60_OPTIONS)),
        local_sigma_ratio_min=float(rng.choice(LOCAL_SIGMA_OPTIONS)),
        stop_loss_pips=stop_loss_pips,
        ladder_pips=ladder_pips,
        ladder_fractions=FRACTION_PATTERNS[int(rng.integers(0, len(FRACTION_PATTERNS)))],
        trail_stop_pips=trail_stop_pips,
        ttl_bars=int(rng.choice(TTL_OPTIONS)),
    )


def build_candidate_specs(instruments: list[str], samples_per_pair_side: int, seed: int) -> list[ContinuationSpec]:
    specs: dict[str, ContinuationSpec] = {}
    for instrument in instruments:
        for side in ("long", "short"):
            draw_idx = 0
            while draw_idx < samples_per_pair_side:
                spec = sample_spec(seed, instrument, side, draw_idx)
                specs.setdefault(spec.name, spec)
                draw_idx += 1
    return list(specs.values())


def snapshot_paths(root: Path, instrument: str) -> list[Path]:
    return [
        root / f"{instrument}_5yr_m1.parquet",
        root / f"{instrument}_5yr_m1_bid_ask.parquet",
    ]


def prepare_research_snapshot(source_dir: Path, snapshot_dir: Path, instruments: list[str]) -> None:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    for instrument in instruments:
        for source_path in snapshot_paths(source_dir, instrument):
            if not source_path.exists():
                raise SystemExit(f"Missing required research input: {source_path}")
            target_path = snapshot_dir / source_path.name
            shutil.copy2(source_path, target_path)


def snapshot_missing_enrichment(snapshot_dir: Path, instruments: list[str]) -> list[str]:
    missing: list[str] = []
    for instrument in instruments:
        path = snapshot_dir / f"{instrument}_5yr_m1.parquet"
        column_names = list(pq.read_schema(path).names)
        if any(column not in column_names for column in REQUIRED_ENRICHED_COLUMNS):
            missing.append(instrument)
    return missing


def ensure_snapshot_enriched(snapshot_dir: Path, instruments: list[str]) -> None:
    missing = snapshot_missing_enrichment(snapshot_dir, instruments)
    if not missing:
        return
    enrich_script = REPO_ROOT / "enrich_forex_research_data.py"
    command = [
        sys.executable,
        str(enrich_script),
        "--data-dir",
        str(snapshot_dir),
        "--instruments",
        *missing,
    ]
    subprocess.run(command, check=True)


def total_sample_years(sample_start: pd.Timestamp, sample_end: pd.Timestamp) -> float:
    return max((sample_end - sample_start).days / 365.25, 0.01)


def score_candidate(row: dict[str, Any], sample_years: float) -> float:
    trade_count = float(row["trades"])
    if trade_count <= 0:
        return -999.0

    sharpe = float(row["sharpe_ann"])
    win_rate = float(row["win_rate_pct"])
    trades_per_week = float(row["trades_per_week"])
    roi_pct = float(row["roi_pct"])
    drawdown_pct = abs(float(row["max_drawdown_pct"]))
    coverage_ratio = float(row["active_years"]) / max(sample_years, 1.0)
    last_trade_gap_years = float(row["last_trade_gap_years"])

    score = 0.0
    score += min(sharpe / 1.5, 2.0) * 3.0
    score += min(win_rate / 70.0, 2.0) * 2.0
    score += min(trades_per_week / 0.5, 2.0) * 1.75
    score += min(max(roi_pct, 0.0) / 35.0, 2.0) * 1.25
    score += min(coverage_ratio, 1.0) * 2.0
    score -= max(drawdown_pct - 5.0, 0.0) / 2.5
    score -= max(last_trade_gap_years - 1.0, 0.0) * 2.0
    if trade_count < 20:
        score -= 1.5
    return float(score)


def summarize_spec(
    spec: ContinuationSpec,
    datasets: dict[str, dict[str, object]],
    start_balance: float,
    risk_pct: float,
    max_leverage: float,
    max_concurrent: int,
    scenario: str,
) -> dict[str, Any]:
    execution_profile = resolve_execution_profile(scenario)
    trades = simulate_module(spec, datasets[spec.instrument], execution_profile=execution_profile)
    sample_start = pd.Timestamp(datasets[spec.instrument]["df"]["timestamp"].iloc[0]).tz_convert("UTC")
    sample_end = pd.Timestamp(datasets[spec.instrument]["df"]["timestamp"].iloc[-1]).tz_convert("UTC")
    sample_weeks = max((sample_end - sample_start).days / 7.0, 1.0)
    sample_years = total_sample_years(sample_start, sample_end)

    if trades.empty:
        row = {
            **asdict(spec),
            "module": spec.name,
            "scenario": scenario,
            "trades": 0,
            "trades_per_week": 0.0,
            "roi_pct": 0.0,
            "sharpe_ann": 0.0,
            "sortino_ann": 0.0,
            "max_drawdown_pct": 0.0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "active_years": 0,
            "last_trade_at": "",
            "last_trade_gap_years": sample_years,
            "sample_start": sample_start.isoformat(),
            "sample_end": sample_end.isoformat(),
            "score": -999.0,
        }
        return row

    summary, _, _, _, yearly_table, filled_df = simulate_portfolio(
        trades,
        datasets,
        start_balance,
        risk_pct / 100.0,
        max_leverage,
        max_concurrent,
    )
    summary_row = summary.iloc[0]
    trade_stats_row = build_trade_stats(filled_df).iloc[0]
    active_years = int((yearly_table["trades"] > 0).sum())
    last_trade_at = pd.to_datetime(filled_df["entry_time"], utc=True).max()
    last_trade_gap_years = max((sample_end - last_trade_at).days / 365.25, 0.0)
    row = {
        **asdict(spec),
        "module": spec.name,
        "scenario": scenario,
        "trades": int(summary_row["portfolio_trades"]),
        "trades_per_week": float(summary_row["portfolio_trades"]) / sample_weeks,
        "roi_pct": float(summary_row["roi_pct"]),
        "sharpe_ann": float(summary_row["calendar_weekly_sharpe_ann"]),
        "sortino_ann": float(summary_row["calendar_weekly_sortino_ann"]),
        "max_drawdown_pct": float(summary_row["max_drawdown_pct"]),
        "win_rate_pct": float(summary_row["portfolio_win_rate_pct"]),
        "profit_factor": float(summary_row["portfolio_profit_factor"]),
        "avg_hold_minutes": float(trade_stats_row["avg_hold_minutes"]),
        "active_years": active_years,
        "last_trade_at": last_trade_at.isoformat(),
        "last_trade_gap_years": last_trade_gap_years,
        "sample_start": sample_start.isoformat(),
        "sample_end": sample_end.isoformat(),
    }
    row["score"] = score_candidate(row, sample_years)
    return row


def evaluate_specs(
    specs: list[ContinuationSpec],
    datasets: dict[str, dict[str, object]],
    start_balance: float,
    risk_pct: float,
    max_leverage: float,
    max_concurrent: int,
    scenario: str,
    workers: int,
) -> pd.DataFrame:
    def evaluate(spec: ContinuationSpec) -> dict[str, Any]:
        return summarize_spec(spec, datasets, start_balance, risk_pct, max_leverage, max_concurrent, scenario)

    if workers <= 1:
        rows = [evaluate(spec) for spec in specs]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            rows = list(executor.map(evaluate, specs))
    frame = pd.DataFrame(rows)
    return frame.sort_values(["score", "sharpe_ann", "win_rate_pct", "trades"], ascending=[False, False, False, False]).reset_index(drop=True)


def choose_top_by_pair(candidates: pd.DataFrame) -> pd.DataFrame:
    selected_rows: list[pd.Series] = []
    for instrument, group in candidates.groupby("instrument", sort=True):
        usable = group.loc[
            (group["trades"] >= 20)
            & (group["last_trade_gap_years"] <= 2.0)
            & (group["sharpe_ann"] > 0.5)
        ]
        if usable.empty:
            usable = group
        selected_rows.append(usable.iloc[0])
    return pd.DataFrame(selected_rows).sort_values("score", ascending=False).reset_index(drop=True)


def choose_greedy_portfolio(
    candidates: pd.DataFrame,
    target_trades_per_week: float,
    max_modules_per_instrument: int,
) -> pd.DataFrame:
    selected: list[pd.Series] = []
    per_instrument: dict[str, int] = {}
    running_density = 0.0
    filtered = candidates.loc[
        (candidates["trades"] >= 20)
        & (candidates["last_trade_gap_years"] <= 2.0)
        & (candidates["sharpe_ann"] > 0.25)
        & (candidates["win_rate_pct"] >= 45.0)
    ]
    if filtered.empty:
        filtered = candidates.head(20)

    for _, row in filtered.iterrows():
        instrument = str(row["instrument"])
        if per_instrument.get(instrument, 0) >= max_modules_per_instrument:
            continue
        selected.append(row)
        per_instrument[instrument] = per_instrument.get(instrument, 0) + 1
        running_density += float(row["trades_per_week"])
        if running_density >= target_trades_per_week:
            break

    return pd.DataFrame(selected).reset_index(drop=True)


def specs_from_candidate_frame(frame: pd.DataFrame) -> list[ContinuationSpec]:
    specs: list[ContinuationSpec] = []
    for row in frame.to_dict(orient="records"):
        specs.append(
            ContinuationSpec(
                instrument=str(row["instrument"]),
                side=str(row["side"]),
                hour_start=int(row["hour_start"]),
                hour_end=int(row["hour_end"]),
                range_ratio_min=float(row["range_ratio_min"]),
                imbalance_min=float(row["imbalance_min"]),
                node_distance_max=float(row["node_distance_max"]),
                ret60_min_abs=float(row["ret60_min_abs"]),
                local_sigma_ratio_min=float(row["local_sigma_ratio_min"]),
                stop_loss_pips=int(row["stop_loss_pips"]),
                ladder_pips=tuple(int(value) for value in row["ladder_pips"]),
                ladder_fractions=tuple(float(value) for value in row["ladder_fractions"]),
                trail_stop_pips=int(row["trail_stop_pips"]),
                ttl_bars=int(row["ttl_bars"]),
            )
        )
    return specs


def run_portfolio_from_specs(
    specs: list[ContinuationSpec],
    datasets: dict[str, dict[str, object]],
    scenario: str,
    start_balance: float,
    risk_pct: float,
    max_leverage: float,
    max_concurrent: int,
) -> dict[str, Any]:
    execution_profile = resolve_execution_profile(scenario)
    frames = [
        simulate_module(spec, datasets[spec.instrument], execution_profile=execution_profile)
        for spec in specs
    ]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        trade_table = pd.DataFrame()
    else:
        trade_table = pd.concat(frames, ignore_index=True)
    summary, module_table, weekly_table, monthly_table, yearly_table, filled_df = simulate_portfolio(
        trade_table,
        datasets,
        start_balance,
        risk_pct / 100.0,
        max_leverage,
        max_concurrent,
    )
    return {
        "summary": summary,
        "module_table": module_table,
        "weekly_table": weekly_table,
        "monthly_table": monthly_table,
        "yearly_table": yearly_table,
        "trade_ledger": filled_df,
        "trade_stats": build_trade_stats(filled_df),
    }


def write_portfolio_pack(target_dir: Path, name: str, specs: list[ContinuationSpec], artifacts: dict[str, Any]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    config_payload = {
        "name": name,
        "description": "Research-selected continuation module pack.",
        "modules": [{**asdict(spec), "name": spec.name} for spec in specs],
    }
    (target_dir / "selected_modules.json").write_text(json.dumps(config_payload, indent=2), encoding="utf-8")
    artifacts["summary"].to_csv(target_dir / "summary.csv", index=False)
    artifacts["module_table"].to_csv(target_dir / "module_table.csv", index=False)
    artifacts["weekly_table"].to_csv(target_dir / "weekly_table.csv", index=False)
    artifacts["monthly_table"].to_csv(target_dir / "monthly_table.csv", index=False)
    artifacts["yearly_table"].to_csv(target_dir / "yearly_table.csv", index=False)
    artifacts["trade_stats"].to_csv(target_dir / "trade_stats.csv", index=False)
    artifacts["trade_ledger"].to_csv(target_dir / "trade_ledger.csv", index=False)


def markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows_"
    columns = [str(column) for column in frame.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in frame.iterrows():
        values = []
        for column in frame.columns:
            value = row[column]
            if isinstance(value, float):
                values.append(f"{value:.4f}")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def render_report(
    candidates: pd.DataFrame,
    top_by_pair: pd.DataFrame,
    greedy: pd.DataFrame,
    pair_portfolio: dict[str, Any],
    greedy_portfolio: dict[str, Any],
    output_dir: Path,
    args: argparse.Namespace,
) -> str:
    def summary_line(label: str, artifacts: dict[str, Any]) -> list[str]:
        row = artifacts["summary"].iloc[0]
        return [
            f"### {label}",
            "",
            f"- ROI: `{row['roi_pct']:.2f}%`",
            f"- Sharpe: `{row['calendar_weekly_sharpe_ann']:.3f}`",
            f"- Max drawdown: `{row['max_drawdown_pct']:.2f}%`",
            f"- Win rate: `{row['portfolio_win_rate_pct']:.2f}%`",
            f"- Profit factor: `{row['portfolio_profit_factor']:.3f}`",
            f"- Trades: `{int(row['portfolio_trades'])}`",
            "",
        ]

    top_candidates = candidates.head(15)[
        [
            "module",
            "instrument",
            "side",
            "score",
            "roi_pct",
            "sharpe_ann",
            "max_drawdown_pct",
            "win_rate_pct",
            "trades",
            "trades_per_week",
            "active_years",
            "last_trade_gap_years",
        ]
    ]
    lines = [
        "# Universe Continuation Research",
        "",
        "This is a bounded continuation-family sweep over the full 11-pair universe using the current M1 bid/ask execution layer.",
        "",
        "## Target",
        "",
        "- Sharpe: `> 1.5`",
        "- Win rate: `>= 70%`",
        "- Trade density: `>= 5 trades/week` across the final portfolio",
        "- Average returns: `~35%`",
        "- Max drawdown: `< 5%`",
        "",
        "## Sweep Settings",
        "",
        f"- Scenario: `{args.scenario}`",
        f"- Instruments: `{', '.join(args.instruments)}`",
        f"- Samples per pair-side: `{args.samples_per_pair_side}`",
        f"- Candidate count: `{len(candidates)}`",
        f"- Output dir: `{output_dir}`",
        "",
        "## Best Individual Candidates",
        "",
        markdown_table(top_candidates),
        "",
        "## Top By Pair",
        "",
        markdown_table(
            top_by_pair[
                [
                    "module",
                    "instrument",
                    "side",
                    "score",
                    "roi_pct",
                    "sharpe_ann",
                    "max_drawdown_pct",
                    "win_rate_pct",
                    "trades",
                    "trades_per_week",
                ]
            ]
        ),
        "",
        "## Portfolio Trials",
        "",
    ]
    lines.extend(summary_line("Top-By-Pair Portfolio", pair_portfolio))
    lines.extend(summary_line("Greedy Density Portfolio", greedy_portfolio))
    lines.extend(
        [
            "## Notes",
            "",
            "- The current repo does not contain a historical broad-miner, so this sweep rebuilds one on top of the locked continuation execution core.",
            "- Results here are only as realistic as the current M1 bid/ask engine. They are materially better than midpoint-only research, but still not true tick-path execution.",
            "- If these portfolios still miss the target, the next step is threshold attribution and regime-specific mining rather than pretending the target is already solved.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    source_dir = Path(args.data_dir).expanduser().resolve()
    research_instruments = sorted(set(args.instruments) | {"usdjpy", "gbpusd"})
    snapshot_dir = output_dir / "research_data"
    prepare_research_snapshot(source_dir, snapshot_dir, research_instruments)
    ensure_snapshot_enriched(snapshot_dir, research_instruments)

    datasets = load_datasets(snapshot_dir, research_instruments)
    specs = build_candidate_specs(args.instruments, args.samples_per_pair_side, args.seed)
    candidates = evaluate_specs(
        specs,
        datasets,
        args.start_balance,
        args.risk_pct,
        args.max_leverage,
        args.max_concurrent,
        args.scenario,
        args.workers,
    )
    candidates.to_csv(output_dir / "all_candidates.csv", index=False)
    candidates.head(200).to_csv(output_dir / "top_candidates.csv", index=False)

    top_by_pair = choose_top_by_pair(candidates)
    top_by_pair.to_csv(output_dir / "top_by_pair.csv", index=False)

    greedy = choose_greedy_portfolio(candidates, args.target_trades_per_week, args.max_modules_per_instrument)
    greedy.to_csv(output_dir / "greedy_portfolio_candidates.csv", index=False)

    pair_specs = specs_from_candidate_frame(top_by_pair)
    greedy_specs = specs_from_candidate_frame(greedy) if not greedy.empty else []
    pair_portfolio = run_portfolio_from_specs(
        pair_specs,
        datasets,
        args.scenario,
        args.start_balance,
        args.risk_pct,
        args.max_leverage,
        args.max_concurrent,
    )
    greedy_portfolio = run_portfolio_from_specs(
        greedy_specs,
        datasets,
        args.scenario,
        args.start_balance,
        args.risk_pct,
        args.max_leverage,
        args.max_concurrent,
    )

    write_portfolio_pack(output_dir / "top_by_pair_portfolio", "top_by_pair_portfolio", pair_specs, pair_portfolio)
    write_portfolio_pack(output_dir / "greedy_density_portfolio", "greedy_density_portfolio", greedy_specs, greedy_portfolio)

    report = render_report(candidates, top_by_pair, greedy, pair_portfolio, greedy_portfolio, output_dir, args)
    (output_dir / "research_report.md").write_text(report, encoding="utf-8")
    print(output_dir)


if __name__ == "__main__":
    main()
