from __future__ import annotations

import argparse
import ast
import json
import math
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from continuation_core import ContinuationSpec, load_datasets, resolve_execution_profile, simulate_module, simulate_portfolio
from locked_portfolio_runtime import build_trade_stats, run_locked_portfolio
from scripts.run_multifamily_fx_research import BreakoutSpec, ReversalSpec, simulate_breakout_module, simulate_reversal_module
from scripts.run_universe_continuation_research import ensure_snapshot_enriched, prepare_research_snapshot


DEFAULT_DATA_DIR = Path("C:/fx_data/m1")
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "portfolio_factory_research"
DEFAULT_CONTINUATION_CSV = REPO_ROOT / "output" / "universe_continuation_research_density" / "all_candidates.csv"
DEFAULT_MULTIFAMILY_CSV = REPO_ROOT / "output" / "multifamily_fx_research" / "all_candidates.csv"
DEFAULT_BASELINE_CONFIG = REPO_ROOT / "configs" / "continuation_portfolio_total_v1.json"


@dataclass
class Candidate:
    source: str
    family: str
    module: str
    instrument: str
    side: str
    heuristic: float
    row: dict[str, Any]
    trades: pd.DataFrame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search for a simultaneous mixed-family FX portfolio using the current realistic execution engine."
    )
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--continuation-csv", default=str(DEFAULT_CONTINUATION_CSV))
    parser.add_argument("--multifamily-csv", default=str(DEFAULT_MULTIFAMILY_CSV))
    parser.add_argument("--baseline-config", default=str(DEFAULT_BASELINE_CONFIG))
    parser.add_argument("--scenario", choices=("base", "conservative", "hard"), default="conservative")
    parser.add_argument("--start-balance", type=float, default=1500.0)
    parser.add_argument("--risk-pct", type=float, default=2.9)
    parser.add_argument("--max-leverage", type=float, default=35.0)
    parser.add_argument("--max-concurrent", type=int, default=2)
    parser.add_argument("--min-trades", type=int, default=8)
    parser.add_argument("--max-candidate-drawdown", type=float, default=35.0)
    parser.add_argument("--max-candidates", type=int, default=48)
    parser.add_argument("--max-candidates-per-instrument", type=int, default=8)
    parser.add_argument("--max-modules", type=int, default=8)
    parser.add_argument("--max-modules-per-instrument", type=int, default=2)
    parser.add_argument("--beam-width", type=int, default=16)
    parser.add_argument("--top-portfolios", type=int, default=5)
    parser.add_argument("--objective-density-metric", choices=("active", "calendar"), default="active")
    parser.add_argument("--target-sharpe", type=float, default=1.7)
    parser.add_argument("--target-roi", type=float, default=25.0)
    parser.add_argument("--target-win-rate", type=float, default=70.0)
    parser.add_argument("--target-trades-per-week", type=float, default=5.0)
    parser.add_argument("--target-max-drawdown", type=float, default=8.0)
    parser.add_argument("--candidate-split-count", type=int, default=3)
    parser.add_argument("--candidate-min-positive-splits", type=int, default=2)
    parser.add_argument("--candidate-min-nonempty-splits", type=int, default=2)
    parser.add_argument("--portfolio-split-count", type=int, default=4)
    parser.add_argument("--target-positive-split-ratio", type=float, default=0.75)
    return parser.parse_args()


def discover_instruments(data_dir: Path) -> list[str]:
    instruments: set[str] = set()
    for path in data_dir.glob("*_5yr_m1.parquet"):
        instruments.add(path.name.replace("_5yr_m1.parquet", ""))
    return sorted(
        instrument
        for instrument in instruments
        if (data_dir / f"{instrument}_5yr_m1_bid_ask.parquet").exists()
    )


def safe_float(value: Any, default: float = 0.0, cap: float | None = None) -> float:
    try:
        numeric = float(value)
    except Exception:
        return default
    if math.isnan(numeric):
        return default
    if cap is not None and math.isfinite(numeric):
        numeric = min(numeric, cap)
    return numeric


def heuristic_score(row: pd.Series) -> float:
    sharpe = safe_float(row.get("sharpe_ann"))
    roi = safe_float(row.get("roi_pct"))
    drawdown = abs(safe_float(row.get("max_drawdown_pct")))
    win_rate = safe_float(row.get("win_rate_pct"))
    profit_factor = safe_float(row.get("profit_factor"), cap=6.0)
    trades = safe_float(row.get("trades"))
    trades_per_week = safe_float(row.get("trades_per_week"))
    gap_years = safe_float(row.get("last_trade_gap_years"))
    active_years = safe_float(row.get("active_years"))

    score = 0.0
    score += sharpe * 5.0
    score += max(roi, 0.0) * 0.12
    score += max(win_rate - 45.0, 0.0) * 0.06
    score += max(profit_factor - 1.0, -1.0) * 1.5
    score += min(trades / 40.0, 1.5) * 1.0
    score += min(trades_per_week / 0.2, 1.5) * 0.75
    score += min(active_years, 4.0) * 0.25
    score -= drawdown * 0.12
    score -= gap_years * 0.5
    return float(score)


def load_candidate_frames(continuation_csv: Path, multifamily_csv: Path) -> pd.DataFrame:
    continuation = pd.read_csv(continuation_csv)
    continuation["source"] = "continuation"
    continuation["family"] = "continuation"

    multifamily = pd.read_csv(multifamily_csv)
    multifamily["source"] = "multifamily"

    combined = pd.concat([continuation, multifamily], ignore_index=True, sort=False)
    combined["heuristic"] = combined.apply(heuristic_score, axis=1)
    combined = combined.sort_values(["heuristic", "sharpe_ann", "roi_pct"], ascending=False).reset_index(drop=True)
    return combined


def select_candidate_pool(candidates: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    filtered = candidates.loc[
        (candidates["last_trade_gap_years"] <= 2.0)
        & (candidates["trades"] >= args.min_trades)
        & (candidates["max_drawdown_pct"] >= -args.max_candidate_drawdown)
    ].copy()
    if filtered.empty:
        filtered = candidates.copy()

    trimmed_groups: list[pd.DataFrame] = []
    for _, group in filtered.groupby("instrument", sort=True):
        trimmed_groups.append(group.head(args.max_candidates_per_instrument))
    trimmed = pd.concat(trimmed_groups, ignore_index=True)

    trimmed = trimmed.sort_values(["heuristic", "sharpe_ann", "roi_pct"], ascending=False)
    trimmed = trimmed.head(args.max_candidates).reset_index(drop=True)
    return trimmed


def as_optional_float(value: Any) -> float | None:
    numeric = safe_float(value, default=float("nan"))
    if math.isnan(numeric):
        return None
    return float(numeric)


def continuation_spec_from_row(row: dict[str, Any]) -> ContinuationSpec:
    return ContinuationSpec(
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
        ladder_pips=tuple(int(value) for value in ast.literal_eval(str(row["ladder_pips"]))),
        ladder_fractions=tuple(float(value) for value in ast.literal_eval(str(row["ladder_fractions"]))),
        trail_stop_pips=int(row["trail_stop_pips"]),
        ttl_bars=int(row["ttl_bars"]),
    )


def breakout_spec_from_row(row: dict[str, Any]) -> BreakoutSpec:
    return BreakoutSpec(
        instrument=str(row["instrument"]),
        side=str(row["side"]),
        hour_utc=int(row["hour_utc"]),
        range_ratio_min=float(row["range_ratio_min"]),
        close_dist_min=float(row["close_dist_min"]),
        breakout_wick_max=as_optional_float(row.get("breakout_wick_max")),
        ret15_min=as_optional_float(row.get("ret15_min")),
        ret15_max=as_optional_float(row.get("ret15_max")),
        z_min=as_optional_float(row.get("z_min")),
        stop_pips=int(row["stop_pips"]),
        target_pips=int(row["target_pips"]),
        shield_trigger_pips=int(row["shield_trigger_pips"]),
        shield_lock_pips=int(row["shield_lock_pips"]),
        ttl_bars=int(row["ttl_bars"]),
    )


def reversal_spec_from_row(row: dict[str, Any]) -> ReversalSpec:
    return ReversalSpec(
        instrument=str(row["instrument"]),
        side=str(row["side"]),
        hour_utc=int(row["hour_utc"]),
        range_ratio_min=float(row["range_ratio_min"]),
        wick_min=float(row["wick_min"]),
        dist_min=float(row["dist_min"]),
        ret15_min=as_optional_float(row.get("ret15_min")),
        ret15_max=as_optional_float(row.get("ret15_max")),
        z_min=as_optional_float(row.get("z_min")),
        stop_pips=int(row["stop_pips"]),
        target_pips=int(row["target_pips"]),
        shield_trigger_pips=int(row["shield_trigger_pips"]),
        shield_lock_pips=int(row["shield_lock_pips"]),
        ttl_bars=int(row["ttl_bars"]),
    )


def build_candidate_objects(
    pool: pd.DataFrame,
    datasets: dict[str, dict[str, object]],
    scenario: str,
    args: argparse.Namespace,
) -> list[Candidate]:
    execution_profile = resolve_execution_profile(scenario)
    built: list[Candidate] = []

    for row in pool.to_dict(orient="records"):
        family = str(row["family"])
        instrument = str(row["instrument"])
        trades = pd.DataFrame()

        if family == "continuation":
            spec = continuation_spec_from_row(row)
            trades = simulate_module(spec, datasets[instrument], execution_profile=execution_profile)
        elif family == "breakout":
            spec = breakout_spec_from_row(row)
            trades = simulate_breakout_module(spec, datasets[instrument], spec.name)
        elif family == "reversal":
            spec = reversal_spec_from_row(row)
            trades = simulate_reversal_module(spec, datasets[instrument], spec.name)
        else:
            raise ValueError(f"Unknown family: {family}")

        if trades.empty:
            continue

        split_metrics = compute_trade_split_metrics(trades, max(args.candidate_split_count, 1))
        row.update(split_metrics)
        if args.candidate_split_count > 1:
            if split_metrics["nonempty_splits"] < args.candidate_min_nonempty_splits:
                continue
            if split_metrics["positive_splits"] < args.candidate_min_positive_splits:
                continue

        built.append(
            Candidate(
                source=str(row["source"]),
                family=family,
                module=str(row["module"]),
                instrument=instrument,
                side=str(row["side"]),
                heuristic=float(row["heuristic"]),
                row=row,
                trades=trades.copy(),
            )
        )

    built.sort(key=lambda item: (item.heuristic, item.module), reverse=True)
    return built


def portfolio_objective(metrics: dict[str, Any], args: argparse.Namespace) -> float:
    sharpe = float(metrics["sharpe_ann"])
    roi = float(metrics["roi_pct"])
    drawdown = abs(float(metrics["max_drawdown_pct"]))
    win_rate = float(metrics["win_rate_pct"])
    profit_factor = min(float(metrics["profit_factor"]), 5.0)
    calendar_tpw = float(metrics["calendar_trades_per_week"])
    active_tpw = float(metrics["trades_per_active_week"])
    trade_count = int(metrics["trade_count"])
    instrument_count = int(metrics["instrument_count"])
    positive_split_ratio = float(metrics.get("portfolio_positive_split_ratio", 0.0))
    worst_split_return_pct = float(metrics.get("portfolio_worst_split_return_pct", 0.0))
    best_instrument_concentration = int(metrics.get("best_instrument_concentration", 0))
    target_density = max(float(args.target_trades_per_week), 0.1)
    density_metric = calendar_tpw if args.objective_density_metric == "calendar" else active_tpw

    score = 0.0
    score += min(sharpe / max(args.target_sharpe, 0.1), 1.5) * 6.0
    score += min(max(roi, 0.0) / max(args.target_roi, 0.1), 1.5) * 4.0
    score += min(win_rate / max(args.target_win_rate, 1.0), 1.5) * 3.0
    score += min(profit_factor / 1.5, 1.5) * 2.0
    score += min(density_metric / target_density, 1.5) * 4.0
    score += min(calendar_tpw / target_density, 1.5) * 2.0
    score += min(instrument_count / 6.0, 1.0) * 1.0
    score += min(positive_split_ratio / max(args.target_positive_split_ratio, 0.1), 1.5) * 3.0
    score -= max(drawdown - args.target_max_drawdown, 0.0) / 1.5
    score -= max(target_density - calendar_tpw, 0.0) * 0.8
    score -= max(args.target_positive_split_ratio - positive_split_ratio, 0.0) * 6.0
    score -= max(-worst_split_return_pct, 0.0) * 0.25
    score -= max(best_instrument_concentration - 2, 0) * 0.75
    score -= max(25 - trade_count, 0) / 10.0
    if roi < 0.0:
        score -= abs(roi) / 5.0
    if sharpe < 0.0:
        score -= abs(sharpe) * 4.0
    return float(score)


def activity_metrics(filled_df: pd.DataFrame, sample_start: pd.Timestamp, sample_end: pd.Timestamp) -> tuple[float, float]:
    trade_count = int(len(filled_df))
    calendar_weeks = max((sample_end - sample_start).days / 7.0, 1.0)
    if filled_df.empty:
        return trade_count / calendar_weeks, 0.0

    entry_time = pd.to_datetime(filled_df["entry_time"], utc=True)
    active_weeks = int(entry_time.dt.tz_localize(None).dt.to_period("W").nunique())
    active_weeks = max(active_weeks, 1)
    return trade_count / calendar_weeks, trade_count / float(active_weeks)


def split_bin_codes(
    timestamps: pd.Series,
    sample_start: pd.Timestamp,
    sample_end: pd.Timestamp,
    split_count: int,
) -> np.ndarray:
    if split_count <= 1 or timestamps.empty:
        return np.zeros(len(timestamps), dtype=np.int16)
    start_ns = pd.Timestamp(sample_start).value
    end_ns = pd.Timestamp(sample_end).value
    span_ns = max(end_ns - start_ns, 1)
    codes = ((timestamps.astype("int64") - start_ns) * split_count) // span_ns
    codes = np.clip(codes.to_numpy(dtype=np.int64), 0, split_count - 1)
    return codes.astype(np.int16)


def compute_trade_split_metrics(
    trades: pd.DataFrame,
    split_count: int,
) -> dict[str, Any]:
    metrics = {
        "split_count": max(int(split_count), 1),
        "nonempty_splits": 0,
        "positive_splits": 0,
        "positive_split_ratio": 0.0,
        "worst_split_pnl_pips": 0.0,
        "best_split_pnl_pips": 0.0,
        "median_split_pnl_pips": 0.0,
    }
    if trades.empty or split_count <= 1:
        return metrics

    entry_time = pd.to_datetime(trades["entry_time"], utc=True)
    exit_time = pd.to_datetime(trades["exit_time"], utc=True)
    sample_start = entry_time.min()
    sample_end = exit_time.max()
    if pd.isna(sample_start) or pd.isna(sample_end) or sample_end <= sample_start:
        return metrics

    working = trades.copy()
    working["_split"] = split_bin_codes(exit_time, sample_start, sample_end, split_count)
    grouped = working.groupby("_split").agg(
        trades=("pnl_pips", "size"),
        pnl_pips=("pnl_pips", "sum"),
    )

    split_pnls: list[float] = []
    positive_splits = 0
    nonempty_splits = 0
    for split_idx in range(split_count):
        if split_idx in grouped.index:
            nonempty_splits += 1
            pnl_pips = float(grouped.at[split_idx, "pnl_pips"])
            split_pnls.append(pnl_pips)
            if pnl_pips > 0.0:
                positive_splits += 1

    if not split_pnls:
        return metrics

    metrics.update(
        {
            "nonempty_splits": nonempty_splits,
            "positive_splits": positive_splits,
            "positive_split_ratio": float(positive_splits / nonempty_splits) if nonempty_splits > 0 else 0.0,
            "worst_split_pnl_pips": float(min(split_pnls)),
            "best_split_pnl_pips": float(max(split_pnls)),
            "median_split_pnl_pips": float(np.median(split_pnls)),
        }
    )
    return metrics


def compute_portfolio_split_metrics(
    filled_df: pd.DataFrame,
    sample_start: pd.Timestamp,
    sample_end: pd.Timestamp,
    split_count: int,
    start_balance: float,
) -> dict[str, Any]:
    metrics = {
        "portfolio_split_count": max(int(split_count), 1),
        "portfolio_nonempty_splits": 0,
        "portfolio_positive_splits": 0,
        "portfolio_positive_split_ratio": 0.0,
        "portfolio_worst_split_return_pct": 0.0,
        "portfolio_best_split_return_pct": 0.0,
        "portfolio_median_split_return_pct": 0.0,
    }
    if filled_df.empty or split_count <= 1:
        return metrics

    exit_time = pd.to_datetime(filled_df["exit_time"], utc=True)
    if pd.isna(sample_start) or pd.isna(sample_end) or sample_end <= sample_start:
        return metrics

    working = filled_df.sort_values(["exit_time", "entry_time", "module"]).reset_index(drop=True).copy()
    working["_split"] = split_bin_codes(exit_time, sample_start, sample_end, split_count)

    split_returns: list[float] = []
    nonempty_splits = 0
    positive_splits = 0
    current_nav = float(start_balance)
    for split_idx in range(split_count):
        split_frame = working.loc[working["_split"] == split_idx]
        start_nav = current_nav
        pnl_dollars = float(split_frame["pnl_dollars"].sum()) if not split_frame.empty else 0.0
        current_nav = start_nav + pnl_dollars
        split_return_pct = (pnl_dollars / start_nav) * 100.0 if start_nav > 1e-12 else 0.0
        split_returns.append(split_return_pct)
        if not split_frame.empty:
            nonempty_splits += 1
        if split_return_pct > 0.0:
            positive_splits += 1

    if not split_returns:
        return metrics

    metrics.update(
        {
            "portfolio_nonempty_splits": nonempty_splits,
            "portfolio_positive_splits": positive_splits,
            "portfolio_positive_split_ratio": float(positive_splits / split_count),
            "portfolio_worst_split_return_pct": float(min(split_returns)),
            "portfolio_best_split_return_pct": float(max(split_returns)),
            "portfolio_median_split_return_pct": float(np.median(split_returns)),
        }
    )
    return metrics


def build_portfolio_metrics(
    key: tuple[str, ...],
    module_map: dict[str, Candidate],
    datasets: dict[str, dict[str, object]],
    start_balance: float,
    risk_pct: float,
    max_leverage: float,
    max_concurrent: int,
    args: argparse.Namespace,
) -> dict[str, Any]:
    frames = [module_map[module].trades for module in key]
    trade_frame = pd.concat(frames, ignore_index=True).sort_values(["entry_time", "module"]).reset_index(drop=True)
    summary, module_table, weekly, monthly, yearly, filled_df = simulate_portfolio(
        trade_frame,
        datasets,
        start_balance,
        risk_pct / 100.0,
        max_leverage,
        max_concurrent,
    )
    trade_stats = build_trade_stats(filled_df)
    summary_row = summary.iloc[0]
    trade_row = trade_stats.iloc[0]

    dataset_starts = [
        pd.Timestamp(data["df"]["timestamp"].iloc[0]).tz_convert("UTC")
        for data in datasets.values()
        if len(data["df"]) > 0
    ]
    dataset_ends = [
        pd.Timestamp(data["df"]["timestamp"].iloc[-1]).tz_convert("UTC")
        for data in datasets.values()
        if len(data["df"]) > 0
    ]
    sample_start = min(dataset_starts)
    sample_end = max(dataset_ends)
    calendar_tpw, active_tpw = activity_metrics(filled_df, sample_start, sample_end)
    split_metrics = compute_portfolio_split_metrics(
        filled_df,
        sample_start,
        sample_end,
        max(args.portfolio_split_count, 1),
        start_balance,
    )

    instruments = sorted({module_map[module].instrument for module in key})
    families = sorted({module_map[module].family for module in key})
    counts = Counter(module_map[module].instrument for module in key)

    metrics = {
        "modules": list(key),
        "module_count": len(key),
        "instrument_count": len(instruments),
        "family_count": len(families),
        "instruments": ", ".join(instruments),
        "families": ", ".join(families),
        "roi_pct": float(summary_row["roi_pct"]),
        "sharpe_ann": float(summary_row["calendar_weekly_sharpe_ann"]),
        "sortino_ann": float(summary_row["calendar_weekly_sortino_ann"]),
        "max_drawdown_pct": float(summary_row["max_drawdown_pct"]),
        "win_rate_pct": float(summary_row["portfolio_win_rate_pct"]),
        "profit_factor": float(summary_row["portfolio_profit_factor"]),
        "trade_count": int(summary_row["portfolio_trades"]),
        "calendar_trades_per_week": float(calendar_tpw),
        "trades_per_active_week": float(active_tpw),
        "positive_calendar_months_pct": float(summary_row["positive_calendar_months_pct"]),
        "avg_calendar_monthly_return_pct": float(summary_row["avg_calendar_monthly_return_pct"]),
        "best_instrument_concentration": int(max(counts.values())) if counts else 0,
        "avg_trade_dollars": float(trade_row["avg_pnl_dollars"]),
        "median_trade_dollars": float(trade_row["median_pnl_dollars"]),
        "sample_start": sample_start.isoformat(),
        "sample_end": sample_end.isoformat(),
    }
    metrics.update(split_metrics)
    metrics["objective"] = portfolio_objective(metrics, args)
    return metrics


def can_add(key: tuple[str, ...], candidate: Candidate, module_map: dict[str, Candidate], max_modules_per_instrument: int) -> bool:
    if candidate.module in key:
        return False
    counts = Counter(module_map[module].instrument for module in key)
    return counts[candidate.instrument] < max_modules_per_instrument


def beam_search(
    candidates: list[Candidate],
    datasets: dict[str, dict[str, object]],
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], dict[tuple[str, ...], dict[str, Any]], dict[str, Candidate]]:
    module_map = {candidate.module: candidate for candidate in candidates}
    cache: dict[tuple[str, ...], dict[str, Any]] = {}
    ranked = [candidate.module for candidate in candidates]

    def evaluate(key: tuple[str, ...]) -> dict[str, Any]:
        if key not in cache:
            cache[key] = build_portfolio_metrics(
                key,
                module_map,
                datasets,
                args.start_balance,
                args.risk_pct,
                args.max_leverage,
                args.max_concurrent,
                args,
            )
        return cache[key]

    beam: list[tuple[str, ...]] = [tuple()]
    all_ranked: dict[tuple[str, ...], dict[str, Any]] = {}

    for _ in range(args.max_modules):
        expansions: dict[tuple[str, ...], dict[str, Any]] = {}
        for key in beam:
            for module in ranked:
                candidate = module_map[module]
                if not can_add(key, candidate, module_map, args.max_modules_per_instrument):
                    continue
                new_key = tuple(sorted((*key, module)))
                if new_key in expansions:
                    continue
                expansions[new_key] = evaluate(new_key)

        if not expansions:
            break

        ordered = sorted(
            expansions.items(),
            key=lambda item: (item[1]["objective"], item[1]["sharpe_ann"], item[1]["roi_pct"]),
            reverse=True,
        )
        beam = [key for key, _ in ordered[: args.beam_width]]
        for key, metrics in ordered[: args.beam_width * 4]:
            all_ranked[key] = metrics

    ranked_results = sorted(
        all_ranked.values(),
        key=lambda item: (item["objective"], item["sharpe_ann"], item["roi_pct"]),
        reverse=True,
    )
    return ranked_results, cache, module_map


def prune_modules(
    metrics: dict[str, Any],
    cache: dict[tuple[str, ...], dict[str, Any]],
    module_map: dict[str, Candidate],
    datasets: dict[str, dict[str, object]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    key = tuple(sorted(metrics["modules"]))

    def evaluate(candidate_key: tuple[str, ...]) -> dict[str, Any]:
        if candidate_key not in cache:
            cache[candidate_key] = build_portfolio_metrics(
                candidate_key,
                module_map,
                datasets,
                args.start_balance,
                args.risk_pct,
                args.max_leverage,
                args.max_concurrent,
                args,
            )
        return cache[candidate_key]

    improved = True
    current = evaluate(key)
    while improved and len(current["modules"]) > 1:
        improved = False
        best = current
        for module in list(current["modules"]):
            candidate_key = tuple(sorted(value for value in current["modules"] if value != module))
            candidate = evaluate(candidate_key)
            if candidate["objective"] > best["objective"]:
                best = candidate
                improved = True
        current = best
    return current


def materialize_portfolio(
    metrics: dict[str, Any],
    module_map: dict[str, Candidate],
    datasets: dict[str, dict[str, object]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    key = tuple(sorted(metrics["modules"]))
    frames = [module_map[module].trades for module in key]
    trade_frame = pd.concat(frames, ignore_index=True).sort_values(["entry_time", "module"]).reset_index(drop=True)
    summary, module_table, weekly, monthly, yearly, filled_df = simulate_portfolio(
        trade_frame,
        datasets,
        args.start_balance,
        args.risk_pct / 100.0,
        args.max_leverage,
        args.max_concurrent,
    )
    trade_stats = build_trade_stats(filled_df)
    selected = pd.DataFrame([module_map[module].row for module in key]).copy()
    selected.insert(0, "selected_rank", range(1, len(selected) + 1))
    manifest = selected.replace({np.nan: None}).to_dict(orient="records")
    return {
        "selected": selected,
        "summary": summary,
        "module_table": module_table,
        "weekly_table": weekly,
        "monthly_table": monthly,
        "yearly_table": yearly,
        "trade_stats": trade_stats,
        "trade_ledger": filled_df,
        "modules_manifest": manifest,
    }


def write_portfolio_pack(target_dir: Path, label: str, artifacts: dict[str, Any]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    artifacts["selected"].to_csv(target_dir / "selected_candidates.csv", index=False)
    artifacts["summary"].to_csv(target_dir / "summary.csv", index=False)
    artifacts["module_table"].to_csv(target_dir / "module_table.csv", index=False)
    artifacts["weekly_table"].to_csv(target_dir / "weekly_table.csv", index=False)
    artifacts["monthly_table"].to_csv(target_dir / "monthly_table.csv", index=False)
    artifacts["yearly_table"].to_csv(target_dir / "yearly_table.csv", index=False)
    artifacts["trade_stats"].to_csv(target_dir / "trade_stats.csv", index=False)
    artifacts["trade_ledger"].to_csv(target_dir / "trade_ledger.csv", index=False)
    (target_dir / "modules.json").write_text(json.dumps(artifacts["modules_manifest"], indent=2), encoding="utf-8")
    (target_dir / "label.txt").write_text(label + "\n", encoding="utf-8")


def read_baseline_metrics(baseline_dir: Path) -> dict[str, Any]:
    summary = pd.read_csv(baseline_dir / "summary.csv").iloc[0]
    trade_stats = pd.read_csv(baseline_dir / "trade_stats.csv").iloc[0]
    module_table = pd.read_csv(baseline_dir / "module_table.csv")
    ledger = pd.read_csv(baseline_dir / "trade_ledger.csv")
    sample_start = pd.to_datetime(ledger["entry_time"], utc=True).min() if not ledger.empty else pd.NaT
    sample_end = pd.to_datetime(ledger["exit_time"], utc=True).max() if not ledger.empty else pd.NaT
    active_weeks = (
        int(pd.to_datetime(ledger["entry_time"], utc=True).dt.tz_localize(None).dt.to_period("W").nunique())
        if not ledger.empty
        else 0
    )
    trades_per_active_week = float(len(ledger) / active_weeks) if active_weeks > 0 else 0.0
    return {
        "label": "baseline_locked_portfolio",
        "objective": np.nan,
        "module_count": int(len(module_table)),
        "instrument_count": int(module_table["instrument"].nunique()) if not module_table.empty else 0,
        "family_count": 1,
        "instruments": ", ".join(sorted(module_table["instrument"].astype(str).unique())) if not module_table.empty else "",
        "families": "continuation",
        "roi_pct": float(summary["roi_pct"]),
        "sharpe_ann": float(summary["calendar_weekly_sharpe_ann"]),
        "sortino_ann": float(summary["calendar_weekly_sortino_ann"]),
        "max_drawdown_pct": float(summary["max_drawdown_pct"]),
        "win_rate_pct": float(summary["portfolio_win_rate_pct"]),
        "profit_factor": float(summary["portfolio_profit_factor"]),
        "trade_count": int(summary["portfolio_trades"]),
        "calendar_trades_per_week": np.nan,
        "trades_per_active_week": trades_per_active_week,
        "positive_calendar_months_pct": float(summary["positive_calendar_months_pct"]),
        "avg_calendar_monthly_return_pct": float(summary["avg_calendar_monthly_return_pct"]),
        "sample_start": "" if pd.isna(sample_start) else sample_start.isoformat(),
        "sample_end": "" if pd.isna(sample_end) else sample_end.isoformat(),
        "avg_trade_dollars": float(trade_stats["avg_pnl_dollars"]),
        "median_trade_dollars": float(trade_stats["median_pnl_dollars"]),
    }


def markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows_"
    lines = [
        "| " + " | ".join(str(column) for column in frame.columns) + " |",
        "| " + " | ".join(["---"] * len(frame.columns)) + " |",
    ]
    for _, row in frame.iterrows():
        values: list[str] = []
        for column in frame.columns:
            value = row[column]
            if isinstance(value, float):
                if math.isnan(value):
                    values.append("")
                else:
                    values.append(f"{value:.4f}")
            else:
                values.append(str(value))
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def render_report(
    output_dir: Path,
    data_dir: Path,
    candidate_pool: pd.DataFrame,
    candidate_objects: list[Candidate],
    baseline_metrics: dict[str, Any] | None,
    top_metrics: list[dict[str, Any]],
    top_artifacts: list[tuple[str, dict[str, Any], dict[str, Any]]],
    args: argparse.Namespace,
) -> str:
    summary_rows = []
    if baseline_metrics is not None:
        summary_rows.append(baseline_metrics)
    summary_rows.extend(
        {
            "label": label,
            "objective": metrics["objective"],
            "module_count": metrics["module_count"],
            "instrument_count": metrics["instrument_count"],
            "families": metrics["families"],
            "roi_pct": metrics["roi_pct"],
            "sharpe_ann": metrics["sharpe_ann"],
            "max_drawdown_pct": metrics["max_drawdown_pct"],
            "win_rate_pct": metrics["win_rate_pct"],
            "profit_factor": metrics["profit_factor"],
            "trade_count": metrics["trade_count"],
            "positive_split_ratio": metrics.get("portfolio_positive_split_ratio", np.nan),
            "worst_split_return_pct": metrics.get("portfolio_worst_split_return_pct", np.nan),
            "trades_per_active_week": metrics["trades_per_active_week"],
        }
        for label, metrics, _ in top_artifacts
    )
    summary_table = pd.DataFrame(summary_rows)

    lines = [
        "# Portfolio Factory Research",
        "",
        "This search tries to build a simultaneous multi-module FX book from the current continuation and breakout/reversal candidate pools.",
        "",
        "## Objective",
        "",
        "- Target Sharpe: `~1.7`",
        "- Target annual return: `22% to 28%`",
        "- Target max drawdown: `< 8%`",
        "- Target style: simultaneous modules, portfolio-level leverage, realistic bid/ask execution",
        "- Robustness gate: contiguous split consistency and concentration-aware selection",
        "",
        "## Search Surface",
        "",
        f"- Data dir: `{data_dir}`",
        f"- Scenario: `{args.scenario}`",
        f"- Candidate CSVs: `{Path(args.continuation_csv).name}`, `{Path(args.multifamily_csv).name}`",
        f"- Candidate pool after filtering: `{len(candidate_pool)}` rows",
        f"- Candidate objects with non-empty trades: `{len(candidate_objects)}`",
        f"- Max modules searched: `{args.max_modules}`",
        f"- Beam width: `{args.beam_width}`",
        "",
        "## Portfolio Ranking",
        "",
        markdown_table(summary_table),
        "",
        "## Best Portfolio Modules",
        "",
        markdown_table(
            top_artifacts[0][2]["selected"][
                [
                    "selected_rank",
                    "source",
                    "family",
                    "module",
                    "instrument",
                    "side",
                    "roi_pct",
                    "sharpe_ann",
                    "max_drawdown_pct",
                    "win_rate_pct",
                    "trades",
                ]
            ]
        )
        if top_artifacts
        else "_No portfolio artifacts_",
        "",
        "## Readout",
        "",
        "- This is the honest portfolio-level result on the current machine's data surface, not a cherry-picked single-module screenshot.",
        "- If the top portfolio still misses the target, the problem is not a lack of combinatorics; it means the present data and candidate universe do not yet support a pod-shop-grade claim.",
        "- The baseline locked portfolio is shown for reference because it was inherited from a different historical lineage and needs to be compared against the current local data, not folklore.",
        "",
        "## Output",
        "",
        f"- Output dir: `{output_dir}`",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    available_instruments = discover_instruments(data_dir)
    if not available_instruments:
        raise SystemExit(f"No enriched bid/ask datasets found under {data_dir}")

    snapshot_dir = output_dir / "research_data"
    prepare_research_snapshot(data_dir, snapshot_dir, available_instruments)
    ensure_snapshot_enriched(snapshot_dir, available_instruments)

    continuation_csv = Path(args.continuation_csv).expanduser().resolve()
    multifamily_csv = Path(args.multifamily_csv).expanduser().resolve()
    candidates = load_candidate_frames(continuation_csv, multifamily_csv)
    candidate_pool = select_candidate_pool(candidates, args)

    datasets = load_datasets(snapshot_dir, available_instruments)
    candidate_objects = build_candidate_objects(candidate_pool, datasets, args.scenario, args)
    if not candidate_objects:
        raise SystemExit("Candidate pool produced no non-empty trade sets.")

    ranked_results, cache, module_map = beam_search(candidate_objects, datasets, args)
    pruned_results: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for metrics in ranked_results[: args.top_portfolios * 4]:
        pruned = prune_modules(metrics, cache, module_map, datasets, args)
        key = tuple(sorted(pruned["modules"]))
        if key in seen:
            continue
        seen.add(key)
        pruned_results.append(pruned)
        if len(pruned_results) >= args.top_portfolios:
            break

    top_artifacts: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    portfolio_rows: list[dict[str, Any]] = []
    for rank, metrics in enumerate(pruned_results, start=1):
        label = f"portfolio_{rank:02d}"
        artifacts = materialize_portfolio(metrics, module_map, datasets, args)
        write_portfolio_pack(output_dir / label, label, artifacts)
        top_artifacts.append((label, metrics, artifacts))
        portfolio_rows.append({"label": label, **metrics})

    pd.DataFrame(portfolio_rows).to_csv(output_dir / "portfolio_rankings.csv", index=False)
    candidate_pool.to_csv(output_dir / "candidate_pool.csv", index=False)

    baseline_metrics: dict[str, Any] | None = None
    baseline_config = Path(args.baseline_config).expanduser().resolve()
    if baseline_config.exists():
        baseline_dir = output_dir / "baseline_locked_portfolio"
        try:
            run_locked_portfolio(
                config_path=baseline_config,
                data_dir_override=snapshot_dir,
                output_dir_override=baseline_dir,
                start_balance=args.start_balance,
                risk_pct=args.risk_pct,
                max_leverage=args.max_leverage,
                max_concurrent=args.max_concurrent,
                scenario=args.scenario,
            )
            baseline_metrics = read_baseline_metrics(baseline_dir)
        except Exception as exc:
            (output_dir / "baseline_error.txt").write_text(str(exc) + "\n", encoding="utf-8")

    report = render_report(output_dir, snapshot_dir, candidate_pool, candidate_objects, baseline_metrics, pruned_results, top_artifacts, args)
    (output_dir / "research_report.md").write_text(report, encoding="utf-8")
    print(output_dir)


if __name__ == "__main__":
    main()
