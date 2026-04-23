from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from continuation_core import simulate_portfolio
from locked_portfolio_runtime import build_trade_stats
from realistic_backtest import compute_slippage_pips, load_dataset_bundle, resolve_trade_path, simulate_reversal_module


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
]

REQUIRED_ENRICHED_COLUMNS = [
    "ask_lambda",
    "bid_lambda",
    "imbalance_ratio",
    "node_mean",
    "distance_to_node_pips",
    "local_sigma",
]

HOURS = list(range(24))
RANGE_RATIO_OPTIONS = [0.85, 1.0, 1.15, 1.25, 1.4, 1.6]
REVERSAL_WICK_OPTIONS = [2.5, 5.0, 7.5, 10.0, 15.0]
REVERSAL_DIST_OPTIONS = [1.0, 2.5, 5.0, 7.5, 10.0]
BREAKOUT_DIST_OPTIONS = [0.5, 1.0, 2.5, 5.0, 7.5]
Z_MIN_OPTIONS = [None, 0.5, 1.0, 1.5, 2.0]
STOP_OPTIONS = [10, 15, 20, 25, 30, 35, 40]
TARGET_MULTIPLIERS = [0.75, 1.0, 1.5, 2.0, 3.0]
SHIELD_TRIGGER_MULTIPLIERS = [0.4, 0.5, 0.67, 0.75]
SHIELD_LOCK_MULTIPLIERS = [0.0, 0.2, 0.33, 0.5]
TTL_OPTIONS = [30, 60, 120, 180, 240]
BREAKOUT_WICK_MAX_OPTIONS = [None, 5.0, 10.0, 15.0]
CANDIDATE_COLUMNS = [
    "family",
    "module",
    "instrument",
    "side",
    "trades",
    "trades_per_week",
    "roi_pct",
    "sharpe_ann",
    "sortino_ann",
    "max_drawdown_pct",
    "win_rate_pct",
    "profit_factor",
    "active_years",
    "last_trade_at",
    "last_trade_gap_years",
    "sample_start",
    "sample_end",
    "score",
]


@dataclass(frozen=True)
class ReversalSpec:
    instrument: str
    side: str
    hour_utc: int
    range_ratio_min: float
    wick_min: float
    dist_min: float
    ret15_min: float | None
    ret15_max: float | None
    z_min: float | None
    stop_pips: int
    target_pips: int
    shield_trigger_pips: int
    shield_lock_pips: int
    ttl_bars: int

    @property
    def family(self) -> str:
        return "reversal"

    @property
    def name(self) -> str:
        return (
            f"{self.instrument}_{self.side}_rev_h{self.hour_utc}"
            f"_rr{fmt(self.range_ratio_min)}_wick{fmt(self.wick_min)}"
            f"_dist{fmt(self.dist_min)}_ret{ret_label(self.ret15_min, self.ret15_max)}"
            f"_z{fmt_optional(self.z_min)}_sl{self.stop_pips}_tp{self.target_pips}"
            f"_shield{self.shield_trigger_pips}-{self.shield_lock_pips}_ttl{self.ttl_bars}"
        )


@dataclass(frozen=True)
class BreakoutSpec:
    instrument: str
    side: str
    hour_utc: int
    range_ratio_min: float
    close_dist_min: float
    breakout_wick_max: float | None
    ret15_min: float | None
    ret15_max: float | None
    z_min: float | None
    stop_pips: int
    target_pips: int
    shield_trigger_pips: int
    shield_lock_pips: int
    ttl_bars: int

    @property
    def family(self) -> str:
        return "breakout"

    @property
    def name(self) -> str:
        return (
            f"{self.instrument}_{self.side}_bo_h{self.hour_utc}"
            f"_rr{fmt(self.range_ratio_min)}_dist{fmt(self.close_dist_min)}"
            f"_wickmax{fmt_optional(self.breakout_wick_max)}"
            f"_ret{ret_label(self.ret15_min, self.ret15_max)}_z{fmt_optional(self.z_min)}"
            f"_sl{self.stop_pips}_tp{self.target_pips}"
            f"_shield{self.shield_trigger_pips}-{self.shield_lock_pips}_ttl{self.ttl_bars}"
        )


def fmt(value: float) -> str:
    return f"{value:g}"


def fmt_optional(value: float | None) -> str:
    return "na" if value is None else fmt(float(value))


def ret_label(ret15_min: float | None, ret15_max: float | None) -> str:
    if ret15_min is not None and ret15_max is not None:
        return f"{fmt(ret15_min)}to{fmt(ret15_max)}"
    if ret15_min is not None:
        return f"ge{fmt(ret15_min)}"
    if ret15_max is not None:
        return f"le{fmt(ret15_max)}"
    return "na"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run reversal/breakout FX research across the 10-pair universe.")
    parser.add_argument("--data-dir", default="C:/fx_data/m1")
    parser.add_argument(
        "--output-dir",
        default=str(Path(__file__).resolve().parent.parent / "output" / "multifamily_fx_research"),
    )
    parser.add_argument("--reversal-samples-per-pair-side", type=int, default=32)
    parser.add_argument("--breakout-samples-per-pair-side", type=int, default=32)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--start-balance", type=float, default=1500.0)
    parser.add_argument("--risk-pct", type=float, default=2.9)
    parser.add_argument("--max-leverage", type=float, default=35.0)
    parser.add_argument("--max-concurrent", type=int, default=2)
    parser.add_argument("--target-trades-per-week", type=float, default=5.0)
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) // 2))
    parser.add_argument("--instruments", nargs="+", default=DEFAULT_INSTRUMENTS, choices=DEFAULT_INSTRUMENTS)
    return parser.parse_args()


def make_rng(seed: int, family: str, instrument: str, side: str, draw_idx: int) -> np.random.Generator:
    digest = hashlib.sha256(f"{seed}|{family}|{instrument}|{side}|{draw_idx}".encode("utf-8")).digest()
    return np.random.default_rng(int.from_bytes(digest[:8], "big"))


def pick_target_and_shield(stop_pips: int, rng: np.random.Generator) -> tuple[int, int, int]:
    target_pips = max(1, int(round(stop_pips * float(rng.choice(TARGET_MULTIPLIERS)))))
    shield_trigger_pips = max(1, int(round(target_pips * float(rng.choice(SHIELD_TRIGGER_MULTIPLIERS)))))
    shield_lock_pips = max(0, int(round(shield_trigger_pips * float(rng.choice(SHIELD_LOCK_MULTIPLIERS)))))
    shield_lock_pips = min(shield_lock_pips, shield_trigger_pips)
    return target_pips, shield_trigger_pips, shield_lock_pips


def build_reversal_specs(instruments: list[str], samples_per_pair_side: int, seed: int) -> list[ReversalSpec]:
    specs: dict[str, ReversalSpec] = {}
    for instrument in instruments:
        for side in ("long", "short"):
            for draw_idx in range(samples_per_pair_side):
                rng = make_rng(seed, "reversal", instrument, side, draw_idx)
                stop_pips = int(rng.choice(STOP_OPTIONS))
                target_pips, shield_trigger_pips, shield_lock_pips = pick_target_and_shield(stop_pips, rng)
                if side == "long":
                    ret15_min = None
                    ret15_max = rng.choice([None, -20.0, -10.0, -5.0, -2.5])
                else:
                    ret15_min = rng.choice([None, 2.5, 5.0, 10.0, 20.0])
                    ret15_max = None
                spec = ReversalSpec(
                    instrument=instrument,
                    side=side,
                    hour_utc=int(rng.choice(HOURS)),
                    range_ratio_min=float(rng.choice(RANGE_RATIO_OPTIONS)),
                    wick_min=float(rng.choice(REVERSAL_WICK_OPTIONS)),
                    dist_min=float(rng.choice(REVERSAL_DIST_OPTIONS)),
                    ret15_min=ret15_min if ret15_min is None else float(ret15_min),
                    ret15_max=ret15_max if ret15_max is None else float(ret15_max),
                    z_min=rng.choice(Z_MIN_OPTIONS),
                    stop_pips=stop_pips,
                    target_pips=target_pips,
                    shield_trigger_pips=shield_trigger_pips,
                    shield_lock_pips=shield_lock_pips,
                    ttl_bars=int(rng.choice(TTL_OPTIONS)),
                )
                specs.setdefault(spec.name, spec)
    return list(specs.values())


def build_breakout_specs(instruments: list[str], samples_per_pair_side: int, seed: int) -> list[BreakoutSpec]:
    specs: dict[str, BreakoutSpec] = {}
    for instrument in instruments:
        for side in ("long", "short"):
            for draw_idx in range(samples_per_pair_side):
                rng = make_rng(seed, "breakout", instrument, side, draw_idx)
                stop_pips = int(rng.choice(STOP_OPTIONS))
                target_pips, shield_trigger_pips, shield_lock_pips = pick_target_and_shield(stop_pips, rng)
                if side == "long":
                    ret15_min = rng.choice([None, 2.5, 5.0, 10.0, 20.0])
                    ret15_max = None
                else:
                    ret15_min = None
                    ret15_max = rng.choice([None, -20.0, -10.0, -5.0, -2.5])
                spec = BreakoutSpec(
                    instrument=instrument,
                    side=side,
                    hour_utc=int(rng.choice(HOURS)),
                    range_ratio_min=float(rng.choice(RANGE_RATIO_OPTIONS)),
                    close_dist_min=float(rng.choice(BREAKOUT_DIST_OPTIONS)),
                    breakout_wick_max=rng.choice(BREAKOUT_WICK_MAX_OPTIONS),
                    ret15_min=ret15_min if ret15_min is None else float(ret15_min),
                    ret15_max=ret15_max if ret15_max is None else float(ret15_max),
                    z_min=rng.choice(Z_MIN_OPTIONS),
                    stop_pips=stop_pips,
                    target_pips=target_pips,
                    shield_trigger_pips=shield_trigger_pips,
                    shield_lock_pips=shield_lock_pips,
                    ttl_bars=int(rng.choice(TTL_OPTIONS)),
                )
                specs.setdefault(spec.name, spec)
    return list(specs.values())


def snapshot_paths(root: Path, instrument: str) -> list[Path]:
    return [root / f"{instrument}_5yr_m1.parquet", root / f"{instrument}_5yr_m1_bid_ask.parquet"]


def prepare_research_snapshot(source_dir: Path, snapshot_dir: Path, instruments: list[str]) -> None:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    for instrument in instruments:
        for source_path in snapshot_paths(source_dir, instrument):
            if not source_path.exists():
                raise SystemExit(f"Missing required research input: {source_path}")
            shutil.copy2(source_path, snapshot_dir / source_path.name)


def snapshot_missing_enrichment(snapshot_dir: Path, instruments: list[str]) -> list[str]:
    missing: list[str] = []
    for instrument in instruments:
        column_names = list(pq.read_schema(snapshot_dir / f"{instrument}_5yr_m1.parquet").names)
        if any(column not in column_names for column in REQUIRED_ENRICHED_COLUMNS):
            missing.append(instrument)
    return missing


def ensure_snapshot_enriched(snapshot_dir: Path, instruments: list[str]) -> None:
    missing = snapshot_missing_enrichment(snapshot_dir, instruments)
    if not missing:
        return
    command = [
        sys.executable,
        str(REPO_ROOT / "enrich_forex_research_data.py"),
        "--data-dir",
        str(snapshot_dir),
        "--instruments",
        *missing,
    ]
    subprocess.run(command, check=True)


def sample_bounds(dataset: dict[str, object]) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(dataset["df"]["timestamp"].iloc[0]).tz_convert("UTC")
    end = pd.Timestamp(dataset["df"]["timestamp"].iloc[-1]).tz_convert("UTC")
    return start, end


def total_sample_years(sample_start: pd.Timestamp, sample_end: pd.Timestamp) -> float:
    return max((sample_end - sample_start).days / 365.25, 0.01)


def score_candidate(row: dict[str, Any], sample_years: float) -> float:
    trade_count = float(row["trades"])
    sharpe = float(row["sharpe_ann"])
    win_rate = float(row["win_rate_pct"])
    trades_per_week = float(row["trades_per_week"])
    roi_pct = float(row["roi_pct"])
    drawdown_pct = abs(float(row["max_drawdown_pct"]))
    coverage_ratio = float(row["active_years"]) / max(sample_years, 1.0)
    last_trade_gap_years = float(row["last_trade_gap_years"])

    score = 0.0
    score += sharpe * 4.0
    score += min(win_rate / 70.0, 2.0) * 2.0
    score += min(trades_per_week / 0.25, 2.0) * 2.0
    score += min(max(roi_pct, 0.0) / 35.0, 2.0) * 1.5
    score += min(coverage_ratio, 1.0) * 1.5
    score -= max(drawdown_pct - 5.0, 0.0) / 2.5
    score -= max(last_trade_gap_years - 1.0, 0.0) * 1.75
    if trade_count < 20:
        score -= 1.25
    return float(score)


def simulate_breakout_module(spec: BreakoutSpec, data: dict[str, object], module_name: str) -> pd.DataFrame:
    mid = data["mid"]
    hour = data["hour"]
    weekday = data["weekday"]
    pip = float(data["pip"])

    if spec.side == "long":
        base = (
            (weekday < 5)
            & (hour == spec.hour_utc)
            & np.isfinite(data["prior_high_60"])
            & (mid["high"] > data["prior_high_60"])
            & (mid["close"] > data["prior_high_60"])
            & (mid["close"] >= data["trend_240"])
            & (data["range_60"] >= data["range_240"] * spec.range_ratio_min)
        )
        dist = (mid["close"] - data["prior_high_60"]) / pip
        wick_against = data["upper_wick"]
    else:
        base = (
            (weekday < 5)
            & (hour == spec.hour_utc)
            & np.isfinite(data["prior_low_60"])
            & (mid["low"] < data["prior_low_60"])
            & (mid["close"] < data["prior_low_60"])
            & (mid["close"] <= data["trend_240"])
            & (data["range_60"] >= data["range_240"] * spec.range_ratio_min)
        )
        dist = (data["prior_low_60"] - mid["close"]) / pip
        wick_against = data["lower_wick"]

    mask = base & (dist >= spec.close_dist_min)
    if spec.breakout_wick_max is not None:
        mask &= wick_against <= spec.breakout_wick_max
    if spec.ret15_min is not None:
        mask &= data["ret15"] >= spec.ret15_min
    if spec.ret15_max is not None:
        mask &= data["ret15"] <= spec.ret15_max
    if spec.z_min is not None:
        mask &= np.abs(data["z60"]) >= spec.z_min

    entries = np.flatnonzero(mask)
    trades: list[dict[str, object]] = []
    next_allowed = 0

    for idx in entries:
        entry_idx = idx + 1
        if entry_idx >= len(mid["open"]) - 1 or entry_idx < next_allowed:
            continue

        slippage_pips = compute_slippage_pips(float(data["range_60"][entry_idx]), float(data["range_240"][entry_idx]))
        slippage = slippage_pips * pip
        if spec.side == "long":
            entry_price = float(data["ask"]["open"][entry_idx] + slippage)
            stop_loss = entry_price - spec.stop_pips * pip
            take_profit = entry_price + spec.target_pips * pip
            shield_trigger = entry_price + spec.shield_trigger_pips * pip
            shield_lock = entry_price + spec.shield_lock_pips * pip
        else:
            entry_price = float(data["bid"]["open"][entry_idx] - slippage)
            stop_loss = entry_price + spec.stop_pips * pip
            take_profit = entry_price - spec.target_pips * pip
            shield_trigger = entry_price - spec.shield_trigger_pips * pip
            shield_lock = entry_price - spec.shield_lock_pips * pip

        exit_idx, exit_price, shielded = resolve_trade_path(
            spec.side,
            entry_price,
            stop_loss,
            take_profit,
            shield_trigger,
            shield_lock,
            data,
            entry_idx,
            spec.ttl_bars,
        )
        pnl_pips = (exit_price - entry_price) / pip if spec.side == "long" else (entry_price - exit_price) / pip
        trades.append(
            {
                "module": module_name,
                "instrument": spec.instrument,
                "side": spec.side,
                "entry_time": pd.Timestamp(data["timestamp"][entry_idx]),
                "exit_time": pd.Timestamp(data["timestamp"][exit_idx]),
                "entry_price": entry_price,
                "exit_price": float(exit_price),
                "pnl_pips": float(pnl_pips),
                "stop_pips": float(spec.stop_pips),
                "pip_size": pip,
                "entry_slippage_pips": slippage_pips,
                "shielded": shielded,
                "used_inferred_bid_ask": bool(data["inferred_bid_ask"]),
            }
        )
        next_allowed = exit_idx + 1

    return pd.DataFrame(trades)


def evaluate_spec(
    family: str,
    spec: ReversalSpec | BreakoutSpec,
    datasets: dict[str, dict[str, object]],
    start_balance: float,
    risk_pct: float,
    max_leverage: float,
    max_concurrent: int,
) -> dict[str, Any]:
    if family == "reversal":
        trades = simulate_reversal_module(spec, datasets[spec.instrument], spec.name)
    elif family == "breakout":
        trades = simulate_breakout_module(spec, datasets[spec.instrument], spec.name)
    else:
        raise ValueError(f"Unknown family: {family}")

    sample_start, sample_end = sample_bounds(datasets[spec.instrument])
    sample_weeks = max((sample_end - sample_start).days / 7.0, 1.0)
    sample_years = total_sample_years(sample_start, sample_end)

    if trades.empty:
        row = {
            "family": family,
            "module": spec.name,
            "instrument": spec.instrument,
            "side": spec.side,
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
        }
    else:
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
        row = {
            "family": family,
            "module": spec.name,
            "instrument": spec.instrument,
            "side": spec.side,
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
            "last_trade_gap_years": max((sample_end - last_trade_at).days / 365.25, 0.0),
            "sample_start": sample_start.isoformat(),
            "sample_end": sample_end.isoformat(),
        }

    for key, value in asdict(spec).items():
        row[key] = value
    row["score"] = score_candidate(row, sample_years)
    return row


def evaluate_family(
    family: str,
    specs: list[ReversalSpec | BreakoutSpec],
    datasets: dict[str, dict[str, object]],
    start_balance: float,
    risk_pct: float,
    max_leverage: float,
    max_concurrent: int,
    workers: int,
) -> pd.DataFrame:
    if not specs:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    def run(spec: ReversalSpec | BreakoutSpec) -> dict[str, Any]:
        return evaluate_spec(family, spec, datasets, start_balance, risk_pct, max_leverage, max_concurrent)

    if workers <= 1:
        rows = [run(spec) for spec in specs]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            rows = list(executor.map(run, specs))
    if not rows:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)
    return pd.DataFrame(rows).sort_values(
        ["score", "sharpe_ann", "win_rate_pct", "trades"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)


def select_best_by_pair(candidates: pd.DataFrame) -> pd.DataFrame:
    selected_rows: list[pd.Series] = []
    for instrument, group in candidates.groupby("instrument", sort=True):
        recent = group.loc[group["last_trade_gap_years"] <= 2.0]
        if not recent.empty:
            robust = recent.loc[recent["trades"] >= 20]
            choice_group = robust if not robust.empty else recent
        else:
            choice_group = group
        selected_rows.append(choice_group.iloc[0])
    return pd.DataFrame(selected_rows).sort_values("score", ascending=False).reset_index(drop=True)


def select_diversified_portfolio(candidates: pd.DataFrame, target_trades_per_week: float) -> pd.DataFrame:
    selected: list[pd.Series] = []
    per_instrument: dict[str, int] = {}
    per_family: dict[str, int] = {}
    running_density = 0.0
    filtered = candidates.loc[
        (candidates["last_trade_gap_years"] <= 2.0)
        & (candidates["sharpe_ann"] > -0.25)
        & (candidates["trades"] >= 10)
    ]
    if filtered.empty:
        filtered = candidates.head(20)

    for _, row in filtered.iterrows():
        instrument = str(row["instrument"])
        family = str(row["family"])
        if per_instrument.get(instrument, 0) >= 2:
            continue
        if per_family.get(family, 0) >= 8:
            continue
        selected.append(row)
        per_instrument[instrument] = per_instrument.get(instrument, 0) + 1
        per_family[family] = per_family.get(family, 0) + 1
        running_density += float(row["trades_per_week"])
        if running_density >= target_trades_per_week:
            break
    return pd.DataFrame(selected).reset_index(drop=True)


def run_portfolio_from_rows(
    rows: pd.DataFrame,
    spec_map: dict[str, ReversalSpec | BreakoutSpec],
    datasets: dict[str, dict[str, object]],
    start_balance: float,
    risk_pct: float,
    max_leverage: float,
    max_concurrent: int,
) -> dict[str, Any]:
    trade_frames: list[pd.DataFrame] = []
    for row in rows.to_dict(orient="records"):
        spec = spec_map[str(row["module"])]
        family = str(row["family"])
        if family == "reversal":
            frame = simulate_reversal_module(spec, datasets[spec.instrument], spec.name)
        elif family == "breakout":
            frame = simulate_breakout_module(spec, datasets[spec.instrument], spec.name)
        else:
            raise ValueError(f"Unknown family: {family}")
        if not frame.empty:
            trade_frames.append(frame)

    trade_table = pd.concat(trade_frames, ignore_index=True) if trade_frames else pd.DataFrame()
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


def write_portfolio_pack(target_dir: Path, rows: pd.DataFrame, artifacts: dict[str, Any]) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    rows.to_csv(target_dir / "selected_candidates.csv", index=False)
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


def summary_lines(label: str, artifacts: dict[str, Any]) -> list[str]:
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


def render_report(
    candidates: pd.DataFrame,
    best_by_pair: pd.DataFrame,
    diversified: pd.DataFrame,
    pair_portfolio: dict[str, Any],
    diversified_portfolio: dict[str, Any],
    output_dir: Path,
    args: argparse.Namespace,
) -> str:
    recent = candidates.loc[candidates["last_trade_gap_years"] <= 2.0]
    lines = [
        "# Multifamily FX Research",
        "",
        "This sweep searches reversal and breakout structures on top of the current M1 bid/ask execution layer.",
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
        f"- Instruments: `{', '.join(args.instruments)}`",
        f"- Reversal samples per pair-side: `{args.reversal_samples_per_pair_side}`",
        f"- Breakout samples per pair-side: `{args.breakout_samples_per_pair_side}`",
        f"- Candidate count: `{len(candidates)}`",
        f"- Recent candidate count: `{len(recent)}`",
        f"- Output dir: `{output_dir}`",
        "",
        "## Best Individual Candidates",
        "",
        markdown_table(
            candidates.head(20)[
                [
                    "family",
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
        ),
        "",
        "## Best By Pair",
        "",
        markdown_table(
            best_by_pair[
                [
                    "family",
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
    lines.extend(summary_lines("Best-By-Pair Portfolio", pair_portfolio))
    lines.extend(summary_lines("Diversified Portfolio", diversified_portfolio))
    lines.extend(
        [
            "## Notes",
            "",
            "- The old reference pack in this repo points at a different data source (`.../razorback`), so it is not directly comparable to the current `C:\\fx_data\\m1` run surface.",
            "- This report is intentionally honest: if a family cannot stay recent, frequent, and profitable at once, it does not get treated as a real edge.",
            "- The next step after this sweep is regime attribution and family blending with the earlier continuation search, not pretending one isolated family solved the problem.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    snapshot_dir = output_dir / "research_data"
    source_dir = Path(args.data_dir).expanduser().resolve()
    prepare_research_snapshot(source_dir, snapshot_dir, args.instruments)
    ensure_snapshot_enriched(snapshot_dir, args.instruments)

    datasets = load_dataset_bundle(snapshot_dir, args.instruments)
    reversal_specs = build_reversal_specs(args.instruments, args.reversal_samples_per_pair_side, args.seed)
    breakout_specs = build_breakout_specs(args.instruments, args.breakout_samples_per_pair_side, args.seed)
    spec_map: dict[str, ReversalSpec | BreakoutSpec] = {spec.name: spec for spec in reversal_specs + breakout_specs}

    reversal_candidates = evaluate_family(
        "reversal",
        reversal_specs,
        datasets,
        args.start_balance,
        args.risk_pct,
        args.max_leverage,
        args.max_concurrent,
        args.workers,
    )
    breakout_candidates = evaluate_family(
        "breakout",
        breakout_specs,
        datasets,
        args.start_balance,
        args.risk_pct,
        args.max_leverage,
        args.max_concurrent,
        args.workers,
    )

    reversal_candidates.to_csv(output_dir / "reversal_candidates.csv", index=False)
    breakout_candidates.to_csv(output_dir / "breakout_candidates.csv", index=False)

    candidate_frames = [frame for frame in (reversal_candidates, breakout_candidates) if not frame.empty]
    if candidate_frames:
        candidates = (
            pd.concat(candidate_frames, ignore_index=True)
            .sort_values(["score", "sharpe_ann", "win_rate_pct", "trades"], ascending=[False, False, False, False])
            .reset_index(drop=True)
        )
    else:
        candidates = pd.DataFrame(columns=CANDIDATE_COLUMNS)
    candidates.to_csv(output_dir / "all_candidates.csv", index=False)

    best_by_pair = select_best_by_pair(candidates)
    best_by_pair.to_csv(output_dir / "best_by_pair.csv", index=False)

    diversified = select_diversified_portfolio(candidates, args.target_trades_per_week)
    diversified.to_csv(output_dir / "diversified_portfolio_candidates.csv", index=False)

    pair_portfolio = run_portfolio_from_rows(
        best_by_pair,
        spec_map,
        datasets,
        args.start_balance,
        args.risk_pct,
        args.max_leverage,
        args.max_concurrent,
    )
    diversified_portfolio = run_portfolio_from_rows(
        diversified,
        spec_map,
        datasets,
        args.start_balance,
        args.risk_pct,
        args.max_leverage,
        args.max_concurrent,
    )

    write_portfolio_pack(output_dir / "best_by_pair_portfolio", best_by_pair, pair_portfolio)
    write_portfolio_pack(output_dir / "diversified_portfolio", diversified, diversified_portfolio)

    report = render_report(candidates, best_by_pair, diversified, pair_portfolio, diversified_portfolio, output_dir, args)
    (output_dir / "research_report.md").write_text(report, encoding="utf-8")
    print(output_dir)


if __name__ == "__main__":
    main()
