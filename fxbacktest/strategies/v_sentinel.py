from __future__ import annotations

import json
import math
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from continuation_core import (
    ExecutionProfile,
    adverse_price_adjustment,
    deterministic_fill_passes,
    load_datasets,
    resolve_execution_profile,
    simulate_portfolio,
)
from locked_portfolio_runtime import (
    LockedPortfolioConfig,
    build_group_table,
    build_max_drawdown_point,
    build_trade_stats,
    load_locked_config,
    write_locked_portfolio_artifacts,
)
from realistic_backtest import compute_slippage_pips


VSENTINEL_LADDER_SCALE_FRACTIONS = (0.33, 0.33, 0.34)
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


@dataclass(frozen=True)
class VSentinelParams:
    vol_ratio: float
    imb_threshold: float
    node_dist: float
    mom_pips: float
    stop_pips: int
    ladder_pips: tuple[int, int, int]
    trail_pips: int
    ttl_bars: int
    sigma_ratio: float = 1.0


@dataclass(frozen=True)
class SessionWindow:
    label: str
    hour_start: int
    hour_end: int


@dataclass(frozen=True)
class VSentinelSpec:
    instrument: str
    session: str
    side: str
    hour_start: int
    hour_end: int
    vol_ratio: float
    imb_threshold: float
    node_dist: float
    mom_pips: float
    stop_loss_pips: int
    ladder_pips: tuple[int, int, int]
    ladder_scale_fractions: tuple[float, float, float]
    trail_stop_pips: int
    ttl_bars: int
    local_sigma_ratio_min: float = 1.0

    @property
    def name(self) -> str:
        ladder = "-".join(str(value) for value in self.ladder_pips)
        scales = "-".join(f"{value:.2f}" for value in self.ladder_scale_fractions)
        return (
            f"vsentinel_{self.instrument}_{self.session}_{self.side}"
            f"_h{self.hour_start}-{self.hour_end}"
            f"_vr{self.vol_ratio:g}_imb{self.imb_threshold:g}_node{self.node_dist:g}"
            f"_mom{self.mom_pips:g}_sl{self.stop_loss_pips}"
            f"_tp{ladder}_scale{scales}_trail{self.trail_stop_pips}_ttl{self.ttl_bars}"
        )


PAIR_PARAMS: dict[str, VSentinelParams] = {
    "eurusd": VSentinelParams(1.25, 2.5, 3.0, 8.0, 15, (15, 30, 45), 10, 180),
    "gbpusd": VSentinelParams(1.25, 3.0, 4.0, 12.0, 20, (20, 40, 60), 15, 180),
    "usdjpy": VSentinelParams(1.20, 2.5, 5.0, 15.0, 25, (25, 50, 75), 15, 240),
    "audusd": VSentinelParams(1.30, 2.0, 4.0, 8.0, 12, (12, 24, 36), 8, 150),
    "usdcad": VSentinelParams(1.25, 2.5, 4.0, 10.0, 15, (15, 30, 45), 10, 180),
    "usdchf": VSentinelParams(1.20, 2.5, 3.0, 8.0, 12, (12, 24, 36), 8, 150),
    "eurjpy": VSentinelParams(1.25, 2.5, 5.0, 15.0, 25, (25, 50, 75), 15, 240),
    "eurgbp": VSentinelParams(1.30, 3.0, 3.0, 6.0, 10, (10, 20, 30), 7, 120),
    "eurchf": VSentinelParams(1.20, 2.5, 3.0, 8.0, 12, (12, 24, 36), 8, 150),
    "audjpy": VSentinelParams(1.25, 2.5, 5.0, 15.0, 25, (25, 50, 75), 15, 240),
}

PAIR_SESSIONS: dict[str, tuple[SessionWindow, ...]] = {
    "eurusd": (SessionWindow("london", 7, 10), SessionWindow("ny", 12, 15)),
    "gbpusd": (SessionWindow("london", 7, 10), SessionWindow("ny", 12, 15)),
    "usdjpy": (SessionWindow("ny", 12, 15), SessionWindow("asia", 0, 3)),
    "audusd": (SessionWindow("asia", 22, 2), SessionWindow("london", 7, 10)),
    "usdcad": (SessionWindow("ny", 12, 15),),
    "usdchf": (SessionWindow("london", 7, 10), SessionWindow("ny", 12, 15)),
    "eurjpy": (SessionWindow("london", 7, 10), SessionWindow("ny", 12, 15)),
    "eurgbp": (SessionWindow("london", 7, 10),),
    "eurchf": (SessionWindow("london", 7, 10),),
    "audjpy": (SessionWindow("asia", 0, 3), SessionWindow("london", 7, 10)),
}


def session_mask(hours: np.ndarray, start: int, end: int) -> np.ndarray:
    if start <= end:
        return (hours >= start) & (hours <= end)
    return (hours >= start) | (hours <= end)


def required_instruments(specs: list[VSentinelSpec]) -> list[str]:
    required = {spec.instrument for spec in specs}
    required.update({"usdjpy", "gbpusd"})
    return sorted(required)


def build_v_sentinel_specs(instruments: list[str] | None = None) -> list[VSentinelSpec]:
    selected = instruments or DEFAULT_INSTRUMENTS
    specs: list[VSentinelSpec] = []
    for instrument in selected:
        params = PAIR_PARAMS[instrument]
        for window in PAIR_SESSIONS[instrument]:
            for side in ("long", "short"):
                specs.append(
                    VSentinelSpec(
                        instrument=instrument,
                        session=window.label,
                        side=side,
                        hour_start=window.hour_start,
                        hour_end=window.hour_end,
                        vol_ratio=params.vol_ratio,
                        imb_threshold=params.imb_threshold,
                        node_dist=params.node_dist,
                        mom_pips=params.mom_pips,
                        stop_loss_pips=params.stop_pips,
                        ladder_pips=params.ladder_pips,
                        ladder_scale_fractions=VSENTINEL_LADDER_SCALE_FRACTIONS,
                        trail_stop_pips=params.trail_pips,
                        ttl_bars=params.ttl_bars,
                        local_sigma_ratio_min=params.sigma_ratio,
                    )
                )
    return specs


class VSentinelStrategy:
    def __init__(self, spec: VSentinelSpec):
        self.spec = spec

    def generate_signals(self, data: dict[str, object]) -> np.ndarray:
        spec = self.spec
        enriched = data["enriched_df"]
        mid = data["mid"]
        hour = data["hour"]
        weekday = data["weekday"]
        local_sigma_ma_240 = data["local_sigma_ma_240"]
        ret60 = data["ret60"]
        ema_240 = data["ema_240"]

        ask_lambda = enriched["ask_lambda"].to_numpy(np.float64)
        bid_lambda = enriched["bid_lambda"].to_numpy(np.float64)
        distance = enriched["distance_to_node_pips"].to_numpy(np.float64)
        local_sigma = enriched["local_sigma"].to_numpy(np.float64)
        mid_close = mid["close"]

        in_session = session_mask(hour, spec.hour_start, spec.hour_end) & (weekday < 5)
        elevated_vol = data["range_60"] > (data["range_240"] * spec.vol_ratio)
        near_node = distance < spec.node_dist
        sigma_ok = local_sigma > (local_sigma_ma_240 * spec.local_sigma_ratio_min)
        trend_long = mid_close > ema_240
        trend_short = mid_close < ema_240
        momentum_long = ret60 > spec.mom_pips
        momentum_short = ret60 < -spec.mom_pips

        if spec.side == "long":
            ratio = np.divide(
                ask_lambda,
                bid_lambda,
                out=np.full_like(ask_lambda, np.inf),
                where=bid_lambda > 1e-12,
            )
            imbalance_ok = ratio > spec.imb_threshold
            trend_ok = trend_long
            momentum_ok = momentum_long
        else:
            ratio = np.divide(
                bid_lambda,
                ask_lambda,
                out=np.full_like(bid_lambda, np.inf),
                where=ask_lambda > 1e-12,
            )
            imbalance_ok = ratio > spec.imb_threshold
            trend_ok = trend_short
            momentum_ok = momentum_short

        return in_session & elevated_vol & near_node & sigma_ok & imbalance_ok & trend_ok & momentum_ok

    def simulate_exit(
        self,
        entry_idx: int,
        data: dict[str, object],
        entry_price: float,
        stop_level: float,
        execution_profile: ExecutionProfile,
    ) -> dict[str, object]:
        spec = self.spec
        bid = data["bid"]
        ask = data["ask"]
        mid = data["mid"]
        pip = float(data["pip"])
        end_idx = min(len(mid["open"]) - 1, entry_idx + spec.ttl_bars)
        remaining = 1.0
        total_pnl_pips = 0.0
        highest_price = entry_price
        lowest_price = entry_price
        trailing_active = False
        trailing_stop = stop_level
        partials: list[dict[str, object]] = []
        hit_levels: set[int] = set()
        exit_idx = end_idx
        exit_price = float(bid["close"][end_idx] if spec.side == "long" else ask["close"][end_idx])

        for bar_idx in range(entry_idx, end_idx + 1):
            long_high = float(bid["high"][bar_idx])
            long_low = float(bid["low"][bar_idx])
            short_high = float(ask["high"][bar_idx])
            short_low = float(ask["low"][bar_idx])
            effective_spread_pips = float(data["effective_spread_pips"][bar_idx])
            exit_slippage_pips = compute_slippage_pips(
                float(data["range_60"][bar_idx]),
                float(data["range_240"][bar_idx]),
                base_slippage=execution_profile.base_slippage_pips,
                vol_sensitivity=execution_profile.vol_factor,
            )

            if spec.side == "long":
                for level_idx, (level, scale_fraction) in enumerate(zip(spec.ladder_pips, spec.ladder_scale_fractions)):
                    if level_idx in hit_levels or remaining <= 1e-12:
                        continue
                    target_price = entry_price + level * pip
                    if long_high >= target_price:
                        fill_fraction = min(remaining, remaining * scale_fraction)
                        if fill_fraction <= 1e-12:
                            continue
                        realized_target_price = adverse_price_adjustment(
                            "long",
                            target_price,
                            pip,
                            effective_spread_pips,
                            exit_slippage_pips,
                            execution_profile.spread_multiplier,
                            is_entry=False,
                        )
                        realized_level = (realized_target_price - entry_price) / pip
                        total_pnl_pips += realized_level * fill_fraction
                        remaining -= fill_fraction
                        hit_levels.add(level_idx)
                        partials.append(
                            {
                                "time": pd.Timestamp(data["timestamp"][bar_idx]).isoformat(),
                                "price": realized_target_price,
                                "fraction": float(fill_fraction),
                                "pips": float(realized_level),
                            }
                        )
                        highest_price = max(highest_price, long_high)
                        if not trailing_active:
                            trailing_active = True
                            trailing_stop = realized_target_price - spec.trail_stop_pips * pip

                if trailing_active and remaining > 1e-12:
                    highest_price = max(highest_price, long_high)
                    trailing_stop = max(trailing_stop, highest_price - spec.trail_stop_pips * pip)

                active_stop = trailing_stop if trailing_active else stop_level
                if remaining > 1e-12 and long_low <= active_stop:
                    realized_stop = adverse_price_adjustment(
                        "long",
                        active_stop,
                        pip,
                        effective_spread_pips,
                        exit_slippage_pips,
                        execution_profile.spread_multiplier,
                        is_entry=False,
                    )
                    exit_pips = (realized_stop - entry_price) / pip
                    total_pnl_pips += exit_pips * remaining
                    exit_idx = bar_idx
                    exit_price = realized_stop
                    partials.append(
                        {
                            "time": pd.Timestamp(data["timestamp"][bar_idx]).isoformat(),
                            "price": realized_stop,
                            "fraction": float(remaining),
                            "pips": float(exit_pips),
                        }
                    )
                    remaining = 0.0
                    break
            else:
                for level_idx, (level, scale_fraction) in enumerate(zip(spec.ladder_pips, spec.ladder_scale_fractions)):
                    if level_idx in hit_levels or remaining <= 1e-12:
                        continue
                    target_price = entry_price - level * pip
                    if short_low <= target_price:
                        fill_fraction = min(remaining, remaining * scale_fraction)
                        if fill_fraction <= 1e-12:
                            continue
                        realized_target_price = adverse_price_adjustment(
                            "short",
                            target_price,
                            pip,
                            effective_spread_pips,
                            exit_slippage_pips,
                            execution_profile.spread_multiplier,
                            is_entry=False,
                        )
                        realized_level = (entry_price - realized_target_price) / pip
                        total_pnl_pips += realized_level * fill_fraction
                        remaining -= fill_fraction
                        hit_levels.add(level_idx)
                        partials.append(
                            {
                                "time": pd.Timestamp(data["timestamp"][bar_idx]).isoformat(),
                                "price": realized_target_price,
                                "fraction": float(fill_fraction),
                                "pips": float(realized_level),
                            }
                        )
                        lowest_price = min(lowest_price, short_low)
                        if not trailing_active:
                            trailing_active = True
                            trailing_stop = realized_target_price + spec.trail_stop_pips * pip

                if trailing_active and remaining > 1e-12:
                    lowest_price = min(lowest_price, short_low)
                    trailing_stop = min(trailing_stop, lowest_price + spec.trail_stop_pips * pip)

                active_stop = trailing_stop if trailing_active else stop_level
                if remaining > 1e-12 and short_high >= active_stop:
                    realized_stop = adverse_price_adjustment(
                        "short",
                        active_stop,
                        pip,
                        effective_spread_pips,
                        exit_slippage_pips,
                        execution_profile.spread_multiplier,
                        is_entry=False,
                    )
                    exit_pips = (entry_price - realized_stop) / pip
                    total_pnl_pips += exit_pips * remaining
                    exit_idx = bar_idx
                    exit_price = realized_stop
                    partials.append(
                        {
                            "time": pd.Timestamp(data["timestamp"][bar_idx]).isoformat(),
                            "price": realized_stop,
                            "fraction": float(remaining),
                            "pips": float(exit_pips),
                        }
                    )
                    remaining = 0.0
                    break

        if remaining > 1e-12:
            ttl_raw_price = float(bid["close"][end_idx] if spec.side == "long" else ask["close"][end_idx])
            ttl_effective_spread_pips = float(data["effective_spread_pips"][end_idx])
            ttl_slippage_pips = compute_slippage_pips(
                float(data["range_60"][end_idx]),
                float(data["range_240"][end_idx]),
                base_slippage=execution_profile.base_slippage_pips,
                vol_sensitivity=execution_profile.vol_factor,
            )
            ttl_price = adverse_price_adjustment(
                spec.side,
                ttl_raw_price,
                pip,
                ttl_effective_spread_pips,
                ttl_slippage_pips,
                execution_profile.spread_multiplier,
                is_entry=False,
            )
            exit_pips = (ttl_price - entry_price) / pip if spec.side == "long" else (entry_price - ttl_price) / pip
            total_pnl_pips += exit_pips * remaining
            exit_idx = end_idx
            exit_price = ttl_price
            partials.append(
                {
                    "time": pd.Timestamp(data["timestamp"][end_idx]).isoformat(),
                    "price": ttl_price,
                    "fraction": float(remaining),
                    "pips": float(exit_pips),
                }
            )

        return {
            "module": spec.name,
            "instrument": spec.instrument,
            "side": spec.side,
            "entry_time": pd.Timestamp(data["timestamp"][entry_idx]),
            "exit_time": pd.Timestamp(data["timestamp"][exit_idx]),
            "entry_price": float(entry_price),
            "exit_price": float(exit_price),
            "pnl_pips": float(total_pnl_pips),
            "stop_pips": float(spec.stop_loss_pips),
            "pip_size": pip,
            "partial_fills": partials,
            "exit_idx": int(exit_idx),
            "used_inferred_bid_ask": bool(data["inferred_bid_ask"]),
        }


def simulate_v_sentinel_spec(
    spec: VSentinelSpec,
    data: dict[str, object],
    execution_profile: ExecutionProfile | None = None,
) -> pd.DataFrame:
    profile = execution_profile or resolve_execution_profile(None)
    strategy = VSentinelStrategy(spec)
    mid = data["mid"]
    ask = data["ask"]
    bid = data["bid"]
    pip = float(data["pip"])
    entries = np.flatnonzero(strategy.generate_signals(data))
    trades: list[dict[str, object]] = []
    next_allowed = 0

    for idx in entries:
        entry_idx = idx + 1 + profile.entry_delay_bars
        if entry_idx >= len(mid["open"]) - 1 or entry_idx < next_allowed:
            continue

        entry_time = pd.Timestamp(data["timestamp"][entry_idx])
        if not deterministic_fill_passes(spec.name, spec.instrument, entry_time, profile.fill_probability):
            next_allowed = entry_idx + 1
            continue

        effective_spread_pips = float(data["effective_spread_pips"][entry_idx])
        slippage_pips = compute_slippage_pips(
            float(data["range_60"][entry_idx]),
            float(data["range_240"][entry_idx]),
            base_slippage=profile.base_slippage_pips,
            vol_sensitivity=profile.vol_factor,
        )

        if spec.side == "long":
            entry_price = adverse_price_adjustment(
                "long",
                float(ask["open"][entry_idx]),
                pip,
                effective_spread_pips,
                slippage_pips,
                profile.spread_multiplier,
                is_entry=True,
            )
            stop_level = entry_price - spec.stop_loss_pips * pip
        else:
            entry_price = adverse_price_adjustment(
                "short",
                float(bid["open"][entry_idx]),
                pip,
                effective_spread_pips,
                slippage_pips,
                profile.spread_multiplier,
                is_entry=True,
            )
            stop_level = entry_price + spec.stop_loss_pips * pip

        trade = strategy.simulate_exit(entry_idx, data, entry_price, stop_level, profile)
        trade["entry_slippage_pips"] = slippage_pips
        trade["fill_probability"] = profile.fill_probability
        trade["spread_multiplier"] = profile.spread_multiplier
        trade["entry_delay_bars"] = profile.entry_delay_bars
        trade["execution_scenario"] = profile.name
        trades.append(trade)
        next_allowed = int(trade["exit_idx"]) + 1

    if not trades:
        return pd.DataFrame()

    frame = pd.DataFrame(trades)
    frame["partial_fills"] = frame["partial_fills"].apply(json.dumps)
    return frame.drop(columns=["exit_idx"])


def _simulate_specs_parallel(
    specs: list[VSentinelSpec],
    datasets: dict[str, dict[str, object]],
    workers: int,
    execution_profile: ExecutionProfile,
) -> pd.DataFrame:
    def run_spec(spec: VSentinelSpec) -> pd.DataFrame:
        return simulate_v_sentinel_spec(spec, datasets[spec.instrument], execution_profile=execution_profile)

    if workers <= 1 or len(specs) <= 1:
        frames = [run_spec(spec) for spec in specs]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            frames = list(executor.map(run_spec, specs))

    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def serialize_specs(specs: list[VSentinelSpec]) -> list[dict[str, Any]]:
    serialized: list[dict[str, Any]] = []
    for spec in specs:
        row = asdict(spec)
        row["name"] = spec.name
        serialized.append(row)
    return serialized


def infer_instruments_from_config(config_path: Path) -> list[str]:
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    configured = raw.get("instruments", DEFAULT_INSTRUMENTS)
    selected = [str(instrument).lower() for instrument in configured]
    return [instrument for instrument in selected if instrument in PAIR_PARAMS]


def build_v_sentinel_artifacts(
    config: LockedPortfolioConfig,
    config_path: Path,
    workers: int | None = None,
) -> dict[str, Any]:
    selected_instruments = infer_instruments_from_config(config_path)
    specs = build_v_sentinel_specs(selected_instruments)
    instrument_universe = required_instruments(specs)
    datasets = load_datasets(config.data_dir, instrument_universe)
    worker_count = max(1, min(workers or min(len(specs), os.cpu_count() or 1), len(specs) or 1))
    trade_table = _simulate_specs_parallel(specs, datasets, worker_count, config.execution_profile)
    summary, module_table, weekly_table, monthly_table, yearly_table, filled_df = simulate_portfolio(
        trade_table,
        datasets,
        config.start_balance,
        config.risk_pct / 100.0,
        config.max_leverage,
        config.max_concurrent,
    )

    run_config = {
        "name": config.name,
        "description": config.description,
        "config_path": str(config.config_path),
        "data_dir": str(config.data_dir),
        "output_dir": str(config.output_dir),
        "algorithm": "v_sentinel",
        "start_balance": config.start_balance,
        "risk_pct": config.risk_pct,
        "max_leverage": config.max_leverage,
        "max_concurrent": config.max_concurrent,
        "execution_scenario": config.execution_profile.name,
        "execution_profile": asdict(config.execution_profile),
        "module_count": len(specs),
        "pair_count": len(selected_instruments),
        "workers": worker_count,
        "instrument_universe": instrument_universe,
        "trade_count_raw": int(len(trade_table)),
        "trade_count_filled": int(len(filled_df)),
    }

    return {
        "config": config,
        "run_config": run_config,
        "specs": specs,
        "selected_modules": serialize_specs(specs),
        "summary": summary,
        "module_table": module_table,
        "weekly_table": weekly_table,
        "monthly_table": monthly_table,
        "yearly_table": yearly_table,
        "trade_stats": build_trade_stats(filled_df),
        "instrument_table": build_group_table(filled_df, "instrument"),
        "side_table": build_group_table(filled_df, "side"),
        "max_drawdown_point": build_max_drawdown_point(filled_df, config.start_balance),
        "trade_ledger": filled_df,
    }


def run_v_sentinel_portfolio(
    config_path: Path,
    data_dir_override: Path | None = None,
    output_dir_override: Path | None = None,
    start_balance: float | None = None,
    risk_pct: float | None = None,
    max_leverage: float | None = None,
    max_concurrent: int | None = None,
    workers: int | None = None,
    scenario: str | None = None,
) -> Path:
    config = load_locked_config(
        config_path,
        data_dir_override=data_dir_override,
        output_dir_override=output_dir_override,
        start_balance=start_balance,
        risk_pct=risk_pct,
        max_leverage=max_leverage,
        max_concurrent=max_concurrent,
        scenario=scenario,
    )
    artifacts = build_v_sentinel_artifacts(config, config_path.expanduser().resolve(), workers=workers)
    return write_locked_portfolio_artifacts(artifacts)
