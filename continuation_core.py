from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from native_acceleration import simulate_trade_path_accelerated
from realistic_backtest import compute_slippage_pips, load_dataset_bundle


REQUIRED_ENRICHED_COLUMNS = [
    "ask_lambda",
    "bid_lambda",
    "imbalance_ratio",
    "node_mean",
    "distance_to_node_pips",
    "local_sigma",
]


@dataclass(frozen=True)
class ExecutionProfile:
    name: str
    base_slippage_pips: float
    vol_factor: float
    entry_delay_bars: int
    fill_probability: float
    spread_multiplier: float


EXECUTION_SCENARIOS: dict[str, ExecutionProfile] = {
    "base": ExecutionProfile(
        name="base",
        base_slippage_pips=0.0,
        vol_factor=0.0,
        entry_delay_bars=0,
        fill_probability=1.0,
        spread_multiplier=1.0,
    ),
    "conservative": ExecutionProfile(
        name="conservative",
        base_slippage_pips=0.3,
        vol_factor=0.5,
        entry_delay_bars=1,
        fill_probability=0.9,
        spread_multiplier=1.2,
    ),
    "hard": ExecutionProfile(
        name="hard",
        base_slippage_pips=0.6,
        vol_factor=1.0,
        entry_delay_bars=2,
        fill_probability=0.75,
        spread_multiplier=1.5,
    ),
}


def resolve_execution_profile(name: str | None) -> ExecutionProfile:
    scenario_name = (name or "base").strip().lower()
    try:
        return EXECUTION_SCENARIOS[scenario_name]
    except KeyError as exc:
        raise SystemExit(f"Unknown execution scenario: {scenario_name}. Expected one of: {', '.join(EXECUTION_SCENARIOS)}") from exc


def deterministic_fill_passes(
    module_name: str,
    instrument: str,
    entry_timestamp: object,
    fill_probability: float,
) -> bool:
    if fill_probability >= 1.0:
        return True
    if fill_probability <= 0.0:
        return False

    timestamp = pd.Timestamp(entry_timestamp).isoformat()
    digest = hashlib.sha256(f"{module_name}|{instrument}|{timestamp}".encode("utf-8")).digest()
    threshold = int.from_bytes(digest[:8], "big") / float(2**64)
    return threshold <= fill_probability


def adverse_price_adjustment(
    side: str,
    raw_price: float,
    pip: float,
    effective_spread_pips: float,
    slippage_pips: float,
    spread_multiplier: float,
    is_entry: bool,
) -> float:
    extra_half_spread_pips = max(spread_multiplier - 1.0, 0.0) * effective_spread_pips / 2.0
    total_penalty = (extra_half_spread_pips + max(slippage_pips, 0.0)) * pip
    if total_penalty <= 0.0:
        return float(raw_price)

    if side == "long":
        return float(raw_price + total_penalty) if is_entry else float(raw_price - total_penalty)
    return float(raw_price - total_penalty) if is_entry else float(raw_price + total_penalty)


def can_use_native_trade_path(profile: ExecutionProfile) -> bool:
    return (
        profile.base_slippage_pips <= 1e-12
        and profile.vol_factor <= 1e-12
        and abs(profile.spread_multiplier - 1.0) <= 1e-12
    )


@dataclass(frozen=True)
class ContinuationSpec:
    instrument: str
    side: str
    hour_start: int
    hour_end: int
    range_ratio_min: float
    imbalance_min: float
    node_distance_max: float
    ret60_min_abs: float
    local_sigma_ratio_min: float
    stop_loss_pips: int
    ladder_pips: tuple[int, int, int]
    ladder_fractions: tuple[float, float, float]
    trail_stop_pips: int
    ttl_bars: int

    @property
    def name(self) -> str:
        ladder = "-".join(str(v) for v in self.ladder_pips)
        fracs = "-".join(f"{v:.1f}" for v in self.ladder_fractions)
        return (
            f"{self.instrument}_{self.side}_h{self.hour_start}-{self.hour_end}"
            f"_rr{self.range_ratio_min:g}_imb{self.imbalance_min:g}"
            f"_node{self.node_distance_max:g}_ret{self.ret60_min_abs:g}"
            f"_sig{self.local_sigma_ratio_min:g}_sl{self.stop_loss_pips}"
            f"_tp{ladder}_frac{fracs}_trail{self.trail_stop_pips}_ttl{self.ttl_bars}"
        )


def load_datasets(data_dir: Path, instruments: list[str] | None = None) -> dict[str, dict[str, object]]:
    datasets = load_dataset_bundle(data_dir, instruments)
    for instrument, data in datasets.items():
        enriched_path = data_dir / f"{instrument}_5yr_m1.parquet"
        try:
            frame = pd.read_parquet(enriched_path, columns=["timestamp", *REQUIRED_ENRICHED_COLUMNS])
        except Exception:
            frame = pd.read_parquet(enriched_path)
        missing = [column for column in REQUIRED_ENRICHED_COLUMNS if column not in frame.columns]
        if missing:
            raise SystemExit(
                f"{instrument}_5yr_m1.parquet is missing enriched columns: {', '.join(missing)}. "
                "Run the enrichment pipeline first to compute Hawkes, node, and local sigma features."
            )

        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.sort_values("timestamp").reset_index(drop=True)
        mid_close = data["mid"]["close"]
        if len(frame) != len(mid_close):
            raise SystemExit(f"{instrument}: enriched parquet row count does not match execution dataset.")

        data["enriched_df"] = frame
        data["ema_240"] = pd.Series(mid_close).ewm(span=240, adjust=False).mean().to_numpy()
        data["local_sigma_ma_240"] = frame["local_sigma"].rolling(240).mean().to_numpy()
        data["ret60"] = pd.Series(mid_close).diff(60).to_numpy() / float(data["pip"])
    return datasets


def simulate_module(
    spec: ContinuationSpec,
    data: dict[str, object],
    execution_profile: ExecutionProfile | None = None,
) -> pd.DataFrame:
    profile = execution_profile or resolve_execution_profile(None)
    enriched = data["enriched_df"]
    mid = data["mid"]
    ask = data["ask"]
    bid = data["bid"]
    pip = float(data["pip"])
    hour = data["hour"]
    weekday = data["weekday"]

    in_window = (hour >= spec.hour_start) & (hour <= spec.hour_end) & (weekday < 5)
    elevated_vol = data["range_60"] > (data["range_240"] * spec.range_ratio_min)
    near_node = enriched["distance_to_node_pips"].to_numpy(np.float64) < spec.node_distance_max
    sigma_ok = enriched["local_sigma"].to_numpy(np.float64) > (data["local_sigma_ma_240"] * spec.local_sigma_ratio_min)
    imbalance_strong = enriched["imbalance_ratio"].to_numpy(np.float64) > spec.imbalance_min
    ema_240 = data["ema_240"]
    ret60 = data["ret60"]

    if spec.side == "long":
        direction = enriched["ask_lambda"].to_numpy(np.float64) > enriched["bid_lambda"].to_numpy(np.float64)
        trend_ok = mid["close"] > ema_240
        momentum_ok = ret60 > spec.ret60_min_abs
    else:
        direction = enriched["bid_lambda"].to_numpy(np.float64) > enriched["ask_lambda"].to_numpy(np.float64)
        trend_ok = mid["close"] < ema_240
        momentum_ok = ret60 < -spec.ret60_min_abs

    mask = in_window & elevated_vol & near_node & sigma_ok & imbalance_strong & direction & trend_ok & momentum_ok
    entries = np.flatnonzero(mask)
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

        trade = simulate_ladder_exit(
            spec,
            data,
            entry_idx,
            entry_price,
            stop_level,
            execution_profile=profile,
        )
        if trade is None:
            continue
        trade["entry_slippage_pips"] = slippage_pips
        trade["fill_probability"] = profile.fill_probability
        trade["spread_multiplier"] = profile.spread_multiplier
        trade["entry_delay_bars"] = profile.entry_delay_bars
        trade["execution_scenario"] = profile.name
        trades.append(trade)
        next_allowed = trade["exit_idx"] + 1

    if not trades:
        return pd.DataFrame()
    frame = pd.DataFrame(trades)
    frame["partial_fills"] = frame["partial_fills"].apply(json.dumps)
    return frame.drop(columns=["exit_idx"])


def simulate_ladder_exit(
    spec: ContinuationSpec,
    data: dict[str, object],
    entry_idx: int,
    entry_price: float,
    stop_level: float,
    execution_profile: ExecutionProfile | None = None,
) -> dict[str, object] | None:
    profile = execution_profile or resolve_execution_profile(None)
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

    if can_use_native_trade_path(profile):
        exec_open = bid["open"][entry_idx : end_idx + 1] if spec.side == "long" else ask["open"][entry_idx : end_idx + 1]
        exec_high = bid["high"][entry_idx : end_idx + 1] if spec.side == "long" else ask["high"][entry_idx : end_idx + 1]
        exec_low = bid["low"][entry_idx : end_idx + 1] if spec.side == "long" else ask["low"][entry_idx : end_idx + 1]
        exec_close = bid["close"][entry_idx : end_idx + 1] if spec.side == "long" else ask["close"][entry_idx : end_idx + 1]
        native_path = simulate_trade_path_accelerated(
            exec_open,
            exec_high,
            exec_low,
            exec_close,
            entry_price,
            stop_level,
            [float(level) * pip for level in spec.ladder_pips],
            [float(frac) for frac in spec.ladder_fractions],
            float(spec.trail_stop_pips) * pip,
            spec.ttl_bars,
            spec.side,
        )
        exit_idx = entry_idx + int(native_path["exit_idx"])
        exit_price = float(native_path["exit_price"])
        partials = [
            {
                "time": pd.Timestamp(data["timestamp"][entry_idx + int(fill["bar_offset"])]).isoformat(),
                "price": float(fill["price"]),
                "fraction": float(fill["fraction"]),
                "pips": float(fill["pnl_delta"]) / pip,
            }
            for fill in native_path["partials"]
        ]
        return {
            "module": spec.name,
            "instrument": spec.instrument,
            "side": spec.side,
            "entry_time": pd.Timestamp(data["timestamp"][entry_idx]),
            "exit_time": pd.Timestamp(data["timestamp"][exit_idx]),
            "entry_price": entry_price,
            "exit_price": float(exit_price),
            "pnl_pips": float(native_path["total_pnl_delta"]) / pip,
            "stop_pips": float(spec.stop_loss_pips),
            "pip_size": pip,
            "partial_fills": partials,
            "exit_idx": exit_idx,
            "used_inferred_bid_ask": bool(data["inferred_bid_ask"]),
        }

    for bar_idx in range(entry_idx, end_idx + 1):
        long_high = float(bid["high"][bar_idx])
        long_low = float(bid["low"][bar_idx])
        short_high = float(ask["high"][bar_idx])
        short_low = float(ask["low"][bar_idx])
        effective_spread_pips = float(data["effective_spread_pips"][bar_idx])
        exit_slippage_pips = compute_slippage_pips(
            float(data["range_60"][bar_idx]),
            float(data["range_240"][bar_idx]),
            base_slippage=profile.base_slippage_pips,
            vol_sensitivity=profile.vol_factor,
        )

        if spec.side == "long":
            for level_idx, (level, frac) in enumerate(zip(spec.ladder_pips, spec.ladder_fractions)):
                if level_idx in hit_levels or remaining <= 0.0:
                    continue
                target_price = entry_price + level * pip
                if long_high >= target_price:
                    fill_fraction = min(remaining, frac)
                    realized_target_price = adverse_price_adjustment(
                        "long",
                        target_price,
                        pip,
                        effective_spread_pips,
                        exit_slippage_pips,
                        profile.spread_multiplier,
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
                            "fraction": fill_fraction,
                            "pips": realized_level,
                        }
                    )
                    highest_price = max(highest_price, long_high)
                    if not trailing_active:
                        trailing_active = True
                        trailing_stop = realized_target_price - spec.trail_stop_pips * pip

            if trailing_active and remaining > 0.0:
                highest_price = max(highest_price, long_high)
                trailing_stop = max(trailing_stop, highest_price - spec.trail_stop_pips * pip)

            active_stop = trailing_stop if trailing_active else stop_level
            if remaining > 0.0 and long_low <= active_stop:
                realized_stop = adverse_price_adjustment(
                    "long",
                    active_stop,
                    pip,
                    effective_spread_pips,
                    exit_slippage_pips,
                    profile.spread_multiplier,
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
                        "fraction": remaining,
                        "pips": exit_pips,
                    }
                )
                remaining = 0.0
                break
        else:
            for level_idx, (level, frac) in enumerate(zip(spec.ladder_pips, spec.ladder_fractions)):
                if level_idx in hit_levels or remaining <= 0.0:
                    continue
                target_price = entry_price - level * pip
                if short_low <= target_price:
                    fill_fraction = min(remaining, frac)
                    realized_target_price = adverse_price_adjustment(
                        "short",
                        target_price,
                        pip,
                        effective_spread_pips,
                        exit_slippage_pips,
                        profile.spread_multiplier,
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
                            "fraction": fill_fraction,
                            "pips": realized_level,
                        }
                    )
                    lowest_price = min(lowest_price, short_low)
                    if not trailing_active:
                        trailing_active = True
                        trailing_stop = realized_target_price + spec.trail_stop_pips * pip

            if trailing_active and remaining > 0.0:
                lowest_price = min(lowest_price, short_low)
                trailing_stop = min(trailing_stop, lowest_price + spec.trail_stop_pips * pip)

            active_stop = trailing_stop if trailing_active else stop_level
            if remaining > 0.0 and short_high >= active_stop:
                realized_stop = adverse_price_adjustment(
                    "short",
                    active_stop,
                    pip,
                    effective_spread_pips,
                    exit_slippage_pips,
                    profile.spread_multiplier,
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
                        "fraction": remaining,
                        "pips": exit_pips,
                    }
                )
                remaining = 0.0
                break

    if remaining > 0.0:
        ttl_raw_price = float(bid["close"][end_idx] if spec.side == "long" else ask["close"][end_idx])
        ttl_effective_spread_pips = float(data["effective_spread_pips"][end_idx])
        ttl_slippage_pips = compute_slippage_pips(
            float(data["range_60"][end_idx]),
            float(data["range_240"][end_idx]),
            base_slippage=profile.base_slippage_pips,
            vol_sensitivity=profile.vol_factor,
        )
        ttl_price = adverse_price_adjustment(
            spec.side,
            ttl_raw_price,
            pip,
            ttl_effective_spread_pips,
            ttl_slippage_pips,
            profile.spread_multiplier,
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
                "fraction": remaining,
                "pips": exit_pips,
            }
        )

    return {
        "module": spec.name,
        "instrument": spec.instrument,
        "side": spec.side,
        "entry_time": pd.Timestamp(data["timestamp"][entry_idx]),
        "exit_time": pd.Timestamp(data["timestamp"][exit_idx]),
        "entry_price": entry_price,
        "exit_price": float(exit_price),
        "pnl_pips": float(total_pnl_pips),
        "stop_pips": float(spec.stop_loss_pips),
        "pip_size": pip,
        "partial_fills": partials,
        "exit_idx": exit_idx,
        "used_inferred_bid_ask": bool(data["inferred_bid_ask"]),
    }


def simulate_portfolio(
    trades: pd.DataFrame,
    datasets: dict[str, dict[str, object]],
    start_balance: float,
    risk_pct: float,
    max_leverage: float,
    max_concurrent: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if trades.empty:
        trades = pd.DataFrame(
            columns=[
                "module",
                "instrument",
                "side",
                "entry_time",
                "exit_time",
                "entry_price",
                "exit_price",
                "pnl_pips",
                "stop_pips",
                "pip_size",
                "partial_fills",
            ]
        )
    else:
        trades = trades.sort_values("entry_time").reset_index(drop=True)
    usdjpy_close = minute_series(datasets["usdjpy"]["df"])
    gbpusd_close = minute_series(datasets["gbpusd"]["df"])

    realized_balance = start_balance
    active_positions: list[dict[str, object]] = []
    filled: list[dict[str, object]] = []

    def realize_until(cutoff: pd.Timestamp) -> None:
        nonlocal realized_balance, active_positions, filled
        closing = [position for position in active_positions if position["exit_time"] <= cutoff]
        if not closing:
            return
        active_positions = [position for position in active_positions if position["exit_time"] > cutoff]
        for position in sorted(closing, key=lambda row: (row["exit_time"], row["entry_time"], row["module"])):
            realized_balance += position["pnl_dollars"]
            position["balance_after_exit"] = realized_balance
            position["exit_year"] = position["exit_time"].year
            filled.append(position)

    for trade in trades.itertuples(index=False):
        realize_until(trade.entry_time)
        if len(active_positions) >= max_concurrent:
            continue

        minute = trade.entry_time.floor("min")
        pip_value_per_unit, usd_per_unit = dollar_exposure(
            trade.instrument,
            trade.entry_price,
            trade.pip_size,
            minute,
            usdjpy_close,
            gbpusd_close,
        )
        gross_open_notional = sum(float(position["usd_notional"]) for position in active_positions)
        risk_dollars = realized_balance * risk_pct
        units_by_risk = risk_dollars / max(trade.stop_pips * pip_value_per_unit, 1e-12)
        available_notional = max(realized_balance * max_leverage - gross_open_notional, 0.0)
        units_by_leverage = available_notional / usd_per_unit if usd_per_unit > 0 else 0.0
        units = min(units_by_risk, units_by_leverage)
        if units <= 1e-12:
            continue
        pnl_dollars = trade.pnl_pips * pip_value_per_unit * units
        active_positions.append(
            {
                **trade._asdict(),
                "units": units,
                "pnl_dollars": pnl_dollars,
                "balance_before_entry": realized_balance,
                "usd_notional": units * usd_per_unit,
                "entry_year": trade.entry_time.year,
            }
        )

    if active_positions:
        for position in sorted(active_positions, key=lambda row: (row["exit_time"], row["entry_time"], row["module"])):
            realized_balance += position["pnl_dollars"]
            position["balance_after_exit"] = realized_balance
            position["exit_year"] = position["exit_time"].year
            filled.append(position)

    filled_df = pd.DataFrame(filled)
    if not filled_df.empty:
        filled_df = filled_df.sort_values(["exit_time", "entry_time", "module"]).reset_index(drop=True)
        filled_df["balance_before_exit"] = filled_df["balance_after_exit"] - filled_df["pnl_dollars"]
    else:
        filled_df = pd.DataFrame(
            columns=[
                "module",
                "instrument",
                "side",
                "entry_time",
                "exit_time",
                "entry_price",
                "exit_price",
                "pnl_pips",
                "stop_pips",
                "pip_size",
                "partial_fills",
                "units",
                "pnl_dollars",
                "balance_before_entry",
                "usd_notional",
                "entry_year",
                "balance_after_exit",
                "exit_year",
                "balance_before_exit",
            ]
        )

    weekly_df = build_nav_period_table(filled_df, datasets, start_balance, "W-SUN", "week", "weekly_return_pct")
    monthly_df = build_nav_period_table(filled_df, datasets, start_balance, "M", "month", "monthly_return_pct")
    yearly_df = build_nav_period_table(filled_df, datasets, start_balance, "Y", "year", "yearly_return_pct")

    module_table = (
        filled_df.groupby("module")
        .agg(
            instrument=("instrument", "first"),
            side=("side", "first"),
            trades=("pnl_dollars", "size"),
            win_rate_pct=("pnl_dollars", lambda s: (s > 0).mean() * 100.0),
            avg_pnl_pips=("pnl_pips", "mean"),
            total_pnl_dollars=("pnl_dollars", "sum"),
        )
        .reset_index()
        .sort_values("total_pnl_dollars", ascending=False)
        if not filled_df.empty
        else pd.DataFrame(
            columns=["module", "instrument", "side", "trades", "win_rate_pct", "avg_pnl_pips", "total_pnl_dollars"]
        )
    )
    if not module_table.empty:
        total_abs_pnl = float(module_table["total_pnl_dollars"].abs().sum())
        module_table["pnl_share_pct"] = (
            module_table["total_pnl_dollars"].abs() / total_abs_pnl * 100.0 if total_abs_pnl > 0 else 0.0
        )

    weekly_returns = weekly_df["weekly_return_pct"].to_numpy(dtype=float) if not weekly_df.empty else np.array([], dtype=float)
    active_weekly_returns = (
        weekly_df.loc[weekly_df["trades"] > 0, "weekly_return_pct"].to_numpy(dtype=float)
        if not weekly_df.empty
        else np.array([], dtype=float)
    )
    monthly_returns = monthly_df["monthly_return_pct"].to_numpy(dtype=float) if not monthly_df.empty else np.array([], dtype=float)
    max_drawdown_pct = compute_max_drawdown_pct(
        filled_df["balance_after_exit"].to_numpy(dtype=float) if not filled_df.empty else np.array([], dtype=float),
        start_balance,
    )
    trade_pnl = filled_df["pnl_dollars"].to_numpy(dtype=float) if not filled_df.empty else np.array([], dtype=float)
    portfolio_win_rate_pct = float((trade_pnl > 0.0).mean() * 100.0) if len(trade_pnl) else 0.0
    portfolio_profit_factor = profit_factor(trade_pnl) if len(trade_pnl) else 0.0
    geometric_monthly_return_pct = (
        ((realized_balance / start_balance) ** (1.0 / len(monthly_df)) - 1.0) * 100.0
        if len(monthly_df) > 0 and realized_balance > 0.0
        else 0.0
    )
    summary = pd.DataFrame(
        [
            {
                "start_balance": start_balance,
                "ending_balance": realized_balance,
                "roi_pct": ((realized_balance / start_balance) - 1.0) * 100.0,
                "portfolio_trades": len(filled_df),
                "avg_active_weekly_return_pct": active_weekly_returns.mean() if len(active_weekly_returns) else 0.0,
                "median_active_weekly_return_pct": np.median(active_weekly_returns) if len(active_weekly_returns) else 0.0,
                "p90_active_weekly_return_pct": np.quantile(active_weekly_returns, 0.90) if len(active_weekly_returns) else 0.0,
                "calendar_weekly_sharpe_ann": annualize_ratio(sharpe_like(weekly_returns)),
                "calendar_weekly_sortino_ann": annualize_ratio(sortino_like(weekly_returns)),
                "avg_calendar_monthly_return_pct": monthly_returns.mean() if len(monthly_returns) else 0.0,
                "median_calendar_monthly_return_pct": np.median(monthly_returns) if len(monthly_returns) else 0.0,
                "worst_calendar_monthly_return_pct": monthly_returns.min() if len(monthly_returns) else 0.0,
                "positive_calendar_months_pct": float((monthly_returns > 0.0).mean() * 100.0) if len(monthly_returns) else 0.0,
                "geometric_monthly_return_pct": geometric_monthly_return_pct,
                "max_drawdown_pct": max_drawdown_pct,
                "portfolio_win_rate_pct": portfolio_win_rate_pct,
                "portfolio_profit_factor": portfolio_profit_factor,
            }
        ]
    )
    return summary, module_table, weekly_df, monthly_df, yearly_df, filled_df


def minute_series(df: pd.DataFrame) -> pd.Series:
    return (
        df.assign(minute=lambda frame: frame["timestamp"].dt.floor("min"))
        .drop_duplicates("minute")
        .set_index("minute")["close"]
    )


def dollar_exposure(
    instrument: str,
    entry_price: float,
    pip_size: float,
    minute: pd.Timestamp,
    usdjpy_close: pd.Series,
    gbpusd_close: pd.Series,
) -> tuple[float, float]:
    if instrument in ("eurusd", "gbpusd"):
        return pip_size, entry_price
    if instrument == "usdjpy":
        usdjpy = float(usdjpy_close.asof(minute))
        return pip_size / usdjpy, 1.0
    usdjpy = float(usdjpy_close.asof(minute))
    gbpusd = float(gbpusd_close.asof(minute))
    return pip_size / usdjpy, gbpusd


def build_nav_period_table(
    filled_df: pd.DataFrame,
    datasets: dict[str, dict[str, object]],
    start_balance: float,
    freq: str,
    label: str,
    return_col: str,
) -> pd.DataFrame:
    timestamps = []
    for data in datasets.values():
        ts = pd.to_datetime(data["df"]["timestamp"], utc=True)
        timestamps.extend([ts.iloc[0].tz_localize(None), ts.iloc[-1].tz_localize(None)])

    period_range = pd.period_range(min(timestamps).to_period(freq), max(timestamps).to_period(freq), freq=freq)
    if filled_df.empty:
        table = pd.DataFrame({label: period_range.astype(str)})
        if label == "year":
            table[label] = table[label].astype(int)
        table["start_nav"] = start_balance
        table["end_nav"] = start_balance
        table["pnl_dollars"] = 0.0
        table["trades"] = 0
        table[return_col] = 0.0
        return table

    exit_times = pd.to_datetime(filled_df["exit_time"], utc=True).dt.tz_localize(None)
    keyed = filled_df.assign(_period=exit_times.dt.to_period(freq).astype(str))
    grouped = keyed.groupby("_period").agg(pnl_dollars=("pnl_dollars", "sum"), trades=("pnl_dollars", "size"))

    rows: list[dict[str, object]] = []
    current_nav = start_balance
    for period in period_range:
        period_key = str(period)
        start_nav = current_nav
        pnl_dollars = float(grouped.at[period_key, "pnl_dollars"]) if period_key in grouped.index else 0.0
        trades = int(grouped.at[period_key, "trades"]) if period_key in grouped.index else 0
        current_nav = start_nav + pnl_dollars
        period_value: object = period_key
        if label == "year":
            period_value = int(period_key)
        rows.append(
            {
                label: period_value,
                "start_nav": start_nav,
                "end_nav": current_nav,
                "pnl_dollars": pnl_dollars,
                "trades": trades,
                return_col: ((current_nav / start_nav) - 1.0) * 100.0 if start_nav > 0 else 0.0,
            }
        )
    return pd.DataFrame(rows)


def compute_max_drawdown_pct(balance_after_exit: np.ndarray, start_balance: float) -> float:
    equity_curve = np.concatenate(([start_balance], balance_after_exit.astype(float)))
    running_max = np.maximum.accumulate(equity_curve)
    drawdowns = equity_curve / running_max - 1.0
    return float(drawdowns.min() * 100.0)


def profit_factor(pnl: np.ndarray) -> float:
    gross_profit = pnl[pnl > 0].sum()
    gross_loss = -pnl[pnl < 0].sum()
    if gross_loss <= 0:
        return float("inf")
    return float(gross_profit / gross_loss)


def sharpe_like(values: np.ndarray) -> float:
    if len(values) < 2:
        return 0.0
    std = values.std(ddof=0)
    if std <= 1e-12:
        return 0.0
    return float(values.mean() / std)


def sortino_like(values: np.ndarray) -> float:
    if len(values) == 0:
        return 0.0
    downside = values[values < 0.0]
    if len(downside) == 0:
        return float("inf") if values.mean() > 0.0 else 0.0
    downside_dev = float(np.sqrt(np.mean(np.square(downside))))
    if downside_dev <= 1e-12:
        return 0.0
    return float(values.mean() / downside_dev)


def annualize_ratio(value: float, periods: float = 52.0) -> float:
    if not np.isfinite(value):
        return value
    return float(value * np.sqrt(periods))
