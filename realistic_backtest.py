from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd


PIP_SIZES = {
    "eurusd": 0.0001,
    "gbpusd": 0.0001,
    "usdjpy": 0.01,
    "gbpjpy": 0.01,
}

_BASE_HOURLY_SPREADS = {
    "eurusd": [
        1.5, 1.5, 1.4, 1.4, 1.3, 1.2, 1.2, 1.1, 1.0, 1.0, 1.0, 1.0,
        1.1, 1.1, 1.0, 1.0, 1.1, 1.2, 1.3, 1.5, 1.7, 2.0, 2.2, 2.4,
    ],
    "gbpusd": [
        1.9, 1.9, 1.8, 1.8, 1.7, 1.6, 1.5, 1.3, 1.2, 1.2, 1.2, 1.2,
        1.3, 1.3, 1.2, 1.2, 1.3, 1.4, 1.6, 1.9, 2.1, 2.4, 2.7, 3.0,
    ],
    "usdjpy": [
        1.6, 1.6, 1.5, 1.4, 1.3, 1.2, 1.2, 1.1, 1.1, 1.1, 1.1, 1.1,
        1.2, 1.2, 1.1, 1.1, 1.2, 1.3, 1.5, 1.7, 1.9, 2.1, 2.2, 2.3,
    ],
    "gbpjpy": [
        4.6, 4.5, 4.4, 4.2, 4.0, 3.7, 3.4, 3.0, 2.8, 2.8, 2.9, 2.9,
        3.0, 3.0, 2.9, 2.9, 3.1, 3.3, 3.6, 4.0, 4.5, 5.0, 5.5, 6.0,
    ],
}

_WEEKDAY_SPREAD_MULTIPLIERS = {
    0: 1.05,
    1: 1.00,
    2: 1.00,
    3: 1.02,
    4: 1.12,
    5: 1.35,
    6: 1.35,
}


def spread_table_pips() -> dict[str, dict[int, dict[int, float]]]:
    table: dict[str, dict[int, dict[int, float]]] = {}
    for instrument, hourly in _BASE_HOURLY_SPREADS.items():
        table[instrument] = {}
        for weekday, multiplier in _WEEKDAY_SPREAD_MULTIPLIERS.items():
            table[instrument][weekday] = {
                hour: round(hourly[hour] * multiplier, 4) for hour in range(24)
            }
    return table


SPREAD_TABLE_PIPS = spread_table_pips()


def lookup_spread_pips(instrument: str, hour: np.ndarray, weekday: np.ndarray) -> np.ndarray:
    table = SPREAD_TABLE_PIPS[instrument]
    return np.array([table[int(day)][int(hr)] for hr, day in zip(hour, weekday)], dtype=np.float64)


def compute_slippage_pips(
    range_60: float,
    range_240: float,
    base_slippage: float = 0.2,
    vol_sensitivity: float = 0.3,
    max_slippage: float = 2.0,
) -> float:
    if not np.isfinite(range_60):
        range_60 = 0.0
    if not np.isfinite(range_240) or range_240 <= 1e-12:
        range_240 = max(range_60, 1.0)
    scaled = base_slippage + (range_60 / range_240) * vol_sensitivity
    return float(min(max_slippage, max(base_slippage, scaled)))


def load_dataset_bundle(
    data_dir: Path,
    instruments: list[str] | None = None,
) -> dict[str, dict[str, object]]:
    out: dict[str, dict[str, object]] = {}
    selected = instruments or list(PIP_SIZES.keys())
    for instrument in selected:
        out[instrument] = load_instrument_dataset(data_dir, instrument, PIP_SIZES[instrument])
    return out


def load_instrument_dataset(data_dir: Path, instrument: str, pip: float) -> dict[str, object]:
    bid_ask_path = None
    for candidate in (
        data_dir / f"{instrument}_5yr_m1_bid_ask.parquet",
        data_dir / f"{instrument}_5yr_m1_ba.parquet",
        data_dir / f"{instrument}_5yr_bid_ask_m1.parquet",
    ):
        if candidate.exists():
            bid_ask_path = candidate
            break

    if bid_ask_path is not None:
        df = pd.read_parquet(
            bid_ask_path,
            columns=[
                "timestamp",
                "open_bid",
                "high_bid",
                "low_bid",
                "close_bid",
                "open_ask",
                "high_ask",
                "low_ask",
                "close_ask",
            ],
        )
        inferred = False
    else:
        midpoint_path = data_dir / f"{instrument}_5yr_m1.parquet"
        df = pd.read_parquet(midpoint_path, columns=["timestamp", "open", "high", "low", "close"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df = df.sort_values("timestamp").reset_index(drop=True)
        hour = df["timestamp"].dt.hour.to_numpy(np.int16)
        weekday = df["timestamp"].dt.weekday.to_numpy(np.int16)
        spread_pips = lookup_spread_pips(instrument, hour, weekday)
        half_spread = (spread_pips * pip) / 2.0
        for label in ("open", "high", "low", "close"):
            df[f"{label}_bid"] = df[label] - half_spread
            df[f"{label}_ask"] = df[label] + half_spread
        inferred = True
        warnings.warn(
            f"{instrument}: bid/ask parquet missing; inferring bid/ask from midpoint candles and spread table.",
            stacklevel=2,
        )

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp").reset_index(drop=True)

    bid = {
        "open": df["open_bid"].to_numpy(np.float64),
        "high": df["high_bid"].to_numpy(np.float64),
        "low": df["low_bid"].to_numpy(np.float64),
        "close": df["close_bid"].to_numpy(np.float64),
    }
    ask = {
        "open": df["open_ask"].to_numpy(np.float64),
        "high": df["high_ask"].to_numpy(np.float64),
        "low": df["low_ask"].to_numpy(np.float64),
        "close": df["close_ask"].to_numpy(np.float64),
    }

    timestamp = pd.to_datetime(df["timestamp"], utc=True)
    hour = timestamp.dt.hour.to_numpy(np.int16)
    weekday = timestamp.dt.weekday.to_numpy(np.int16)
    spread_open_pips = (ask["open"] - bid["open"]) / pip
    table_spread_pips = lookup_spread_pips(instrument, hour, weekday)
    effective_spread_pips = np.maximum(spread_open_pips, table_spread_pips)
    midpoint = {
        "open": (bid["open"] + ask["open"]) / 2.0,
        "high": (bid["high"] + ask["high"]) / 2.0,
        "low": (bid["low"] + ask["low"]) / 2.0,
        "close": (bid["close"] + ask["close"]) / 2.0,
    }

    series_close = pd.Series(midpoint["close"])
    series_high = pd.Series(midpoint["high"])
    series_low = pd.Series(midpoint["low"])
    ret15 = (midpoint["close"] - series_close.shift(15).to_numpy()) / pip
    ret60 = (midpoint["close"] - series_close.shift(60).to_numpy()) / pip
    range_pips = (midpoint["high"] - midpoint["low"]) / pip
    z60 = (
        (pd.Series(ret60) - pd.Series(ret60).rolling(240).mean())
        / pd.Series(ret60).rolling(240).std(ddof=0)
    ).to_numpy()

    frame = pd.DataFrame({"timestamp": timestamp, "close": midpoint["close"]})
    return {
        "timestamp": timestamp.to_numpy(),
        "df": frame,
        "bid": bid,
        "ask": ask,
        "mid": midpoint,
        "pip": pip,
        "hour": hour,
        "weekday": weekday,
        "spread_open_pips": spread_open_pips,
        "effective_spread_pips": effective_spread_pips,
        "ret15": ret15,
        "ret60": ret60,
        "z60": z60,
        "prior_high_60": series_high.shift(1).rolling(60).max().to_numpy(),
        "prior_low_60": series_low.shift(1).rolling(60).min().to_numpy(),
        "range_60": pd.Series(range_pips).rolling(60).mean().to_numpy(),
        "range_240": pd.Series(range_pips).rolling(240).mean().to_numpy(),
        "trend_240": series_close.rolling(240).mean().to_numpy(),
        "upper_wick": ((midpoint["high"] - np.maximum(midpoint["open"], midpoint["close"])) / pip).clip(min=0.0),
        "lower_wick": ((np.minimum(midpoint["open"], midpoint["close"]) - midpoint["low"]) / pip).clip(min=0.0),
        "inferred_bid_ask": inferred,
    }


def deterministic_segments(mid_open: float, mid_high: float, mid_low: float, mid_close: float) -> tuple[str, str, str]:
    if mid_close >= mid_open:
        return ("open", "low", "high", "close")
    return ("open", "high", "low", "close")


def interpolate_cross_fraction(start_value: float, end_value: float, threshold: float) -> float | None:
    if np.isclose(start_value, end_value):
        if np.isclose(start_value, threshold):
            return 0.0
        return None
    lo = min(start_value, end_value)
    hi = max(start_value, end_value)
    if threshold < lo or threshold > hi:
        return None
    return float((threshold - start_value) / (end_value - start_value))


def resolve_trade_path(
    side: str,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    shield_trigger: float,
    shield_lock: float,
    data: dict[str, object],
    entry_idx: int,
    ttl_bars: int,
    use_monte_carlo: bool = False,
    mc_paths: int = 10,
) -> tuple[int, float, bool]:
    pip = float(data["pip"])
    bid = data["bid"]
    ask = data["ask"]
    mid = data["mid"]

    exit_idx = min(len(mid["open"]) - 1, entry_idx + ttl_bars)
    shielded = False

    if use_monte_carlo:
        outcomes = []
        for _ in range(mc_paths):
            mc_exit_idx, mc_exit_price, mc_shielded = resolve_trade_path(
                side,
                entry_price,
                stop_loss,
                take_profit,
                shield_trigger,
                shield_lock,
                data,
                entry_idx,
                ttl_bars,
                use_monte_carlo=False,
                mc_paths=mc_paths,
            )
            outcomes.append((mc_exit_idx, mc_exit_price, mc_shielded))
        avg_exit_price = float(np.mean([item[1] for item in outcomes]))
        avg_exit_idx = int(round(np.mean([item[0] for item in outcomes])))
        return avg_exit_idx, avg_exit_price, any(item[2] for item in outcomes)

    for bar_idx in range(entry_idx, exit_idx + 1):
        order = deterministic_segments(
            mid["open"][bar_idx],
            mid["high"][bar_idx],
            mid["low"][bar_idx],
            mid["close"][bar_idx],
        )

        for start_key, end_key in zip(order, order[1:]):
            seg_bid_start = bid[start_key][bar_idx]
            seg_bid_end = bid[end_key][bar_idx]
            seg_ask_start = ask[start_key][bar_idx]
            seg_ask_end = ask[end_key][bar_idx]

            while True:
                events: list[tuple[float, str]] = []
                if side == "long":
                    if not shielded:
                        frac = interpolate_cross_fraction(seg_ask_start, seg_ask_end, shield_trigger)
                        if frac is not None:
                            events.append((frac, "shield"))
                    frac = interpolate_cross_fraction(seg_bid_start, seg_bid_end, stop_loss)
                    if frac is not None:
                        events.append((frac, "stop"))
                    frac = interpolate_cross_fraction(seg_bid_start, seg_bid_end, take_profit)
                    if frac is not None:
                        events.append((frac, "target"))
                else:
                    if not shielded:
                        frac = interpolate_cross_fraction(seg_bid_start, seg_bid_end, shield_trigger)
                        if frac is not None:
                            events.append((frac, "shield"))
                    frac = interpolate_cross_fraction(seg_ask_start, seg_ask_end, stop_loss)
                    if frac is not None:
                        events.append((frac, "stop"))
                    frac = interpolate_cross_fraction(seg_ask_start, seg_ask_end, take_profit)
                    if frac is not None:
                        events.append((frac, "target"))

                if not events:
                    break

                frac, event = sorted(events, key=lambda item: (item[0], 0 if item[1] == "shield" else 1))[0]
                event_bid = seg_bid_start + (seg_bid_end - seg_bid_start) * frac
                event_ask = seg_ask_start + (seg_ask_end - seg_ask_start) * frac

                if event == "shield":
                    shielded = True
                    stop_loss = shield_lock
                    seg_bid_start = event_bid
                    seg_ask_start = event_ask
                    continue

                if side == "long":
                    if event == "stop":
                        return bar_idx, float(stop_loss), shielded
                    return bar_idx, float(take_profit), shielded

                if event == "stop":
                    return bar_idx, float(stop_loss), shielded
                return bar_idx, float(take_profit), shielded

    if side == "long":
        return exit_idx, float(bid["close"][exit_idx]), shielded
    return exit_idx, float(ask["close"][exit_idx]), shielded


def simulate_reversal_module(
    spec: object,
    data: dict[str, object],
    module_name: str,
    use_monte_carlo: bool = False,
    mc_paths: int = 10,
) -> pd.DataFrame:
    mid = data["mid"]
    hour = data["hour"]
    weekday = data["weekday"]
    pip = float(data["pip"])

    if getattr(spec, "side") == "long":
        base = (
            (weekday < 5)
            & (hour == getattr(spec, "hour_utc"))
            & np.isfinite(data["prior_low_60"])
            & (mid["low"] < data["prior_low_60"])
            & (mid["close"] > data["prior_low_60"])
            & (mid["close"] >= data["trend_240"])
            & (data["range_60"] >= data["range_240"] * getattr(spec, "range_ratio_min"))
        )
        wick = data["lower_wick"]
        dist = (data["prior_low_60"] - mid["low"]) / pip
    else:
        base = (
            (weekday < 5)
            & (hour == getattr(spec, "hour_utc"))
            & np.isfinite(data["prior_high_60"])
            & (mid["high"] > data["prior_high_60"])
            & (mid["close"] < data["prior_high_60"])
            & (mid["close"] <= data["trend_240"])
            & (data["range_60"] >= data["range_240"] * getattr(spec, "range_ratio_min"))
        )
        wick = data["upper_wick"]
        dist = (mid["high"] - data["prior_high_60"]) / pip

    mask = base & (wick >= getattr(spec, "wick_min")) & (dist >= getattr(spec, "dist_min"))
    if getattr(spec, "ret15_min") is not None:
        mask &= data["ret15"] >= getattr(spec, "ret15_min")
    if getattr(spec, "ret15_max") is not None:
        mask &= data["ret15"] <= getattr(spec, "ret15_max")
    if getattr(spec, "z_min") is not None:
        mask &= np.abs(data["z60"]) >= getattr(spec, "z_min")

    entries = np.flatnonzero(mask)
    trades: list[dict[str, object]] = []
    next_allowed = 0

    for idx in entries:
        entry_idx = idx + 1
        if entry_idx >= len(mid["open"]) - 1 or entry_idx < next_allowed:
            continue

        slippage_pips = compute_slippage_pips(
            float(data["range_60"][entry_idx]),
            float(data["range_240"][entry_idx]),
        )
        slippage = slippage_pips * pip

        if getattr(spec, "side") == "long":
            entry_price = float(data["ask"]["open"][entry_idx] + slippage)
            stop_loss = entry_price - (getattr(spec, "stop_pips") * pip)
            take_profit = entry_price + (getattr(spec, "target_pips") * pip)
            shield_trigger = entry_price + (getattr(spec, "shield_trigger_pips") * pip)
            shield_lock = entry_price + (getattr(spec, "shield_lock_pips") * pip)
        else:
            entry_price = float(data["bid"]["open"][entry_idx] - slippage)
            stop_loss = entry_price + (getattr(spec, "stop_pips") * pip)
            take_profit = entry_price - (getattr(spec, "target_pips") * pip)
            shield_trigger = entry_price - (getattr(spec, "shield_trigger_pips") * pip)
            shield_lock = entry_price - (getattr(spec, "shield_lock_pips") * pip)

        exit_idx, exit_price, shielded = resolve_trade_path(
            getattr(spec, "side"),
            entry_price,
            stop_loss,
            take_profit,
            shield_trigger,
            shield_lock,
            data,
            entry_idx,
            getattr(spec, "ttl_bars"),
            use_monte_carlo=use_monte_carlo,
            mc_paths=mc_paths,
        )
        pnl_pips = (exit_price - entry_price) / pip if getattr(spec, "side") == "long" else (entry_price - exit_price) / pip
        trades.append(
            {
                "module": module_name,
                "instrument": getattr(spec, "instrument"),
                "side": getattr(spec, "side"),
                "entry_time": pd.Timestamp(data["timestamp"][entry_idx]),
                "exit_time": pd.Timestamp(data["timestamp"][exit_idx]),
                "entry_price": entry_price,
                "exit_price": float(exit_price),
                "pnl_pips": float(pnl_pips),
                "stop_pips": float(getattr(spec, "stop_pips")),
                "pip_size": pip,
                "entry_slippage_pips": slippage_pips,
                "shielded": shielded,
                "used_inferred_bid_ask": bool(data["inferred_bid_ask"]),
            }
        )
        next_allowed = exit_idx + 1

    return pd.DataFrame(trades)
