from __future__ import annotations

import json
import os
from typing import Any

import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture


if os.getenv("FXBACKTEST_DISABLE_RUST", "").strip() in {"1", "true", "TRUE", "yes", "YES"}:
    _fxbacktest_core = None
    FXBACKTEST_CORE_IMPORT_ERROR = "disabled by FXBACKTEST_DISABLE_RUST"
else:
    try:
        import fxbacktest_core as _fxbacktest_core
    except ImportError as exc:  # pragma: no cover - environment dependent
        _fxbacktest_core = None
        FXBACKTEST_CORE_IMPORT_ERROR = str(exc)
    else:  # pragma: no cover - environment dependent
        FXBACKTEST_CORE_IMPORT_ERROR = ""


FXBACKTEST_CORE_AVAILABLE = _fxbacktest_core is not None


def hawkes_intensity_reference(shocks: np.ndarray, alpha: float) -> np.ndarray:
    array = np.asarray(shocks, dtype=np.float64)
    out = np.zeros(len(array), dtype=np.float64)
    current = 0.0
    decay = float(np.exp(-alpha))
    for idx, shock in enumerate(array):
        current = current * decay + float(shock)
        out[idx] = current
    return out


def hawkes_intensity_accelerated(shocks: np.ndarray, alpha: float) -> np.ndarray:
    array = np.asarray(shocks, dtype=np.float64)
    if FXBACKTEST_CORE_AVAILABLE:  # pragma: no branch - tiny wrapper
        return np.asarray(_fxbacktest_core.hawkes_intensity(array.tolist(), float(alpha)), dtype=np.float64)
    return hawkes_intensity_reference(array, alpha)


def rolling_gmm_nodes_reference(
    timestamp: pd.Series,
    mid_close: np.ndarray,
    components: int,
    lookback_hours: int,
    refit_hours: int,
) -> np.ndarray:
    frame = pd.DataFrame({"timestamp": pd.to_datetime(timestamp, utc=True), "mid_close": np.asarray(mid_close, dtype=np.float64)})
    hourly = (
        frame.set_index("timestamp")
        .resample("1h")
        .agg(mid_close=("mid_close", "last"))
        .dropna()
        .reset_index()
    )

    node_by_hour = np.full(len(hourly), np.nan, dtype=np.float64)
    values = hourly["mid_close"].to_numpy(dtype=np.float64)
    refit_step = max(1, int(refit_hours))

    for idx in range(len(hourly)):
        if idx < lookback_hours:
            continue
        if idx % refit_step != 0 and np.isfinite(node_by_hour[idx - 1]):
            node_by_hour[idx] = node_by_hour[idx - 1]
            continue

        sample = values[idx - lookback_hours : idx].reshape(-1, 1)
        model = GaussianMixture(n_components=int(components), covariance_type="full", random_state=42)
        model.fit(sample)
        means = np.sort(model.means_.flatten())
        current_price = values[idx]
        nearest_idx = int(np.argmin(np.abs(means - current_price)))
        node_by_hour[idx] = means[nearest_idx]

    node_hourly = pd.DataFrame({"timestamp": hourly["timestamp"], "node_mean": node_by_hour})
    node_hourly["node_mean"] = node_hourly["node_mean"].ffill().bfill()
    merged = (
        frame.assign(hour=lambda df: df["timestamp"].dt.floor("1h"))
        .merge(
            node_hourly.rename(columns={"timestamp": "hour"}),
            on="hour",
            how="left",
        )
        .drop(columns=["hour"])
    )
    merged["node_mean"] = merged["node_mean"].ffill().bfill()
    return merged["node_mean"].to_numpy(dtype=np.float64)


def rolling_gmm_nodes_accelerated(
    timestamp: pd.Series,
    mid_close: np.ndarray,
    components: int,
    lookback_hours: int,
    refit_hours: int,
) -> np.ndarray:
    ts = pd.to_datetime(timestamp, utc=True)
    prices = np.asarray(mid_close, dtype=np.float64)
    if FXBACKTEST_CORE_AVAILABLE:  # pragma: no branch - tiny wrapper
        ts_ms = np.array([int(pd.Timestamp(value).value // 1_000_000) for value in ts], dtype=np.int64)
        result = _fxbacktest_core.rolling_gmm_nodes(
            prices.tolist(),
            ts_ms.tolist(),
            int(lookback_hours),
            int(components),
            int(refit_hours),
        )
        return np.asarray(result, dtype=np.float64)
    return rolling_gmm_nodes_reference(ts, prices, components, lookback_hours, refit_hours)


def simulate_trade_path_reference(
    open_quote: np.ndarray,
    high_quote: np.ndarray,
    low_quote: np.ndarray,
    close_quote: np.ndarray,
    entry_price: float,
    stop_loss: float,
    ladder_deltas: list[float],
    ladder_fractions: list[float],
    trail_delta: float,
    ttl_bars: int,
    side: str,
) -> dict[str, Any]:
    del open_quote  # Only kept for parity with the Rust signature.
    high = np.asarray(high_quote, dtype=np.float64)
    low = np.asarray(low_quote, dtype=np.float64)
    close = np.asarray(close_quote, dtype=np.float64)
    end_idx = min(len(close) - 1, int(ttl_bars))
    remaining = 1.0
    total_pnl_delta = 0.0
    trailing_active = False
    trailing_stop = float(stop_loss)
    highest_price = float(entry_price)
    lowest_price = float(entry_price)
    hit_levels: set[int] = set()
    partials: list[dict[str, Any]] = []
    exit_idx = end_idx
    exit_price = float(close[end_idx])

    for bar_idx in range(end_idx + 1):
        if side == "long":
            long_high = float(high[bar_idx])
            long_low = float(low[bar_idx])
            for level_idx, (delta, frac) in enumerate(zip(ladder_deltas, ladder_fractions)):
                if level_idx in hit_levels or remaining <= 0.0:
                    continue
                target_price = float(entry_price + delta)
                if long_high >= target_price:
                    fill_fraction = min(remaining, float(frac))
                    pnl_delta = float(target_price - entry_price)
                    total_pnl_delta += pnl_delta * fill_fraction
                    remaining -= fill_fraction
                    hit_levels.add(level_idx)
                    partials.append(
                        {
                            "bar_offset": bar_idx,
                            "price": target_price,
                            "fraction": fill_fraction,
                            "pnl_delta": pnl_delta,
                            "event": "target",
                        }
                    )
                    highest_price = max(highest_price, long_high)
                    if not trailing_active:
                        trailing_active = True
                        trailing_stop = target_price - float(trail_delta)

            if trailing_active and remaining > 0.0:
                highest_price = max(highest_price, long_high)
                trailing_stop = max(trailing_stop, highest_price - float(trail_delta))

            active_stop = trailing_stop if trailing_active else stop_loss
            if remaining > 0.0 and long_low <= active_stop:
                pnl_delta = float(active_stop - entry_price)
                total_pnl_delta += pnl_delta * remaining
                exit_idx = bar_idx
                exit_price = float(active_stop)
                partials.append(
                    {
                        "bar_offset": bar_idx,
                        "price": float(active_stop),
                        "fraction": remaining,
                        "pnl_delta": pnl_delta,
                        "event": "stop",
                    }
                )
                remaining = 0.0
                break
        else:
            short_high = float(high[bar_idx])
            short_low = float(low[bar_idx])
            for level_idx, (delta, frac) in enumerate(zip(ladder_deltas, ladder_fractions)):
                if level_idx in hit_levels or remaining <= 0.0:
                    continue
                target_price = float(entry_price - delta)
                if short_low <= target_price:
                    fill_fraction = min(remaining, float(frac))
                    pnl_delta = float(entry_price - target_price)
                    total_pnl_delta += pnl_delta * fill_fraction
                    remaining -= fill_fraction
                    hit_levels.add(level_idx)
                    partials.append(
                        {
                            "bar_offset": bar_idx,
                            "price": target_price,
                            "fraction": fill_fraction,
                            "pnl_delta": pnl_delta,
                            "event": "target",
                        }
                    )
                    lowest_price = min(lowest_price, short_low)
                    if not trailing_active:
                        trailing_active = True
                        trailing_stop = target_price + float(trail_delta)

            if trailing_active and remaining > 0.0:
                lowest_price = min(lowest_price, short_low)
                trailing_stop = min(trailing_stop, lowest_price + float(trail_delta))

            active_stop = trailing_stop if trailing_active else stop_loss
            if remaining > 0.0 and short_high >= active_stop:
                pnl_delta = float(entry_price - active_stop)
                total_pnl_delta += pnl_delta * remaining
                exit_idx = bar_idx
                exit_price = float(active_stop)
                partials.append(
                    {
                        "bar_offset": bar_idx,
                        "price": float(active_stop),
                        "fraction": remaining,
                        "pnl_delta": pnl_delta,
                        "event": "stop",
                    }
                )
                remaining = 0.0
                break

    if remaining > 0.0:
        ttl_price = float(close[end_idx])
        pnl_delta = float(ttl_price - entry_price) if side == "long" else float(entry_price - ttl_price)
        total_pnl_delta += pnl_delta * remaining
        exit_idx = end_idx
        exit_price = ttl_price
        partials.append(
            {
                "bar_offset": end_idx,
                "price": ttl_price,
                "fraction": remaining,
                "pnl_delta": pnl_delta,
                "event": "ttl",
            }
        )

    return {
        "total_pnl_delta": float(total_pnl_delta),
        "exit_idx": int(exit_idx),
        "exit_price": float(exit_price),
        "partials": partials,
    }


def simulate_trade_path_accelerated(
    open_quote: np.ndarray,
    high_quote: np.ndarray,
    low_quote: np.ndarray,
    close_quote: np.ndarray,
    entry_price: float,
    stop_loss: float,
    ladder_deltas: list[float],
    ladder_fractions: list[float],
    trail_delta: float,
    ttl_bars: int,
    side: str,
) -> dict[str, Any]:
    open_arr = np.asarray(open_quote, dtype=np.float64)
    high_arr = np.asarray(high_quote, dtype=np.float64)
    low_arr = np.asarray(low_quote, dtype=np.float64)
    close_arr = np.asarray(close_quote, dtype=np.float64)
    if FXBACKTEST_CORE_AVAILABLE:  # pragma: no branch - tiny wrapper
        total_pnl_delta, exit_idx, partial_json = _fxbacktest_core.simulate_trade_path_rust(
            open_arr.tolist(),
            high_arr.tolist(),
            low_arr.tolist(),
            close_arr.tolist(),
            float(entry_price),
            float(stop_loss),
            [float(value) for value in ladder_deltas],
            [float(value) for value in ladder_fractions],
            float(trail_delta),
            int(ttl_bars),
            side,
        )
        partials = json.loads(partial_json)
        exit_price = float(partials[-1]["price"]) if partials else float(close_arr[min(len(close_arr) - 1, int(exit_idx))])
        return {
            "total_pnl_delta": float(total_pnl_delta),
            "exit_idx": int(exit_idx),
            "exit_price": exit_price,
            "partials": partials,
        }
    return simulate_trade_path_reference(
        open_arr,
        high_arr,
        low_arr,
        close_arr,
        entry_price,
        stop_loss,
        ladder_deltas,
        ladder_fractions,
        trail_delta,
        ttl_bars,
        side,
    )
