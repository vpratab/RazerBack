from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from statistics import NormalDist
import sys
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.pipeline_utilities import DISCOVERY


NORMAL = NormalDist()
EULER_MASCHERONI = 0.5772156649015329
MONTH_NAME_MAP = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


@dataclass(frozen=True)
class WalkForwardWindow:
    window_id: int
    train_start: pd.Timestamp
    train_end: pd.Timestamp
    test_start: pd.Timestamp
    test_end: pd.Timestamp


def _ensure_utc_timestamp(value: pd.Timestamp | str) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _safe_std(values: pd.Series | np.ndarray, ddof: int = 1) -> float:
    arr = np.asarray(values, dtype=float)
    if arr.size <= ddof:
        return float("nan")
    return float(np.std(arr, ddof=ddof))


def _safe_mean(values: pd.Series | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return float("nan")
    return float(np.mean(arr))


def _resolve_timestamp_column(frame: pd.DataFrame, explicit: str | None = None) -> str:
    if explicit and explicit in frame.columns:
        return explicit
    for candidate in ("exit_time", "timestamp", "entry_time", "filled_at", "closed_at"):
        if candidate in frame.columns:
            return candidate
    raise KeyError("Could not resolve a timestamp column in trade history.")


def _resolve_return_column(frame: pd.DataFrame, explicit: str | None = None) -> str | None:
    if explicit and explicit in frame.columns:
        return explicit
    for candidate in ("return_pct", "return", "ret", "pnl_pct", "r_multiple"):
        if candidate in frame.columns:
            return candidate
    return None


def _resolve_pnl_column(frame: pd.DataFrame, explicit: str | None = None) -> str | None:
    if explicit and explicit in frame.columns:
        return explicit
    for candidate in ("net_pnl", "pnl", "pnl_usd", "profit", "profit_loss"):
        if candidate in frame.columns:
            return candidate
    return None


def trade_history_to_daily_returns(
    trades: pd.DataFrame,
    *,
    timestamp_col: str | None = None,
    return_col: str | None = None,
    pnl_col: str | None = None,
    starting_capital: float = 1.0,
) -> pd.Series:
    if trades.empty:
        return pd.Series(dtype=float, name="daily_return")

    ts_col = _resolve_timestamp_column(trades, timestamp_col)
    ts = pd.to_datetime(trades[ts_col], utc=True)
    work = trades.copy()
    work[ts_col] = ts
    work["date"] = work[ts_col].dt.floor("D")

    resolved_return = _resolve_return_column(work, return_col)
    if resolved_return is not None:
        daily = work.groupby("date")[resolved_return].sum().astype(float).sort_index()
        daily.name = "daily_return"
        return daily

    resolved_pnl = _resolve_pnl_column(work, pnl_col)
    if resolved_pnl is None:
        raise KeyError("Trade history must provide either a return column or a pnl column.")

    daily_pnl = work.groupby("date")[resolved_pnl].sum().astype(float).sort_index()
    equity = float(starting_capital)
    returns: list[float] = []
    for pnl_value in daily_pnl.to_numpy():
        if math.isclose(equity, 0.0):
            returns.append(0.0)
        else:
            returns.append(float(pnl_value) / equity)
        equity += float(pnl_value)
    return pd.Series(returns, index=daily_pnl.index, name="daily_return")


def daily_pnl_series(
    trades: pd.DataFrame,
    *,
    timestamp_col: str | None = None,
    pnl_col: str | None = None,
) -> pd.Series:
    if trades.empty:
        return pd.Series(dtype=float, name="daily_pnl")
    ts_col = _resolve_timestamp_column(trades, timestamp_col)
    pnl_name = _resolve_pnl_column(trades, pnl_col)
    if pnl_name is None:
        raise KeyError("Trade history must provide a pnl column for daily pnl conversion.")
    work = trades.copy()
    work[ts_col] = pd.to_datetime(work[ts_col], utc=True)
    work["date"] = work[ts_col].dt.floor("D")
    daily = work.groupby("date")[pnl_name].sum().astype(float).sort_index()
    daily.name = "daily_pnl"
    return daily


def annualized_sharpe_ratio(returns: pd.Series, periods_per_year: int = 252) -> float:
    clean = returns.dropna().astype(float)
    if clean.size < 2:
        return float("nan")
    stdev = _safe_std(clean, ddof=1)
    if not np.isfinite(stdev) or math.isclose(stdev, 0.0):
        return float("nan")
    return _safe_mean(clean) / stdev * math.sqrt(periods_per_year)


def max_drawdown(returns: pd.Series) -> float:
    clean = returns.fillna(0.0).astype(float)
    if clean.empty:
        return 0.0
    wealth = (1.0 + clean).cumprod()
    running_peak = wealth.cummax()
    drawdown = wealth / running_peak - 1.0
    return float(drawdown.min())


def profit_factor(trades: pd.DataFrame, pnl_col: str | None = None) -> float:
    if trades.empty:
        return float("nan")
    resolved = _resolve_pnl_column(trades, pnl_col)
    if resolved is None:
        raise KeyError("Trade history must provide a pnl column for profit factor.")
    pnl = trades[resolved].astype(float)
    gross_profit = float(pnl[pnl > 0.0].sum())
    gross_loss = float(-pnl[pnl < 0.0].sum())
    if math.isclose(gross_loss, 0.0):
        return float("inf") if gross_profit > 0.0 else float("nan")
    return gross_profit / gross_loss


def win_rate(trades: pd.DataFrame, pnl_col: str | None = None) -> float:
    if trades.empty:
        return float("nan")
    resolved = _resolve_pnl_column(trades, pnl_col)
    if resolved is None:
        raise KeyError("Trade history must provide a pnl column for win rate.")
    pnl = trades[resolved].astype(float)
    if pnl.empty:
        return float("nan")
    return float((pnl > 0.0).mean())


def calendar_year_returns(returns: pd.Series) -> pd.Series:
    clean = returns.fillna(0.0).astype(float)
    if clean.empty:
        return pd.Series(dtype=float, name="calendar_year_return")
    yearly = clean.groupby(clean.index.year).apply(lambda values: float((1.0 + values).prod() - 1.0))
    yearly.name = "calendar_year_return"
    return yearly


def monthly_returns(returns: pd.Series) -> pd.Series:
    clean = returns.fillna(0.0).astype(float)
    if clean.empty:
        return pd.Series(dtype=float, name="monthly_return")
    monthly = clean.groupby(pd.Grouper(freq="M")).apply(lambda values: float((1.0 + values).prod() - 1.0))
    monthly.name = "monthly_return"
    return monthly


def monthly_returns_heatmap(returns: pd.Series) -> pd.DataFrame:
    monthly = monthly_returns(returns)
    if monthly.empty:
        return pd.DataFrame()
    heatmap = monthly.to_frame().reset_index()
    heatmap["year"] = heatmap["date"].dt.year
    heatmap["month"] = heatmap["date"].dt.month.map(MONTH_NAME_MAP)
    pivot = heatmap.pivot(index="year", columns="month", values="monthly_return")
    ordered_cols = [MONTH_NAME_MAP[month] for month in range(1, 13)]
    pivot = pivot.reindex(columns=ordered_cols)
    return pivot.sort_index()


def trade_frequency_per_year(trades: pd.DataFrame, timestamp_col: str | None = None) -> float:
    if trades.empty:
        return 0.0
    ts_col = _resolve_timestamp_column(trades, timestamp_col)
    ts = pd.to_datetime(trades[ts_col], utc=True).sort_values()
    span_days = max((ts.iloc[-1] - ts.iloc[0]).days, 1)
    span_years = max(span_days / 365.25, 1.0 / 365.25)
    return float(len(trades) / span_years)


def sample_skewness(values: pd.Series | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < 3:
        return 0.0
    mean_value = float(np.mean(arr))
    std_value = float(np.std(arr, ddof=0))
    if math.isclose(std_value, 0.0):
        return 0.0
    centered = (arr - mean_value) / std_value
    return float(np.mean(centered ** 3))


def sample_kurtosis(values: pd.Series | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < 4:
        return 3.0
    mean_value = float(np.mean(arr))
    std_value = float(np.std(arr, ddof=0))
    if math.isclose(std_value, 0.0):
        return 3.0
    centered = (arr - mean_value) / std_value
    return float(np.mean(centered ** 4))


def lag1_autocorrelation(values: pd.Series | np.ndarray) -> float:
    arr = np.asarray(values, dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < 3:
        return 0.0
    x = arr[:-1]
    y = arr[1:]
    x_centered = x - x.mean()
    y_centered = y - y.mean()
    denom = math.sqrt(float(np.sum(x_centered ** 2) * np.sum(y_centered ** 2)))
    if math.isclose(denom, 0.0):
        return 0.0
    return float(np.sum(x_centered * y_centered) / denom)


def effective_sample_size(sample_length: int | float, autocorrelation: float = 0.0) -> float:
    n = float(sample_length)
    if n <= 1.0:
        return n
    rho = float(np.clip(autocorrelation, -0.99, 0.99))
    adjusted = n * (1.0 - rho) / (1.0 + rho)
    return float(np.clip(adjusted, 2.0, n))


def expected_max_sharpe_ratio(
    num_trials: int,
    sharpe_mean: float = 0.0,
    sharpe_std: float = 1.0,
) -> float:
    if num_trials <= 1 or not np.isfinite(sharpe_std) or sharpe_std <= 0.0:
        return float(sharpe_mean)
    z_first = NORMAL.inv_cdf(1.0 - 1.0 / float(num_trials))
    z_second = NORMAL.inv_cdf(1.0 - 1.0 / (float(num_trials) * math.e))
    return float(sharpe_mean + sharpe_std * ((1.0 - EULER_MASCHERONI) * z_first + EULER_MASCHERONI * z_second))


def probabilistic_sharpe_ratio(
    observed_sharpe: float,
    benchmark_sharpe: float,
    sample_length: int | float,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
    autocorrelation: float = 0.0,
) -> float:
    n_eff = effective_sample_size(sample_length, autocorrelation)
    denominator_term = 1.0 - skewness * observed_sharpe + ((kurtosis - 1.0) / 4.0) * (observed_sharpe ** 2)
    if n_eff <= 1.0 or denominator_term <= 0.0:
        return float("nan")
    z_score = (observed_sharpe - benchmark_sharpe) * math.sqrt(n_eff - 1.0) / math.sqrt(denominator_term)
    return float(NORMAL.cdf(z_score))


def deflated_sharpe_ratio(
    *,
    observed_sharpe: float,
    num_trials: int,
    sample_length: int | float,
    skewness: float,
    kurtosis: float,
    autocorrelation: float,
    trial_sharpes: pd.Series | np.ndarray | None = None,
    sharpe_mean: float = 0.0,
    sharpe_std: float | None = None,
) -> dict[str, float]:
    sr_mean = float(sharpe_mean)
    sr_std = sharpe_std
    if trial_sharpes is not None:
        sr_values = np.asarray(trial_sharpes, dtype=float)
        sr_values = sr_values[np.isfinite(sr_values)]
        if sr_values.size >= 1:
            sr_mean = float(np.mean(sr_values))
        if sr_values.size >= 2:
            sr_std = float(np.std(sr_values, ddof=1))
    n_eff = effective_sample_size(sample_length, autocorrelation)
    if sr_std is None or not np.isfinite(sr_std) or sr_std <= 0.0:
        sr_std = math.sqrt(1.0 / max(n_eff - 1.0, 1.0))
    benchmark = expected_max_sharpe_ratio(num_trials=num_trials, sharpe_mean=sr_mean, sharpe_std=sr_std)
    dsr_value = probabilistic_sharpe_ratio(
        observed_sharpe=observed_sharpe,
        benchmark_sharpe=benchmark,
        sample_length=sample_length,
        skewness=skewness,
        kurtosis=kurtosis,
        autocorrelation=autocorrelation,
    )
    return {
        "dsr": float(dsr_value),
        "benchmark_sharpe": float(benchmark),
        "effective_sample_size": float(n_eff),
        "sharpe_mean": float(sr_mean),
        "sharpe_std": float(sr_std),
    }


def build_walk_forward_windows(
    *,
    discovery_start: pd.Timestamp = DISCOVERY.start,
    train_months: int = 6,
    test_months: int = 3,
    window_count: int = 8,
    step_months: int = 12,
) -> list[WalkForwardWindow]:
    windows: list[WalkForwardWindow] = []
    start = discovery_start.normalize()
    for idx in range(window_count):
        train_start = start + pd.DateOffset(months=idx * step_months)
        train_end = train_start + pd.DateOffset(months=train_months) - pd.Timedelta(seconds=1)
        test_start = train_end + pd.Timedelta(seconds=1)
        test_end = test_start + pd.DateOffset(months=test_months) - pd.Timedelta(seconds=1)
        windows.append(
            WalkForwardWindow(
                window_id=idx + 1,
                train_start=_ensure_utc_timestamp(train_start),
                train_end=_ensure_utc_timestamp(train_end),
                test_start=_ensure_utc_timestamp(test_start),
                test_end=_ensure_utc_timestamp(test_end),
            )
        )
    return windows


def walk_forward_analysis(
    trade_history: pd.DataFrame,
    *,
    timestamp_col: str | None = None,
    return_col: str | None = None,
    pnl_col: str | None = None,
    starting_capital: float = 1.0,
    windows: list[WalkForwardWindow] | None = None,
) -> pd.DataFrame:
    if trade_history.empty:
        result = pd.DataFrame(columns=[
            "window_id",
            "train_start",
            "train_end",
            "test_start",
            "test_end",
            "train_return",
            "test_return",
            "train_sharpe",
            "test_sharpe",
            "test_profitable",
            "train_trades",
            "test_trades",
        ])
        return result

    ts_col = _resolve_timestamp_column(trade_history, timestamp_col)
    work = trade_history.copy()
    work[ts_col] = pd.to_datetime(work[ts_col], utc=True)
    chosen_windows = windows or build_walk_forward_windows()
    rows: list[dict[str, Any]] = []
    for window in chosen_windows:
        train_mask = (work[ts_col] >= window.train_start) & (work[ts_col] <= window.train_end)
        test_mask = (work[ts_col] >= window.test_start) & (work[ts_col] <= window.test_end)
        train_trades = work.loc[train_mask].copy()
        test_trades = work.loc[test_mask].copy()
        train_returns = trade_history_to_daily_returns(
            train_trades,
            timestamp_col=ts_col,
            return_col=return_col,
            pnl_col=pnl_col,
            starting_capital=starting_capital,
        )
        test_returns = trade_history_to_daily_returns(
            test_trades,
            timestamp_col=ts_col,
            return_col=return_col,
            pnl_col=pnl_col,
            starting_capital=starting_capital,
        )
        train_total = float((1.0 + train_returns.fillna(0.0)).prod() - 1.0) if not train_returns.empty else 0.0
        test_total = float((1.0 + test_returns.fillna(0.0)).prod() - 1.0) if not test_returns.empty else 0.0
        rows.append(
            {
                "window_id": window.window_id,
                "train_start": window.train_start,
                "train_end": window.train_end,
                "test_start": window.test_start,
                "test_end": window.test_end,
                "train_return": train_total,
                "test_return": test_total,
                "train_sharpe": annualized_sharpe_ratio(train_returns) if not train_returns.empty else float("nan"),
                "test_sharpe": annualized_sharpe_ratio(test_returns) if not test_returns.empty else float("nan"),
                "test_profitable": bool(test_total > 0.0),
                "train_trades": int(len(train_trades)),
                "test_trades": int(len(test_trades)),
            }
        )
    return pd.DataFrame(rows)


def walk_forward_summary(walk_forward_frame: pd.DataFrame) -> dict[str, Any]:
    if walk_forward_frame.empty:
        return {
            "windows_total": 0,
            "windows_with_trades": 0,
            "positive_windows": 0,
            "robust": False,
        }
    windows_with_trades = int((walk_forward_frame["test_trades"] > 0).sum())
    positive_windows = int(walk_forward_frame["test_profitable"].sum())
    return {
        "windows_total": int(len(walk_forward_frame)),
        "windows_with_trades": windows_with_trades,
        "positive_windows": positive_windows,
        "robust": bool(positive_windows >= 6),
    }


def summarize_module_trade_history(
    trade_history: pd.DataFrame,
    *,
    timestamp_col: str | None = None,
    return_col: str | None = None,
    pnl_col: str | None = None,
    starting_capital: float = 1.0,
    periods_per_year: int = 252,
) -> dict[str, Any]:
    daily_returns = trade_history_to_daily_returns(
        trade_history,
        timestamp_col=timestamp_col,
        return_col=return_col,
        pnl_col=pnl_col,
        starting_capital=starting_capital,
    )
    summary = {
        "trade_count": int(len(trade_history)),
        "sharpe_ann": annualized_sharpe_ratio(daily_returns, periods_per_year=periods_per_year),
        "max_drawdown": max_drawdown(daily_returns),
        "profit_factor": profit_factor(trade_history, pnl_col=pnl_col),
        "win_rate": win_rate(trade_history, pnl_col=pnl_col),
        "trade_frequency_per_year": trade_frequency_per_year(trade_history, timestamp_col=timestamp_col),
        "calendar_year_returns": calendar_year_returns(daily_returns),
        "monthly_returns": monthly_returns(daily_returns),
        "monthly_returns_heatmap": monthly_returns_heatmap(daily_returns),
        "daily_returns": daily_returns,
        "skewness": sample_skewness(daily_returns),
        "kurtosis": sample_kurtosis(daily_returns),
        "autocorrelation": lag1_autocorrelation(daily_returns),
    }
    return summary


def daily_pnl_correlation_matrix(
    module_trade_histories: dict[str, pd.DataFrame],
    *,
    timestamp_col: str | None = None,
    pnl_col: str | None = None,
) -> pd.DataFrame:
    aligned: dict[str, pd.Series] = {}
    for module_name, trades in module_trade_histories.items():
        aligned[module_name] = daily_pnl_series(trades, timestamp_col=timestamp_col, pnl_col=pnl_col)
    if not aligned:
        return pd.DataFrame()
    frame = pd.concat(aligned, axis=1).fillna(0.0).sort_index()
    return frame.corr()
