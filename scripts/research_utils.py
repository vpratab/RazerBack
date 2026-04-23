from __future__ import annotations

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

from continuation_core import simulate_portfolio
from scripts.run_multifamily_fx_research import BreakoutSpec, ReversalSpec


BREAKOUT_PATTERN = re.compile(
    r"^(?P<instrument>[a-z]+)_(?P<side>long|short)_bo_h(?P<hour>\d+)"
    r"_rr(?P<rr>-?\d+(?:\.\d+)?)_dist(?P<dist>-?\d+(?:\.\d+)?)"
    r"_wickmax(?P<wick>na|-?\d+(?:\.\d+)?)"
    r"_ret(?P<ret>(?:na|ge-?\d+(?:\.\d+)?|le-?\d+(?:\.\d+)?))"
    r"_z(?P<z>na|-?\d+(?:\.\d+)?)"
    r"_sl(?P<sl>\d+)_tp(?P<tp>\d+)"
    r"_shield(?P<trigger>\d+)-(?P<lock>\d+)"
    r"_ttl(?P<ttl>\d+)$"
)

REVERSAL_PATTERN = re.compile(
    r"^(?P<instrument>[a-z]+)_(?P<side>long|short)_rev_h(?P<hour>\d+)"
    r"_rr(?P<rr>-?\d+(?:\.\d+)?)_wick(?P<wick>-?\d+(?:\.\d+)?)"
    r"_dist(?P<dist>-?\d+(?:\.\d+)?)"
    r"_ret(?P<ret>(?:na|ge-?\d+(?:\.\d+)?|le-?\d+(?:\.\d+)?))"
    r"_z(?P<z>na|-?\d+(?:\.\d+)?)"
    r"_sl(?P<sl>\d+)_tp(?P<tp>\d+)"
    r"_shield(?P<trigger>\d+)-(?P<lock>\d+)"
    r"_ttl(?P<ttl>\d+)$"
)


def parse_optional_num(token: str) -> float | None:
    return None if token == "na" else float(token)


def parse_ret(token: str) -> tuple[float | None, float | None]:
    if token == "na":
        return None, None
    if token.startswith("ge"):
        return float(token[2:]), None
    if token.startswith("le"):
        return None, float(token[2:])
    raise ValueError(f"Unknown ret token: {token}")


def spec_from_name(name: str) -> BreakoutSpec | ReversalSpec:
    breakout_match = BREAKOUT_PATTERN.match(name)
    if breakout_match:
        ret15_min, ret15_max = parse_ret(breakout_match.group("ret"))
        return BreakoutSpec(
            instrument=breakout_match.group("instrument"),
            side=breakout_match.group("side"),
            hour_utc=int(breakout_match.group("hour")),
            range_ratio_min=float(breakout_match.group("rr")),
            close_dist_min=float(breakout_match.group("dist")),
            breakout_wick_max=parse_optional_num(breakout_match.group("wick")),
            ret15_min=ret15_min,
            ret15_max=ret15_max,
            z_min=parse_optional_num(breakout_match.group("z")),
            stop_pips=int(breakout_match.group("sl")),
            target_pips=int(breakout_match.group("tp")),
            shield_trigger_pips=int(breakout_match.group("trigger")),
            shield_lock_pips=int(breakout_match.group("lock")),
            ttl_bars=int(breakout_match.group("ttl")),
        )

    reversal_match = REVERSAL_PATTERN.match(name)
    if reversal_match:
        ret15_min, ret15_max = parse_ret(reversal_match.group("ret"))
        return ReversalSpec(
            instrument=reversal_match.group("instrument"),
            side=reversal_match.group("side"),
            hour_utc=int(reversal_match.group("hour")),
            range_ratio_min=float(reversal_match.group("rr")),
            wick_min=float(reversal_match.group("wick")),
            dist_min=float(reversal_match.group("dist")),
            ret15_min=ret15_min,
            ret15_max=ret15_max,
            z_min=parse_optional_num(reversal_match.group("z")),
            stop_pips=int(reversal_match.group("sl")),
            target_pips=int(reversal_match.group("tp")),
            shield_trigger_pips=int(reversal_match.group("trigger")),
            shield_lock_pips=int(reversal_match.group("lock")),
            ttl_bars=int(reversal_match.group("ttl")),
        )

    raise ValueError(f"Could not parse module name: {name}")


def filter_dataset(data: dict[str, object], year: int) -> dict[str, object]:
    start_ts = pd.Timestamp(f"{year}-01-01", tz="UTC")
    end_ts = pd.Timestamp(f"{year}-12-31 23:59:59", tz="UTC")
    ts = pd.to_datetime(data["timestamp"], utc=True)
    mask = (ts >= start_ts) & (ts <= end_ts)
    idx = np.flatnonzero(mask.to_numpy() if hasattr(mask, "to_numpy") else np.asarray(mask))
    out: dict[str, object] = {}
    for key, value in data.items():
        if key in ("pip", "inferred_bid_ask"):
            out[key] = value
        elif key == "df":
            out[key] = value.loc[mask].reset_index(drop=True).copy()
        elif key in ("bid", "ask", "mid"):
            out[key] = {inner_key: np.asarray(inner_value)[idx] for inner_key, inner_value in value.items()}
        else:
            arr = np.asarray(value)
            out[key] = value if arr.shape == () else arr[idx]
    return out


def trade_key_frame(frame: pd.DataFrame) -> pd.Series:
    return frame["module"].astype(str) + "|" + pd.to_datetime(frame["entry_time"], utc=True).astype(str)


def apply_overlay(
    raw_trades: pd.DataFrame,
    datasets: dict[str, dict[str, object]],
    annual_stop_pct: float | None,
    monthly_stop_pct: float | None,
    start_balance: float = 1500.0,
    risk_pct: float = 0.029,
    max_leverage: float = 35.0,
    max_concurrent: int = 2,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if annual_stop_pct is None and monthly_stop_pct is None:
        return simulate_portfolio(raw_trades, datasets, start_balance, risk_pct, max_leverage, max_concurrent)

    active = raw_trades.copy().sort_values(["entry_time", "module"]).reset_index(drop=True)
    skipped_keys: set[str] = set()

    for _ in range(5):
        summary, module_table, weekly, monthly, yearly, filled = simulate_portfolio(
            active,
            datasets,
            start_balance,
            risk_pct,
            max_leverage,
            max_concurrent,
        )
        if filled.empty:
            return summary, module_table, weekly, monthly, yearly, filled

        filled = filled.sort_values(["entry_time", "module"]).reset_index(drop=True)
        filled["entry_ts"] = pd.to_datetime(filled["entry_time"], utc=True)
        filled["exit_ts"] = pd.to_datetime(filled["exit_time"], utc=True)
        active["entry_ts"] = pd.to_datetime(active["entry_time"], utc=True)
        active_keys = trade_key_frame(active)

        new_skips: set[str] = set()
        annual_peak = start_balance
        monthly_peak = start_balance
        current_month = None
        for row in filled.to_dict(orient="records"):
            entry_ts = pd.Timestamp(row["entry_ts"])
            exit_ts = pd.Timestamp(row["exit_ts"])
            month = entry_ts.to_period("M")
            if month != current_month:
                current_month = month
                monthly_peak = float(row["balance_before_entry"])
            annual_peak = max(annual_peak, float(row["balance_before_entry"]), float(row["balance_after_exit"]))
            monthly_peak = max(monthly_peak, float(row["balance_before_entry"]), float(row["balance_after_exit"]))
            annual_dd = (float(row["balance_after_exit"]) / annual_peak - 1.0) * 100.0
            monthly_dd = (float(row["balance_after_exit"]) / monthly_peak - 1.0) * 100.0

            if annual_stop_pct is not None and annual_dd <= annual_stop_pct:
                mask = active["entry_ts"] > exit_ts
                new_skips.update(active_keys.loc[mask].tolist())
                break
            if monthly_stop_pct is not None and monthly_dd <= monthly_stop_pct:
                mask = (active["entry_ts"] > exit_ts) & (active["entry_ts"].dt.to_period("M") == month)
                new_skips.update(active_keys.loc[mask].tolist())

        if not new_skips - skipped_keys:
            return summary, module_table, weekly, monthly, yearly, filled.drop(columns=["entry_ts", "exit_ts"], errors="ignore")

        skipped_keys.update(new_skips)
        keep = ~trade_key_frame(raw_trades).isin(skipped_keys)
        active = raw_trades.loc[keep].copy().sort_values(["entry_time", "module"]).reset_index(drop=True)

    return simulate_portfolio(active, datasets, start_balance, risk_pct, max_leverage, max_concurrent)


def load_modules_from_rankings(variant_rankings: Path, label: str) -> list[str]:
    rankings = pd.read_csv(variant_rankings)
    rows = rankings.loc[rankings["label"] == label]
    if rows.empty:
        raise SystemExit(f"Could not find label {label!r} in {variant_rankings}")
    modules = json.loads(rows.iloc[0]["modules_json"])
    if not isinstance(modules, list):
        raise SystemExit(f"Expected modules_json for {label!r} to decode to a list.")
    return [str(module) for module in modules]
