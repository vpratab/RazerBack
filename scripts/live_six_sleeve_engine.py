from __future__ import annotations

import argparse
import atexit
import json
import math
import msvcrt
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRATCH_ROOT = Path(r"C:\Users\saanvi\Documents\Codex\2026-04-20-what-i-need-from-the-new")
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SCRATCH_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRATCH_ROOT))

from gbpjpy_session_mesh_search_20260425 import SessionSpec, build_specs, signal_for_spec
from live_reporting import ensure_live_paths, export_trade_ledger, upsert_trade_record
from logging_utils import configure_rotating_logger, log_json
from oanda_client import OandaClient, OandaCredentials, OandaInstrument
from realistic_backtest import PIP_SIZES
from scripts.research_utils import spec_from_name
from scripts.run_multifamily_fx_research import BreakoutSpec, ReversalSpec


START_BALANCE = 1500.0
RISK_PCT = 0.029
MAX_LEVERAGE = 35.0
MAX_CONCURRENT = 2
HISTORY_BARS = 60 * 24 * 120
STREAM_SLEEP_SECONDS = 5
DEFAULT_LIVE_ROOT = Path(r"C:\fx_data\live_six_sleeve_clean_oos")
DEFAULT_MANIFEST = SCRATCH_ROOT / "clean_oos_validation_final_20260426" / "clean_lock_module_manifest.csv"
DEFAULT_DATA_DIR = Path(r"C:\fx_data\m1")


class SingleInstanceLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = open(self.path, "a+b")
        try:
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            self.handle.close()
            self.handle = None
            raise SystemExit(f"Live engine already running (lock held at {self.path}).") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(str(os.getpid()).encode("ascii"))
        self.handle.flush()
        atexit.register(self.release)

    def release(self) -> None:
        if self.handle is None:
            return
        try:
            self.handle.seek(0)
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass
        try:
            self.handle.close()
        finally:
            self.handle = None


def repo_to_oanda(instrument: str) -> str:
    return f"{instrument[:3].upper()}_{instrument[3:].upper()}"


def oanda_to_repo(instrument: str) -> str:
    return instrument.replace("_", "").lower()


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def append_csv_row(path: Path, row: dict[str, Any]) -> None:
    frame = pd.DataFrame([row])
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, mode="a", header=not path.exists(), index=False)


@dataclass(frozen=True)
class LockedModule:
    sleeve: str
    instrument: str
    module: str
    weight: float
    kind: str
    spec: BreakoutSpec | ReversalSpec | SessionSpec

    @property
    def side(self) -> str:
        value = getattr(self.spec, "side", None)
        if value:
            return str(value)
        return "long" if str(getattr(self.spec, "mode")).endswith("long") else "short"

    @property
    def stop_pips(self) -> float:
        return float(getattr(self.spec, "stop_pips", getattr(self.spec, "stop", 0.0)))

    @property
    def target_pips(self) -> float:
        return float(getattr(self.spec, "target_pips", getattr(self.spec, "target", 0.0)))

    @property
    def ttl_minutes(self) -> int:
        return int(getattr(self.spec, "ttl_bars", getattr(self.spec, "hold", 0)))

    @property
    def shield_trigger_pips(self) -> float:
        return float(getattr(self.spec, "shield_trigger_pips", 0.0))

    @property
    def shield_lock_pips(self) -> float:
        return float(getattr(self.spec, "shield_lock_pips", 0.0))


@dataclass
class MinuteAccumulator:
    minute: pd.Timestamp
    open_bid: float
    high_bid: float
    low_bid: float
    close_bid: float
    open_ask: float
    high_ask: float
    low_ask: float
    close_ask: float

    @classmethod
    def from_quote(cls, minute: pd.Timestamp, bid: float, ask: float) -> "MinuteAccumulator":
        return cls(
            minute=minute,
            open_bid=bid,
            high_bid=bid,
            low_bid=bid,
            close_bid=bid,
            open_ask=ask,
            high_ask=ask,
            low_ask=ask,
            close_ask=ask,
        )

    def update(self, bid: float, ask: float) -> None:
        self.high_bid = max(self.high_bid, bid)
        self.low_bid = min(self.low_bid, bid)
        self.close_bid = bid
        self.high_ask = max(self.high_ask, ask)
        self.low_ask = min(self.low_ask, ask)
        self.close_ask = ask

    def as_row(self) -> dict[str, Any]:
        return {
            "timestamp": self.minute,
            "open_bid": self.open_bid,
            "high_bid": self.high_bid,
            "low_bid": self.low_bid,
            "close_bid": self.close_bid,
            "open_ask": self.open_ask,
            "high_ask": self.high_ask,
            "low_ask": self.low_ask,
            "close_ask": self.close_ask,
        }


class InstrumentState:
    def __init__(self, instrument: str, frame: pd.DataFrame) -> None:
        self.instrument = instrument
        self.pip = float(PIP_SIZES[instrument])
        self.frame = frame.sort_values("timestamp").tail(HISTORY_BARS).reset_index(drop=True).copy()
        self._bootstrap_features()

    def _bootstrap_features(self) -> None:
        frame = self.frame
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame["mid_open"] = (frame["open_bid"] + frame["open_ask"]) / 2.0
        frame["mid_high"] = (frame["high_bid"] + frame["high_ask"]) / 2.0
        frame["mid_low"] = (frame["low_bid"] + frame["low_ask"]) / 2.0
        frame["mid_close"] = (frame["close_bid"] + frame["close_ask"]) / 2.0

        series_close = frame["mid_close"]
        series_high = frame["mid_high"]
        series_low = frame["mid_low"]
        range_pips = (frame["mid_high"] - frame["mid_low"]) / self.pip
        ret60 = series_close.diff(60) / self.pip

        frame["ret15"] = series_close.diff(15) / self.pip
        frame["ret30"] = series_close.diff(30) / self.pip
        frame["ret60"] = ret60
        frame["ret120"] = series_close.diff(120) / self.pip
        frame["z60"] = (ret60 - ret60.rolling(240).mean()) / ret60.rolling(240).std(ddof=0)
        frame["prior_high_60"] = series_high.shift(1).rolling(60).max()
        frame["prior_low_60"] = series_low.shift(1).rolling(60).min()
        frame["range_pips"] = range_pips
        frame["range_60"] = pd.Series(range_pips).rolling(60).mean()
        frame["range_240"] = pd.Series(range_pips).rolling(240).mean()
        frame["trend_240"] = series_close.rolling(240).mean()
        frame["upper_wick"] = ((frame["mid_high"] - np.maximum(frame["mid_open"], frame["mid_close"])) / self.pip).clip(lower=0.0)
        frame["lower_wick"] = ((np.minimum(frame["mid_open"], frame["mid_close"]) - frame["mid_low"]) / self.pip).clip(lower=0.0)
        frame["dist_trend"] = (frame["mid_close"] - frame["trend_240"]) / self.pip
        frame["vol_rank"] = frame["range_240"].rank(pct=True, method="average")
        frame["hour"] = frame["timestamp"].dt.hour
        frame["minute"] = frame["timestamp"].dt.minute
        frame["weekday"] = frame["timestamp"].dt.weekday
        self.frame = frame

    def append_bar(self, candle: dict[str, Any]) -> None:
        row = pd.DataFrame([candle])
        row["timestamp"] = pd.to_datetime(row["timestamp"], utc=True)
        frame = pd.concat([self.frame, row], ignore_index=True).tail(HISTORY_BARS).reset_index(drop=True)
        idx = frame.index[-1]
        frame.loc[idx, "mid_open"] = (float(frame.loc[idx, "open_bid"]) + float(frame.loc[idx, "open_ask"])) / 2.0
        frame.loc[idx, "mid_high"] = (float(frame.loc[idx, "high_bid"]) + float(frame.loc[idx, "high_ask"])) / 2.0
        frame.loc[idx, "mid_low"] = (float(frame.loc[idx, "low_bid"]) + float(frame.loc[idx, "low_ask"])) / 2.0
        frame.loc[idx, "mid_close"] = (float(frame.loc[idx, "close_bid"]) + float(frame.loc[idx, "close_ask"])) / 2.0

        mid_open = float(frame.loc[idx, "mid_open"])
        mid_high = float(frame.loc[idx, "mid_high"])
        mid_low = float(frame.loc[idx, "mid_low"])
        mid_close = float(frame.loc[idx, "mid_close"])
        frame.loc[idx, "range_pips"] = (mid_high - mid_low) / self.pip
        frame.loc[idx, "ret15"] = self._diff_pips(frame["mid_close"], 15)
        frame.loc[idx, "ret30"] = self._diff_pips(frame["mid_close"], 30)
        frame.loc[idx, "ret60"] = self._diff_pips(frame["mid_close"], 60)
        frame.loc[idx, "ret120"] = self._diff_pips(frame["mid_close"], 120)

        ret_window = frame["ret60"].iloc[max(0, len(frame) - 240):].dropna()
        ret60_value = float(frame.loc[idx, "ret60"]) if pd.notna(frame.loc[idx, "ret60"]) else math.nan
        if len(ret_window) >= 2 and math.isfinite(ret60_value):
            ret_std = float(ret_window.std(ddof=0))
            frame.loc[idx, "z60"] = 0.0 if ret_std <= 1e-12 else (ret60_value - float(ret_window.mean())) / ret_std
        else:
            frame.loc[idx, "z60"] = math.nan

        frame.loc[idx, "prior_high_60"] = self._prior_extreme(frame["mid_high"], 60, np.max)
        frame.loc[idx, "prior_low_60"] = self._prior_extreme(frame["mid_low"], 60, np.min)
        frame.loc[idx, "range_60"] = self._tail_mean(frame["range_pips"], 60)
        frame.loc[idx, "range_240"] = self._tail_mean(frame["range_pips"], 240)
        frame.loc[idx, "trend_240"] = self._tail_mean(frame["mid_close"], 240)
        frame.loc[idx, "upper_wick"] = max(0.0, (mid_high - max(mid_open, mid_close)) / self.pip)
        frame.loc[idx, "lower_wick"] = max(0.0, (min(mid_open, mid_close) - mid_low) / self.pip)
        trend_value = float(frame.loc[idx, "trend_240"]) if pd.notna(frame.loc[idx, "trend_240"]) else math.nan
        frame.loc[idx, "dist_trend"] = (mid_close - trend_value) / self.pip if math.isfinite(trend_value) else math.nan
        frame.loc[idx, "vol_rank"] = self._vol_rank(frame["range_240"])
        timestamp = pd.Timestamp(frame.loc[idx, "timestamp"])
        frame.loc[idx, "hour"] = int(timestamp.hour)
        frame.loc[idx, "minute"] = int(timestamp.minute)
        frame.loc[idx, "weekday"] = int(timestamp.weekday())
        self.frame = frame

    @staticmethod
    def _tail_mean(series: pd.Series, lookback: int) -> float:
        window = series.iloc[max(0, len(series) - lookback):]
        if window.empty:
            return math.nan
        return float(window.mean())

    @staticmethod
    def _prior_extreme(series: pd.Series, lookback: int, reducer) -> float:
        if len(series) <= 1:
            return math.nan
        window = series.iloc[max(0, len(series) - lookback - 1):-1]
        if window.empty:
            return math.nan
        return float(reducer(window.to_numpy(dtype=float)))

    def _diff_pips(self, series: pd.Series, bars: int) -> float:
        if len(series) <= bars:
            return math.nan
        return float((float(series.iloc[-1]) - float(series.iloc[-1 - bars])) / self.pip)

    @staticmethod
    def _vol_rank(series: pd.Series) -> float:
        current = float(series.iloc[-1]) if pd.notna(series.iloc[-1]) else math.nan
        if not math.isfinite(current):
            return math.nan
        hist = series.iloc[:-1].dropna().to_numpy(dtype=float)
        if hist.size == 0:
            return 1.0
        return float((np.sum(hist <= current) + 1.0) / (hist.size + 1.0))

    def latest(self) -> pd.Series:
        return self.frame.iloc[-1]


def load_history(data_dir: Path, instrument: str) -> pd.DataFrame:
    path = data_dir / f"{instrument}_5yr_m1_bid_ask.parquet"
    frame = pd.read_parquet(path).tail(HISTORY_BARS).reset_index(drop=True)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return frame


def pip_value_per_unit(instrument: str, instrument_states: dict[str, InstrumentState], entry_price: float) -> float:
    quote = instrument[3:]
    pip = float(PIP_SIZES[instrument])
    if quote == "usd":
        return pip
    if quote == "jpy":
        usdjpy = float(instrument_states["usdjpy"].latest()["mid_close"])
        return pip / max(usdjpy, 1e-9)
    if quote == "gbp":
        gbpusd = float(instrument_states["gbpusd"].latest()["mid_close"])
        return pip * gbpusd
    return pip / max(entry_price, 1e-9)


def usd_notional_per_unit(instrument: str, entry_price: float, instrument_states: dict[str, InstrumentState]) -> float:
    base = instrument[:3]
    quote = instrument[3:]
    if base == "usd":
        return 1.0
    if quote == "usd":
        return entry_price
    if quote == "jpy":
        usdjpy = float(instrument_states["usdjpy"].latest()["mid_close"])
        return entry_price / max(usdjpy, 1e-9)
    if quote == "gbp":
        gbpusd = float(instrument_states["gbpusd"].latest()["mid_close"])
        return entry_price * gbpusd
    return entry_price


def calculate_units(
    *,
    module: LockedModule,
    entry_price: float,
    strategy_balance: float,
    open_notional: float,
    instrument_states: dict[str, InstrumentState],
) -> float:
    pip_value = pip_value_per_unit(module.instrument, instrument_states, entry_price)
    usd_per_unit = usd_notional_per_unit(module.instrument, entry_price, instrument_states)
    risk_dollars = strategy_balance * RISK_PCT * float(module.weight)
    units_by_risk = risk_dollars / max(module.stop_pips * pip_value, 1e-12)
    available_notional = max(strategy_balance * MAX_LEVERAGE - open_notional, 0.0)
    units_by_leverage = available_notional / max(usd_per_unit, 1e-12)
    return float(max(0.0, min(units_by_risk, units_by_leverage)))


def load_locked_modules(manifest_path: Path) -> list[LockedModule]:
    manifest = pd.read_csv(manifest_path)
    session_specs = {f"gbpjpy_{spec.code}": spec for spec in build_specs()}
    modules: list[LockedModule] = []
    for row in manifest.to_dict(orient="records"):
        pair = str(row["pair"]).upper()
        module_name = str(row["module"])
        weight = float(row["weight"])
        instrument = pair.lower()
        if pair == "GBPJPY":
            spec = session_specs[module_name]
            kind = "session"
        else:
            spec = spec_from_name(module_name)
            kind = "breakout" if isinstance(spec, BreakoutSpec) else "reversal"
            instrument = str(spec.instrument)
        modules.append(
            LockedModule(
                sleeve=pair,
                instrument=instrument,
                module=module_name,
                weight=weight,
                kind=kind,
                spec=spec,
            )
        )
    return modules


def evaluate_breakout(spec: BreakoutSpec, row: pd.Series, pip: float) -> bool:
    if int(row["weekday"]) >= 5 or int(row["hour"]) != spec.hour_utc:
        return False
    if not math.isfinite(float(row["range_60"])) or not math.isfinite(float(row["range_240"])) or float(row["range_240"]) <= 0.0:
        return False
    if float(row["range_60"]) < float(row["range_240"]) * spec.range_ratio_min:
        return False
    if spec.side == "long":
        prior = float(row["prior_high_60"])
        if not math.isfinite(prior):
            return False
        if not (float(row["mid_high"]) > prior and float(row["mid_close"]) > prior and float(row["mid_close"]) >= float(row["trend_240"])):
            return False
        dist = (float(row["mid_close"]) - prior) / pip
        wick_against = float(row["upper_wick"])
    else:
        prior = float(row["prior_low_60"])
        if not math.isfinite(prior):
            return False
        if not (float(row["mid_low"]) < prior and float(row["mid_close"]) < prior and float(row["mid_close"]) <= float(row["trend_240"])):
            return False
        dist = (prior - float(row["mid_close"])) / pip
        wick_against = float(row["lower_wick"])
    if dist < spec.close_dist_min:
        return False
    if spec.breakout_wick_max is not None and wick_against > spec.breakout_wick_max:
        return False
    ret15 = float(row["ret15"]) if pd.notna(row["ret15"]) else math.nan
    if spec.ret15_min is not None and (not math.isfinite(ret15) or ret15 < spec.ret15_min):
        return False
    if spec.ret15_max is not None and (not math.isfinite(ret15) or ret15 > spec.ret15_max):
        return False
    if spec.z_min is not None:
        z60 = float(row["z60"]) if pd.notna(row["z60"]) else math.nan
        if not math.isfinite(z60) or abs(z60) < spec.z_min:
            return False
    return True


def evaluate_reversal(spec: ReversalSpec, row: pd.Series, pip: float) -> bool:
    if int(row["weekday"]) >= 5 or int(row["hour"]) != spec.hour_utc:
        return False
    if not math.isfinite(float(row["range_60"])) or not math.isfinite(float(row["range_240"])) or float(row["range_240"]) <= 0.0:
        return False
    if float(row["range_60"]) < float(row["range_240"]) * spec.range_ratio_min:
        return False
    if spec.side == "long":
        prior = float(row["prior_low_60"])
        if not math.isfinite(prior):
            return False
        if not (float(row["mid_low"]) < prior and float(row["mid_close"]) > prior and float(row["mid_close"]) >= float(row["trend_240"])):
            return False
        wick = float(row["lower_wick"])
        dist = (prior - float(row["mid_low"])) / pip
    else:
        prior = float(row["prior_high_60"])
        if not math.isfinite(prior):
            return False
        if not (float(row["mid_high"]) > prior and float(row["mid_close"]) < prior and float(row["mid_close"]) <= float(row["trend_240"])):
            return False
        wick = float(row["upper_wick"])
        dist = (float(row["mid_high"]) - prior) / pip
    if wick < spec.wick_min or dist < spec.dist_min:
        return False
    ret15 = float(row["ret15"]) if pd.notna(row["ret15"]) else math.nan
    if spec.ret15_min is not None and (not math.isfinite(ret15) or ret15 < spec.ret15_min):
        return False
    if spec.ret15_max is not None and (not math.isfinite(ret15) or ret15 > spec.ret15_max):
        return False
    if spec.z_min is not None:
        z60 = float(row["z60"]) if pd.notna(row["z60"]) else math.nan
        if not math.isfinite(z60) or abs(z60) < spec.z_min:
            return False
    return True


def evaluate_session(spec: SessionSpec, row: pd.Series) -> bool:
    if int(row["weekday"]) >= 5:
        return False
    if int(row["hour"]) != spec.hour or int(row["minute"]) != 0:
        return False
    if spec.weekdays is not None and int(row["weekday"]) not in spec.weekdays:
        return False
    vol_rank = float(row["vol_rank"]) if pd.notna(row["vol_rank"]) else math.nan
    if spec.vol_low is not None and (not math.isfinite(vol_rank) or vol_rank < spec.vol_low):
        return False
    if spec.vol_high is not None and (not math.isfinite(vol_rank) or vol_rank > spec.vol_high):
        return False
    dist_trend = float(row["dist_trend"]) if pd.notna(row["dist_trend"]) else math.nan
    if spec.dist_low is not None and (not math.isfinite(dist_trend) or dist_trend < spec.dist_low):
        return False
    if spec.dist_high is not None and (not math.isfinite(dist_trend) or dist_trend > spec.dist_high):
        return False
    ret_col = f"ret{spec.lookback}"
    ret_value = float(row[ret_col]) if ret_col in row and pd.notna(row[ret_col]) else math.nan
    return bool(signal_for_spec(ret_value, spec.mode, spec.threshold))


def evaluate_module(module: LockedModule, state: InstrumentState) -> bool:
    row = state.latest()
    pip = state.pip
    if module.kind == "breakout":
        return evaluate_breakout(module.spec, row, pip)  # type: ignore[arg-type]
    if module.kind == "reversal":
        return evaluate_reversal(module.spec, row, pip)  # type: ignore[arg-type]
    return evaluate_session(module.spec, row)  # type: ignore[arg-type]


def write_live_test_manifest(
    *,
    live_paths,
    account_summary: dict[str, Any],
    modules: list[LockedModule],
    manifest_path: Path,
    dry_run: bool,
    effective_start_balance: float,
) -> None:
    payload = {
        "engine": "six_sleeve_clean_oos_live_test",
        "lock_status": "frozen 2019-12-31",
        "manifest_path": str(manifest_path),
        "dry_run": dry_run,
        "account_id": account_summary.get("id"),
        "account_alias": account_summary.get("alias"),
        "environment": "practice",
        "strategy_start_balance": effective_start_balance,
        "research_reference_start_balance": START_BALANCE,
        "risk_pct_per_trade": RISK_PCT * 100.0,
        "max_leverage": MAX_LEVERAGE,
        "max_concurrent": MAX_CONCURRENT,
        "sleeves": [
            {
                "sleeve": module.sleeve,
                "instrument": module.instrument,
                "module": module.module,
                "weight": module.weight,
                "kind": module.kind,
            }
            for module in modules
        ],
    }
    save_json(live_paths.root / "live_test_manifest.json", payload)
    readme = [
        "# Six-Sleeve Live Test",
        "",
        f"- Lock status: `frozen 2019-12-31`",
        f"- Account: `{account_summary.get('alias', '')}` / `{account_summary.get('id', '')}`",
        f"- Environment: `practice`",
        f"- Strategy start balance: `${effective_start_balance:,.2f}`",
        f"- Research reference start balance: `${START_BALANCE:,.2f}`",
        f"- Risk per trade: `{RISK_PCT * 100:.2f}% * module weight`",
        f"- Max leverage: `{MAX_LEVERAGE}x`",
        f"- Concurrency cap: `{MAX_CONCURRENT}`",
        "",
        "## Log files",
        "",
        f"- [live_trading.log]({live_paths.root / 'live_trading.log'})",
        f"- [engine_heartbeat.json]({live_paths.heartbeat_path})",
        f"- [engine_state.json]({live_paths.state_path})",
        f"- [positions_snapshot.json]({live_paths.positions_snapshot_path})",
        "",
        "## Exports",
        "",
        f"- [trade_ledger.csv]({live_paths.exports / 'trade_ledger.csv'})",
        f"- [closed_trades_enriched.csv]({live_paths.exports / 'closed_trades_enriched.csv'})",
        f"- [signal_journal.csv]({live_paths.exports / 'signal_journal.csv'})",
        f"- [daily_equity.csv]({live_paths.exports / 'daily_equity.csv'})",
        f"- [sleeve_activity.csv]({live_paths.exports / 'sleeve_activity.csv'})",
        f"- [system_summary.json]({live_paths.exports / 'system_summary.json'})",
        "",
        "This is the isolated practice-live test for the clean OOS six-sleeve lock. No parameter changes are permitted once launched.",
    ]
    (live_paths.root / "README.md").write_text("\n".join(str(line) for line in readme), encoding="utf-8")


def infer_exit_reason(trade_state: dict[str, Any], exit_price: float) -> str:
    if trade_state.get("manual_exit_reason"):
        return str(trade_state["manual_exit_reason"])
    target = float(trade_state.get("take_profit_price", math.nan))
    stop = float(trade_state.get("active_stop_loss_price", trade_state.get("stop_loss_price", math.nan)))
    side = str(trade_state["side"])
    tolerance = float(PIP_SIZES[str(trade_state["instrument"])]) * 0.15
    if math.isfinite(target) and abs(exit_price - target) <= tolerance:
        return "target"
    if math.isfinite(stop) and abs(exit_price - stop) <= tolerance:
        return "shield_stop" if bool(trade_state.get("shielded")) else "stop"
    return "broker_close"


def refresh_live_exports(live_paths, strategy_start_balance: float) -> None:
    enriched_csv = live_paths.exports / "closed_trades_enriched.csv"
    if not enriched_csv.exists():
        return
    trades = pd.read_csv(enriched_csv)
    if trades.empty:
        return
    trades["entry_time"] = pd.to_datetime(trades["entry_time"], utc=True, errors="coerce")
    trades["exit_time"] = pd.to_datetime(trades["exit_time"], utc=True, errors="coerce")
    trades["pnl_dollars"] = trades["pnl_dollars"].astype(float)
    trades["pnl_pips"] = trades["pnl_pips"].astype(float)
    trades["strategy_balance_after"] = trades["strategy_balance_after"].astype(float)
    trades["hold_minutes"] = trades["hold_minutes"].astype(float)
    trades["win"] = trades["pnl_dollars"] > 0.0

    daily = (
        trades.sort_values("exit_time")
        .assign(date=lambda frame: frame["exit_time"].dt.date.astype(str))
        .groupby("date", as_index=False)
        .agg(
            ending_balance=("strategy_balance_after", "last"),
            pnl_dollars=("pnl_dollars", "sum"),
            trades=("trade_id", "size"),
        )
    )
    daily["cum_return_pct"] = (daily["ending_balance"] / strategy_start_balance - 1.0) * 100.0
    daily.to_csv(live_paths.exports / "daily_equity.csv", index=False)
    daily.to_parquet(live_paths.exports / "daily_equity.parquet", index=False)

    sleeve = (
        trades.groupby("sleeve", as_index=False)
        .agg(
            trades=("trade_id", "size"),
            wins=("win", "sum"),
            pnl_dollars=("pnl_dollars", "sum"),
            avg_hold_minutes=("hold_minutes", "mean"),
            avg_pnl_pips=("pnl_pips", "mean"),
        )
        .sort_values("trades", ascending=False)
    )
    sleeve["win_rate_pct"] = sleeve["wins"] / sleeve["trades"] * 100.0
    sleeve["roi_contribution_pct"] = sleeve["pnl_dollars"] / strategy_start_balance * 100.0
    sleeve.to_csv(live_paths.exports / "sleeve_activity.csv", index=False)
    sleeve.to_parquet(live_paths.exports / "sleeve_activity.parquet", index=False)

    gross_profit = float(trades.loc[trades["pnl_dollars"] > 0.0, "pnl_dollars"].sum())
    gross_loss = float(-trades.loc[trades["pnl_dollars"] < 0.0, "pnl_dollars"].sum())
    win_rate_pct = float(trades["win"].mean() * 100.0)
    ending_balance = float(trades["strategy_balance_after"].iloc[-1])
    roi_pct = (ending_balance / strategy_start_balance - 1.0) * 100.0
    equity = np.concatenate(([strategy_start_balance], trades["strategy_balance_after"].to_numpy(dtype=float)))
    peaks = np.maximum.accumulate(equity)
    max_drawdown_pct = float((equity / np.maximum(peaks, 1e-12) - 1.0).min() * 100.0)
    summary = {
        "trade_count": int(len(trades)),
        "win_rate_pct": win_rate_pct,
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0.0 else float("inf"),
        "total_pnl_dollars": float(trades["pnl_dollars"].sum()),
        "total_pnl_pips": float(trades["pnl_pips"].sum()),
        "ending_balance": ending_balance,
        "start_balance": strategy_start_balance,
        "roi_pct": roi_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "avg_hold_minutes": float(trades["hold_minutes"].mean()),
    }
    save_json(live_paths.exports / "system_summary.json", summary)


def sync_completed_transactions(
    *,
    client: OandaClient,
    state: dict[str, Any],
    logger,
    live_paths,
) -> None:
    last_id = state.get("last_transaction_id")
    if not last_id:
        return
    payload = client.transactions_since(str(last_id))
    state["last_transaction_id"] = str(payload.get("lastTransactionID", last_id))
    for transaction in payload.get("transactions", []):
        if transaction.get("type") != "ORDER_FILL":
            continue
        if "tradesClosed" not in transaction:
            continue
        broker_account_balance = float(transaction.get("accountBalance", 0.0))
        for closed in transaction.get("tradesClosed", []):
            trade_id = str(closed["tradeID"])
            trade_state = state.get("managed_trades", {}).get(trade_id)
            if trade_state is None:
                continue
            price = float(closed.get("price", transaction.get("price", trade_state["entry_price"])))
            units_closed = abs(float(closed.get("units", trade_state["remaining_units"])))
            pnl_dollars = float(closed.get("realizedPL", 0.0)) + float(closed.get("financing", 0.0))
            fraction = units_closed / max(abs(float(trade_state["initial_units"])), 1e-12)
            trade_state["partial_fills"].append(
                {
                    "time": transaction["time"],
                    "price": price,
                    "fraction": fraction,
                    "pnl_dollars": pnl_dollars,
                }
            )
            trade_state["remaining_units"] = float(trade_state["remaining_units"]) - units_closed
            if trade_state["remaining_units"] > 1e-6:
                continue

            total_pnl = float(sum(fill["pnl_dollars"] for fill in trade_state["partial_fills"]))
            state["strategy_balance"] = float(state.get("strategy_balance", START_BALANCE)) + total_pnl
            side = str(trade_state["side"])
            entry_price = float(trade_state["entry_price"])
            instrument = str(trade_state["instrument"])
            pip = float(PIP_SIZES[instrument])
            pnl_pips = ((price - entry_price) / pip) if side == "long" else ((entry_price - price) / pip)
            exit_reason = infer_exit_reason(trade_state, price)
            hold_minutes = (
                pd.Timestamp(transaction["time"]).tz_convert("UTC") - pd.Timestamp(trade_state["entry_time"]).tz_convert("UTC")
            ).total_seconds() / 60.0
            generic_row = {
                "id": int(trade_id),
                "entry_time": trade_state["entry_time"],
                "exit_time": transaction["time"],
                "instrument": instrument,
                "side": side,
                "entry_price": entry_price,
                "exit_price": price,
                "pnl_pips": pnl_pips,
                "pnl_dollars": total_pnl,
                "balance_after": float(state["strategy_balance"]),
                "partial_fills": trade_state["partial_fills"],
            }
            upsert_trade_record(live_paths.db_path, generic_row)
            export_trade_ledger(live_paths.db_path, live_paths.exports)

            enriched_row = {
                "trade_id": int(trade_id),
                "sleeve": trade_state["sleeve"],
                "module": trade_state["module"],
                "instrument": instrument,
                "side": side,
                "weight": float(trade_state["weight"]),
                "entry_time": trade_state["entry_time"],
                "exit_time": transaction["time"],
                "entry_price": entry_price,
                "exit_price": price,
                "pnl_pips": pnl_pips,
                "pnl_dollars": total_pnl,
                "strategy_balance_after": float(state["strategy_balance"]),
                "broker_account_balance_after": broker_account_balance,
                "initial_units": float(trade_state["initial_units"]),
                "hold_minutes": float(hold_minutes),
                "stop_loss_price": float(trade_state["stop_loss_price"]),
                "take_profit_price": float(trade_state["take_profit_price"]),
                "shield_trigger_price": float(trade_state["shield_trigger_price"]),
                "shield_lock_price": float(trade_state["shield_lock_price"]),
                "shielded": bool(trade_state.get("shielded", False)),
                "exit_reason": exit_reason,
                "partial_fills_json": json.dumps(trade_state["partial_fills"], ensure_ascii=True),
            }
            append_csv_row(live_paths.exports / "closed_trades_enriched.csv", enriched_row)
            pd.DataFrame([enriched_row]).to_parquet(live_paths.exports / "closed_trades_enriched_last.parquet", index=False)
            refresh_live_exports(live_paths, float(state.get("strategy_start_balance", START_BALANCE)))
            log_json(
                logger,
                "trade_completed",
                trade_id=trade_id,
                sleeve=trade_state["sleeve"],
                module=trade_state["module"],
                instrument=instrument,
                pnl_dollars=total_pnl,
                exit_reason=exit_reason,
                strategy_balance=float(state["strategy_balance"]),
            )
            state["managed_trades"].pop(trade_id, None)


def signal_row(event: str, module: LockedModule, timestamp: pd.Timestamp, reason: str | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    row = {
        "timestamp": timestamp.isoformat(),
        "event": event,
        "sleeve": module.sleeve,
        "module": module.module,
        "instrument": module.instrument,
        "side": module.side,
        "weight": module.weight,
        "reason": reason or "",
    }
    if extra:
        row.update(extra)
    return row


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the six-sleeve clean OOS lock on the OANDA practice account.")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--live-root", default=str(DEFAULT_LIVE_ROOT))
    parser.add_argument("--env-file")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stream-seconds", type=int)
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--allow-existing-broker-state", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    live_paths = ensure_live_paths(Path(args.live_root))
    instance_lock = SingleInstanceLock(live_paths.root / "live_six_sleeve_engine.lock")
    instance_lock.acquire()
    logger = configure_rotating_logger("razerback.live_six_sleeve", live_paths.root / "live_trading.log")
    credentials = OandaCredentials.from_env(Path(args.env_file) if args.env_file else None)
    client = OandaClient(credentials)
    manifest_path = Path(args.manifest).expanduser().resolve()
    data_dir = Path(args.data_dir).expanduser().resolve()
    modules = load_locked_modules(manifest_path)
    modules_by_instrument: dict[str, list[LockedModule]] = {}
    for module in modules:
        modules_by_instrument.setdefault(module.instrument, []).append(module)

    account_summary = client.account_summary()
    open_trades = client.list_open_trades()
    pending_orders = client.list_pending_orders()
    effective_start_balance = min(START_BALANCE, float(account_summary.get("balance", START_BALANCE)))
    write_live_test_manifest(
        live_paths=live_paths,
        account_summary=account_summary,
        modules=modules,
        manifest_path=manifest_path,
        dry_run=args.dry_run,
        effective_start_balance=effective_start_balance,
    )
    log_json(
        logger,
        "engine_bootstrap",
        account_id=client.account_id,
        alias=account_summary.get("alias", ""),
        environment=credentials.environment,
        open_trades=len(open_trades),
        pending_orders=len(pending_orders),
        dry_run=args.dry_run,
    )

    if (open_trades or pending_orders) and not args.allow_existing_broker_state:
        raise SystemExit(
            f"Broker account is not clean (open_trades={len(open_trades)}, pending_orders={len(pending_orders)}). "
            "Flatten or rerun with --allow-existing-broker-state if you intentionally want to continue."
        )

    if args.validate_only:
        save_json(
            live_paths.root / "validation_snapshot.json",
            {
                "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
                "account_id": client.account_id,
                "alias": account_summary.get("alias", ""),
                "open_trades": len(open_trades),
                "pending_orders": len(pending_orders),
                "module_count": len(modules),
                "manifest_path": str(manifest_path),
                "effective_start_balance": effective_start_balance,
            },
        )
        return

    repo_instruments = sorted(set(module.instrument for module in modules) | {"usdjpy", "gbpusd"})
    oanda_instruments = [repo_to_oanda(instrument) for instrument in repo_instruments]
    instrument_meta = client.get_instruments(oanda_instruments)
    instrument_states = {instrument: InstrumentState(instrument, load_history(data_dir, instrument)) for instrument in repo_instruments}
    latest_quotes: dict[str, dict[str, Any]] = {}
    accumulators: dict[str, MinuteAccumulator] = {}

    if live_paths.state_path.exists():
        state = json.loads(live_paths.state_path.read_text(encoding="utf-8"))
    else:
        state = {
            "last_transaction_id": str(account_summary.get("lastTransactionID", "0")),
            "managed_trades": {},
            "pending_signals": {},
            "strategy_balance": effective_start_balance,
            "strategy_start_balance": effective_start_balance,
            "launched_at": pd.Timestamp.now(tz="UTC").isoformat(),
        }
    state.setdefault("managed_trades", {})
    state.setdefault("pending_signals", {})
    state.setdefault("strategy_balance", START_BALANCE)
    state.setdefault("strategy_start_balance", effective_start_balance)
    save_json(live_paths.state_path, state)

    def active_or_pending(module_name: str) -> bool:
        if module_name in state["pending_signals"]:
            return True
        return any(str(item["module"]) == module_name for item in state["managed_trades"].values())

    def process_due_signals(instrument: str, tick_ts: pd.Timestamp, bid: float, ask: float) -> None:
        due_names = [
            name
            for name, item in state["pending_signals"].items()
            if item["instrument"] == instrument and pd.Timestamp(item["execute_at"]) <= tick_ts
        ]
        if not due_names:
            return
        sync_completed_transactions(client=client, state=state, logger=logger, live_paths=live_paths)
        for module_name in sorted(due_names):
            pending = state["pending_signals"].pop(module_name, None)
            if pending is None:
                continue
            module = next(item for item in modules_by_instrument[instrument] if item.module == module_name)
            if len(state["managed_trades"]) >= MAX_CONCURRENT:
                row = signal_row("signal_skipped", module, tick_ts, reason="concurrency_cap")
                append_csv_row(live_paths.exports / "signal_journal.csv", row)
                log_json(logger, "signal_skipped", module=module.module, sleeve=module.sleeve, reason="concurrency_cap")
                continue
            entry_price = ask if module.side == "long" else bid
            open_notional = sum(float(item["usd_notional"]) for item in state["managed_trades"].values())
            units = calculate_units(
                module=module,
                entry_price=entry_price,
                strategy_balance=float(state["strategy_balance"]),
                open_notional=open_notional,
                instrument_states=instrument_states,
            )
            if units <= 0.0:
                row = signal_row("signal_skipped", module, tick_ts, reason="zero_units")
                append_csv_row(live_paths.exports / "signal_journal.csv", row)
                log_json(logger, "signal_skipped", module=module.module, sleeve=module.sleeve, reason="zero_units")
                continue
            if args.dry_run:
                row = signal_row(
                    "dry_run_order_candidate",
                    module,
                    tick_ts,
                    extra={"entry_price": entry_price, "units": units, "strategy_balance": state["strategy_balance"]},
                )
                append_csv_row(live_paths.exports / "signal_journal.csv", row)
                log_json(logger, "dry_run_order_candidate", module=module.module, sleeve=module.sleeve, entry_price=entry_price, units=units)
                continue

            side_sign = 1.0 if module.side == "long" else -1.0
            broker_instrument = instrument_meta[repo_to_oanda(instrument)]
            stop_loss_price = entry_price - module.stop_pips * PIP_SIZES[instrument] if module.side == "long" else entry_price + module.stop_pips * PIP_SIZES[instrument]
            take_profit_price = entry_price + module.target_pips * PIP_SIZES[instrument] if module.side == "long" else entry_price - module.target_pips * PIP_SIZES[instrument]
            shield_trigger_price = entry_price + module.shield_trigger_pips * PIP_SIZES[instrument] if module.side == "long" else entry_price - module.shield_trigger_pips * PIP_SIZES[instrument]
            shield_lock_price = entry_price + module.shield_lock_pips * PIP_SIZES[instrument] if module.side == "long" else entry_price - module.shield_lock_pips * PIP_SIZES[instrument]
            response = client.place_market_order(
                instrument=broker_instrument,
                units=side_sign * units,
                stop_loss_price=stop_loss_price,
                client_id=module.module,
                tag=f"SixSleeve:{module.sleeve}",
            )
            fill_tx = response.get("orderFillTransaction", {})
            opened = fill_tx.get("tradeOpened", {})
            trade_id = str(opened.get("tradeID", ""))
            if not trade_id:
                row = signal_row("order_failed", module, tick_ts, reason="no_trade_id", extra={"response_json": json.dumps(response, ensure_ascii=True)})
                append_csv_row(live_paths.exports / "signal_journal.csv", row)
                log_json(logger, "order_failed", module=module.module, sleeve=module.sleeve, response=response)
                continue
            fill_price = float(fill_tx.get("price", entry_price))
            filled_units = abs(float(opened.get("units", units)))
            try:
                client.update_trade_orders(trade_id, take_profit_price=take_profit_price, instrument=broker_instrument)
            except Exception as exc:  # pragma: no cover - network dependent
                log_json(logger, "take_profit_update_failed", module=module.module, trade_id=trade_id, error=str(exc))
            usd_notional = filled_units * usd_notional_per_unit(instrument, fill_price, instrument_states)
            state["managed_trades"][trade_id] = {
                "trade_id": trade_id,
                "module": module.module,
                "sleeve": module.sleeve,
                "instrument": instrument,
                "side": module.side,
                "weight": module.weight,
                "entry_time": fill_tx.get("time", tick_ts.isoformat()),
                "entry_price": fill_price,
                "initial_units": filled_units,
                "remaining_units": filled_units,
                "usd_notional": usd_notional,
                "stop_loss_price": stop_loss_price,
                "active_stop_loss_price": stop_loss_price,
                "take_profit_price": take_profit_price,
                "shield_trigger_price": shield_trigger_price,
                "shield_lock_price": shield_lock_price,
                "shielded": False,
                "ttl_at": (pd.Timestamp(fill_tx.get("time", tick_ts.isoformat())) + pd.Timedelta(minutes=module.ttl_minutes)).isoformat(),
                "partial_fills": [],
                "manual_exit_reason": None,
            }
            row = signal_row(
                "trade_opened",
                module,
                tick_ts,
                extra={
                    "trade_id": trade_id,
                    "entry_price": fill_price,
                    "units": filled_units,
                    "stop_loss_price": stop_loss_price,
                    "take_profit_price": take_profit_price,
                    "strategy_balance": state["strategy_balance"],
                },
            )
            append_csv_row(live_paths.exports / "signal_journal.csv", row)
            log_json(
                logger,
                "trade_opened",
                trade_id=trade_id,
                module=module.module,
                sleeve=module.sleeve,
                instrument=instrument,
                entry_price=fill_price,
                units=filled_units,
            )

    def process_shields(instrument: str, tick_ts: pd.Timestamp, bid: float, ask: float) -> None:
        if args.dry_run:
            return
        broker_instrument = instrument_meta[repo_to_oanda(instrument)]
        for trade_id, trade_state in list(state["managed_trades"].items()):
            if trade_state["instrument"] != instrument or trade_state.get("shielded", False):
                continue
            if trade_state["side"] == "long":
                armed = ask >= float(trade_state["shield_trigger_price"])
            else:
                armed = bid <= float(trade_state["shield_trigger_price"])
            if not armed:
                continue
            try:
                client.update_trade_orders(
                    trade_id,
                    stop_loss_price=float(trade_state["shield_lock_price"]),
                    instrument=broker_instrument,
                )
                trade_state["shielded"] = True
                trade_state["active_stop_loss_price"] = float(trade_state["shield_lock_price"])
                log_json(
                    logger,
                    "shield_armed",
                    trade_id=trade_id,
                    module=trade_state["module"],
                    sleeve=trade_state["sleeve"],
                    instrument=instrument,
                    timestamp=tick_ts.isoformat(),
                    stop_loss_price=trade_state["active_stop_loss_price"],
                )
            except Exception as exc:  # pragma: no cover - network dependent
                log_json(logger, "shield_update_failed", trade_id=trade_id, instrument=instrument, error=str(exc))

    def process_ttl(now_ts: pd.Timestamp) -> None:
        if args.dry_run:
            return
        for trade_id, trade_state in list(state["managed_trades"].items()):
            if now_ts < pd.Timestamp(trade_state["ttl_at"]):
                continue
            if trade_state.get("manual_exit_reason"):
                continue
            try:
                trade_state["manual_exit_reason"] = "ttl"
                client.close_trade(trade_id)
                log_json(logger, "trade_ttl_close_requested", trade_id=trade_id, module=trade_state["module"], sleeve=trade_state["sleeve"])
            except Exception as exc:  # pragma: no cover - network dependent
                trade_state["manual_exit_reason"] = None
                log_json(logger, "trade_ttl_close_failed", trade_id=trade_id, error=str(exc))

    price_event_count = 0
    stream_started = time.monotonic()
    while True:
        try:
            log_json(logger, "stream_connect", environment=credentials.environment, account_id=client.account_id)
            for payload in client.pricing_stream(oanda_instruments):
                if payload.get("type") != "PRICE":
                    continue
                price_event_count += 1
                oanda_name = str(payload["instrument"])
                instrument = oanda_to_repo(oanda_name)
                bid = float(payload["bids"][0]["price"])
                ask = float(payload["asks"][0]["price"])
                tick_ts = pd.Timestamp(payload["time"]).tz_convert("UTC")
                latest_quotes[instrument] = {"bid": bid, "ask": ask, "timestamp": tick_ts.isoformat()}

                process_due_signals(instrument, tick_ts, bid, ask)
                process_shields(instrument, tick_ts, bid, ask)

                minute = tick_ts.floor("min")
                if instrument not in accumulators:
                    accumulators[instrument] = MinuteAccumulator.from_quote(minute, bid, ask)
                    continue

                accumulator = accumulators[instrument]
                if minute == accumulator.minute:
                    accumulator.update(bid, ask)
                else:
                    completed = accumulator.as_row()
                    accumulators[instrument] = MinuteAccumulator.from_quote(minute, bid, ask)
                    instrument_states[instrument].append_bar(completed)
                    completed_ts = pd.Timestamp(completed["timestamp"]).tz_convert("UTC")
                    process_ttl(completed_ts)
                    sync_completed_transactions(client=client, state=state, logger=logger, live_paths=live_paths)

                    for module in modules_by_instrument.get(instrument, []):
                        if active_or_pending(module.module):
                            continue
                        triggered = evaluate_module(module, instrument_states[instrument])
                        if triggered:
                            execute_at = completed_ts + pd.Timedelta(minutes=1)
                            state["pending_signals"][module.module] = {
                                "execute_at": execute_at.isoformat(),
                                "instrument": instrument,
                                "scheduled_from": completed_ts.isoformat(),
                            }
                            row = signal_row("signal_scheduled", module, completed_ts, extra={"execute_at": execute_at.isoformat()})
                            append_csv_row(live_paths.exports / "signal_journal.csv", row)
                            log_json(
                                logger,
                                "signal_scheduled",
                                module=module.module,
                                sleeve=module.sleeve,
                                instrument=instrument,
                                execute_at=execute_at.isoformat(),
                            )

                    save_json(
                        live_paths.heartbeat_path,
                        {
                            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
                            "account_id": client.account_id,
                            "strategy_balance": float(state["strategy_balance"]),
                            "managed_trade_count": len(state["managed_trades"]),
                            "pending_signal_count": len(state["pending_signals"]),
                            "dry_run": args.dry_run,
                            "price_event_count": price_event_count,
                            "last_completed_bar_instrument": instrument,
                            "last_completed_bar_time": completed_ts.isoformat(),
                        },
                    )
                    save_json(live_paths.positions_snapshot_path, {"managed_trades": state["managed_trades"], "pending_signals": state["pending_signals"]})
                    save_json(live_paths.state_path, state)
                    if args.stream_seconds is not None and (time.monotonic() - stream_started) >= args.stream_seconds:
                        log_json(logger, "stream_complete", dry_run=args.dry_run, seconds=args.stream_seconds, price_event_count=price_event_count)
                        return
        except Exception as exc:  # pragma: no cover - network dependent
            log_json(logger, "stream_error", error=str(exc))
            save_json(live_paths.state_path, state)
            time.sleep(STREAM_SLEEP_SECONDS)


if __name__ == "__main__":
    main()
