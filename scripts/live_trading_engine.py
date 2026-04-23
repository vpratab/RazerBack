from __future__ import annotations

import argparse
import atexit
import json
import msvcrt
import os
import sys
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from continuation_core import ContinuationSpec, resolve_execution_profile
from live_reporting import ensure_live_paths, export_trade_ledger, upsert_trade_record
from locked_portfolio_runtime import load_locked_config, specs_from_rows
from logging_utils import configure_rotating_logger, log_json
from native_acceleration import hawkes_intensity_accelerated, rolling_gmm_nodes_accelerated
from oanda_client import OandaClient, OandaCredentials, OandaInstrument
from realistic_backtest import PIP_SIZES


LOCAL_SIGMA_BARS = 240
GMM_COMPONENTS = 4
GMM_LOOKBACK_HOURS = 720
GMM_REFIT_HOURS = 4
HISTORY_BARS = 60 * 24 * 45
STREAM_SLEEP_SECONDS = 5
DEFAULT_DRY_RUN_SECONDS = 90


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


def normalize_repo_instrument_name(value: str) -> str:
    return value.replace("_", "").lower()


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
        ask_diff = frame["close_ask"].diff().fillna(0.0).to_numpy(dtype=np.float64) / self.pip
        bid_diff = frame["close_bid"].diff().fillna(0.0).to_numpy(dtype=np.float64) / self.pip
        frame["ask_lambda"] = hawkes_intensity_accelerated(np.clip(ask_diff, 0.0, None), 0.15)
        frame["bid_lambda"] = hawkes_intensity_accelerated(np.clip(-bid_diff, 0.0, None), 0.15)
        weaker = np.minimum(frame["ask_lambda"], frame["bid_lambda"])
        stronger = np.maximum(frame["ask_lambda"], frame["bid_lambda"])
        frame["imbalance_ratio"] = np.where(weaker > 1e-12, stronger / weaker, np.inf)
        frame["local_sigma"] = frame["mid_close"].rolling(LOCAL_SIGMA_BARS).std(ddof=0)
        frame["node_mean"] = rolling_gmm_nodes_accelerated(
            frame["timestamp"],
            frame["mid_close"].to_numpy(dtype=np.float64),
            GMM_COMPONENTS,
            GMM_LOOKBACK_HOURS,
            GMM_REFIT_HOURS,
        )
        frame["distance_to_node_pips"] = (frame["mid_close"] - frame["node_mean"]).abs() / self.pip
        frame["ema_240"] = frame["mid_close"].ewm(span=240, adjust=False).mean()
        frame["local_sigma_ma_240"] = frame["local_sigma"].rolling(240).mean()
        frame["ret60"] = frame["mid_close"].diff(60) / self.pip
        frame["range_pips"] = (frame["mid_high"] - frame["mid_low"]) / self.pip
        frame["range_60"] = frame["range_pips"].rolling(60).mean()
        frame["range_240"] = frame["range_pips"].rolling(240).mean()
        frame["hour"] = frame["timestamp"].dt.hour
        frame["weekday"] = frame["timestamp"].dt.weekday
        self.frame = frame
        self.hourly_closes = deque(
            (
                frame.assign(hour_bucket=frame["timestamp"].dt.floor("1h"))
                .drop_duplicates("hour_bucket", keep="last")["mid_close"]
                .tail(GMM_LOOKBACK_HOURS + GMM_REFIT_HOURS + 24)
                .to_list()
            ),
            maxlen=GMM_LOOKBACK_HOURS + GMM_REFIT_HOURS + 24,
        )

    def append_bar(self, candle: dict[str, Any]) -> None:
        row = pd.DataFrame([candle])
        row["timestamp"] = pd.to_datetime(row["timestamp"], utc=True)
        frame = pd.concat([self.frame, row], ignore_index=True).tail(HISTORY_BARS).reset_index(drop=True)
        frame["mid_open"] = (frame["open_bid"] + frame["open_ask"]) / 2.0
        frame["mid_high"] = (frame["high_bid"] + frame["high_ask"]) / 2.0
        frame["mid_low"] = (frame["low_bid"] + frame["low_ask"]) / 2.0
        frame["mid_close"] = (frame["close_bid"] + frame["close_ask"]) / 2.0
        ask_diff = (frame["close_ask"].iloc[-1] - frame["close_ask"].iloc[-2]) / self.pip
        bid_diff = (frame["close_bid"].iloc[-1] - frame["close_bid"].iloc[-2]) / self.pip
        decay = float(np.exp(-0.15))
        prev_ask = float(frame["ask_lambda"].iloc[-2]) if "ask_lambda" in frame.columns and pd.notna(frame["ask_lambda"].iloc[-2]) else 0.0
        prev_bid = float(frame["bid_lambda"].iloc[-2]) if "bid_lambda" in frame.columns and pd.notna(frame["bid_lambda"].iloc[-2]) else 0.0
        ask_lambda = prev_ask * decay + max(ask_diff, 0.0)
        bid_lambda = prev_bid * decay + max(-bid_diff, 0.0)
        weaker = min(ask_lambda, bid_lambda)
        stronger = max(ask_lambda, bid_lambda)
        imbalance_ratio = np.inf if weaker <= 1e-12 else stronger / weaker

        new_hour = frame["timestamp"].iloc[-1].floor("1h")
        prev_hour = frame["timestamp"].iloc[-2].floor("1h")
        if new_hour != prev_hour:
            self.hourly_closes.append(float(frame["mid_close"].iloc[-2]))
        should_refit_node = (
            new_hour != prev_hour
            and len(self.hourly_closes) >= GMM_LOOKBACK_HOURS
            and (len(self.hourly_closes) % GMM_REFIT_HOURS == 0)
        )
        if should_refit_node:
            frame["node_mean"] = rolling_gmm_nodes_accelerated(
                frame["timestamp"],
                frame["mid_close"].to_numpy(dtype=np.float64),
                GMM_COMPONENTS,
                GMM_LOOKBACK_HOURS,
                GMM_REFIT_HOURS,
            )
            node_mean = float(frame["node_mean"].iloc[-1])
        else:
            node_mean = float(frame["node_mean"].iloc[-2]) if "node_mean" in frame.columns and pd.notna(frame["node_mean"].iloc[-2]) else float(frame["mid_close"].iloc[-1])
            frame.loc[frame.index[-1], "node_mean"] = node_mean

        frame.loc[frame.index[-1], "ask_lambda"] = ask_lambda
        frame.loc[frame.index[-1], "bid_lambda"] = bid_lambda
        frame.loc[frame.index[-1], "imbalance_ratio"] = imbalance_ratio
        frame.loc[frame.index[-1], "local_sigma"] = float(frame["mid_close"].tail(LOCAL_SIGMA_BARS).std(ddof=0))
        frame.loc[frame.index[-1], "distance_to_node_pips"] = abs(float(frame["mid_close"].iloc[-1]) - node_mean) / self.pip
        frame.loc[frame.index[-1], "ema_240"] = float(frame["mid_close"].ewm(span=240, adjust=False).mean().iloc[-1])
        frame.loc[frame.index[-1], "local_sigma_ma_240"] = float(frame["local_sigma"].tail(240).mean())
        frame.loc[frame.index[-1], "ret60"] = (
            float(frame["mid_close"].iloc[-1] - frame["mid_close"].iloc[-61]) / self.pip if len(frame) > 60 else np.nan
        )
        frame.loc[frame.index[-1], "range_pips"] = float(frame["mid_high"].iloc[-1] - frame["mid_low"].iloc[-1]) / self.pip
        frame.loc[frame.index[-1], "range_60"] = float(frame["range_pips"].tail(60).mean())
        frame.loc[frame.index[-1], "range_240"] = float(frame["range_pips"].tail(240).mean())
        frame.loc[frame.index[-1], "hour"] = int(frame["timestamp"].iloc[-1].hour)
        frame.loc[frame.index[-1], "weekday"] = int(frame["timestamp"].iloc[-1].weekday())
        self.frame = frame

    def latest(self) -> pd.Series:
        return self.frame.iloc[-1]

    def minute_series(self) -> pd.Series:
        return self.frame.set_index("timestamp")["mid_close"]


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
    if instrument == "usdjpy":
        return pip / max(entry_price, 1e-9)
    if quote == "jpy":
        usdjpy = float(instrument_states["usdjpy"].latest()["mid_close"])
        return pip / max(usdjpy, 1e-9)
    if quote == "gbp":
        gbpusd = float(instrument_states["gbpusd"].latest()["mid_close"])
        return pip * gbpusd
    if quote == "chf":
        usdchf = float(instrument_states["usdchf"].latest()["mid_close"]) if "usdchf" in instrument_states else 1.0
        return pip / max(usdchf, 1e-9)
    if quote == "cad":
        usdcad = float(instrument_states["usdcad"].latest()["mid_close"]) if "usdcad" in instrument_states else 1.0
        return pip / max(usdcad, 1e-9)
    return pip


def calculate_units(
    *,
    spec: ContinuationSpec,
    entry_price: float,
    account_balance: float,
    risk_pct: float,
    max_leverage: float,
    open_notional: float,
    instrument_states: dict[str, InstrumentState],
) -> float:
    pip_value = pip_value_per_unit(spec.instrument, instrument_states, entry_price)
    risk_dollars = account_balance * risk_pct
    units_by_risk = risk_dollars / max(spec.stop_loss_pips * pip_value, 1e-12)
    available_notional = max(account_balance * max_leverage - open_notional, 0.0)
    units_by_leverage = available_notional / max(entry_price, 1e-12)
    return float(max(0.0, min(units_by_risk, units_by_leverage)))


def evaluate_spec(spec: ContinuationSpec, state: InstrumentState) -> bool:
    row = state.latest()
    if int(row["weekday"]) >= 5:
        return False
    if int(row["hour"]) < spec.hour_start or int(row["hour"]) > spec.hour_end:
        return False
    if not np.isfinite(row["range_60"]) or not np.isfinite(row["range_240"]) or row["range_240"] <= 0.0:
        return False
    if row["range_60"] <= row["range_240"] * spec.range_ratio_min:
        return False
    if not np.isfinite(row["distance_to_node_pips"]) or row["distance_to_node_pips"] >= spec.node_distance_max:
        return False
    if not np.isfinite(row["local_sigma"]) or not np.isfinite(row["local_sigma_ma_240"]) or row["local_sigma"] <= row["local_sigma_ma_240"] * spec.local_sigma_ratio_min:
        return False
    if row["imbalance_ratio"] <= spec.imbalance_min:
        return False

    if spec.side == "long":
        direction = row["ask_lambda"] > row["bid_lambda"]
        trend_ok = row["mid_close"] > row["ema_240"]
        momentum_ok = row["ret60"] > spec.ret60_min_abs
    else:
        direction = row["bid_lambda"] > row["ask_lambda"]
        trend_ok = row["mid_close"] < row["ema_240"]
        momentum_ok = row["ret60"] < -spec.ret60_min_abs
    return bool(direction and trend_ok and momentum_ok)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the live OANDA continuation engine.")
    parser.add_argument("--config", default=str(Path(__file__).resolve().parent.parent / "configs" / "continuation_portfolio_total_v1.json"))
    parser.add_argument("--data-dir", default="C:/fx_data/m1")
    parser.add_argument("--live-root", default="C:/fx_data/live")
    parser.add_argument("--scenario", default="conservative", choices=("base", "conservative", "hard"))
    parser.add_argument("--env-file")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stream-seconds", type=int)
    parser.add_argument("--test-trade", help="Instrument to trade once, e.g. EURUSD or EUR_USD.")
    parser.add_argument("--units", type=float, default=1.0, help="Unit size for --test-trade.")
    return parser.parse_args()


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")


def sync_completed_transactions(
    client: OandaClient,
    state: dict[str, Any],
    logger,
    db_path: Path,
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
        account_balance = float(transaction.get("accountBalance", 0.0))
        for closed in transaction.get("tradesClosed", []):
            trade_id = str(closed["tradeID"])
            broker_trade = state.get("managed_trades", {}).get(trade_id)
            if broker_trade is None:
                continue
            price = float(closed.get("price", transaction.get("price", broker_trade["entry_price"])))
            units_closed = abs(float(closed.get("units", broker_trade["remaining_units"])))
            pnl_dollars = float(closed.get("realizedPL", 0.0)) + float(closed.get("financing", 0.0))
            fraction = units_closed / max(abs(float(broker_trade["initial_units"])), 1e-12)
            broker_trade["partial_fills"].append(
                {
                    "time": transaction["time"],
                    "price": price,
                    "fraction": fraction,
                    "pnl_dollars": pnl_dollars,
                }
            )
            broker_trade["remaining_units"] = float(broker_trade["remaining_units"]) - units_closed
            if broker_trade["remaining_units"] <= 1e-6:
                side = broker_trade["side"]
                entry_price = float(broker_trade["entry_price"])
                pip = float(PIP_SIZES[broker_trade["instrument"]])
                pnl_pips = ((price - entry_price) / pip) if side == "long" else ((entry_price - price) / pip)
                row = {
                    "id": int(trade_id),
                    "entry_time": broker_trade["entry_time"],
                    "exit_time": transaction["time"],
                    "instrument": broker_trade["instrument"],
                    "side": side,
                    "entry_price": entry_price,
                    "exit_price": price,
                    "pnl_pips": pnl_pips,
                    "pnl_dollars": float(sum(fill["pnl_dollars"] for fill in broker_trade["partial_fills"])),
                    "balance_after": account_balance,
                    "partial_fills": broker_trade["partial_fills"],
                }
                upsert_trade_record(db_path, row)
                log_json(logger, "trade_completed", trade_id=trade_id, instrument=row["instrument"], pnl_dollars=row["pnl_dollars"])
                state["managed_trades"].pop(trade_id, None)
                export_trade_ledger(db_path, db_path.parent / "exports")


def cancel_pending_orders(client: OandaClient, trade_state: dict[str, Any], logger) -> None:
    for order_id in trade_state.get("ladder_order_ids", []):
        try:
            client.cancel_order(str(order_id))
            log_json(logger, "cancel_order", order_id=order_id)
        except Exception as exc:  # pragma: no cover - network dependent
            log_json(logger, "cancel_order_failed", order_id=order_id, error=str(exc))


def summarize_account(client: OandaClient, logger) -> dict[str, Any]:
    summary = client.account_summary()
    log_json(
        logger,
        "account_summary",
        account_id=client.account_id,
        alias=summary.get("alias", ""),
        balance=summary.get("balance"),
        nav=summary.get("NAV"),
        open_trade_count=summary.get("openTradeCount"),
        pending_order_count=summary.get("pendingOrderCount"),
        last_transaction_id=summary.get("lastTransactionID"),
    )
    return summary


def run_test_trade(
    *,
    client: OandaClient,
    logger,
    live_paths,
    instrument_value: str,
    units: float,
) -> None:
    repo_instrument = normalize_repo_instrument_name(instrument_value)
    if repo_instrument not in PIP_SIZES:
        raise SystemExit(f"Unsupported test-trade instrument: {instrument_value}")

    oanda_name = repo_to_oanda(repo_instrument)
    instrument_meta = client.get_instruments([oanda_name])
    broker_instrument = instrument_meta[oanda_name]
    side_units = abs(float(units))

    summarize_account(client, logger)
    response = client.place_market_order(
        instrument=broker_instrument,
        units=side_units,
        client_id=f"test:{repo_instrument}",
        tag="RazerBackTestTrade",
    )
    fill_tx = response.get("orderFillTransaction", {})
    opened = fill_tx.get("tradeOpened", {})
    trade_id = str(opened.get("tradeID", ""))
    if not trade_id:
        raise SystemExit(f"Test trade failed to open: {response}")

    entry_price = float(fill_tx.get("price"))
    entry_time = str(fill_tx.get("time"))
    log_json(logger, "test_trade_opened", instrument=repo_instrument, trade_id=trade_id, entry_price=entry_price, units=side_units)
    time.sleep(2)

    close_response = client.close_trade(trade_id)
    close_fill = close_response.get("orderFillTransaction", {})
    closed = (close_fill.get("tradesClosed") or [{}])[0]
    exit_price = float(closed.get("price", close_fill.get("price", entry_price)))
    exit_time = str(close_fill.get("time", entry_time))
    pnl_dollars = float(closed.get("realizedPL", 0.0)) + float(closed.get("financing", 0.0))
    balance_after = float(close_fill.get("accountBalance", 0.0))
    pip = float(PIP_SIZES[repo_instrument])
    pnl_pips = (exit_price - entry_price) / pip
    row = {
        "id": int(trade_id),
        "entry_time": entry_time,
        "exit_time": exit_time,
        "instrument": repo_instrument,
        "side": "long",
        "entry_price": entry_price,
        "exit_price": exit_price,
        "pnl_pips": pnl_pips,
        "pnl_dollars": pnl_dollars,
        "balance_after": balance_after,
        "partial_fills": [
            {"time": entry_time, "price": entry_price, "fraction": 1.0, "event": "entry"},
            {"time": exit_time, "price": exit_price, "fraction": 1.0, "event": "exit"},
        ],
    }
    upsert_trade_record(live_paths.db_path, row)
    export_trade_ledger(live_paths.db_path, live_paths.exports)
    log_json(logger, "test_trade_closed", instrument=repo_instrument, trade_id=trade_id, exit_price=exit_price, pnl_dollars=pnl_dollars)


def main() -> None:
    args = parse_args()
    live_paths = ensure_live_paths(Path(args.live_root))
    instance_lock = SingleInstanceLock(live_paths.root / "live_trading_engine.lock")
    instance_lock.acquire()
    logger = configure_rotating_logger("razerback.live", live_paths.root / "live_trading.log")
    credentials = OandaCredentials.from_env(Path(args.env_file) if args.env_file else None)
    client = OandaClient(credentials)
    if args.dry_run and args.stream_seconds is None:
        args.stream_seconds = DEFAULT_DRY_RUN_SECONDS

    if args.test_trade:
        run_test_trade(
            client=client,
            logger=logger,
            live_paths=live_paths,
            instrument_value=args.test_trade,
            units=args.units,
        )
        return

    config = load_locked_config(Path(args.config), data_dir_override=Path(args.data_dir), scenario=args.scenario)
    specs = specs_from_rows(config.modules)
    scenario = resolve_execution_profile(args.scenario)
    repo_instruments = sorted({spec.instrument for spec in specs} | {"usdjpy", "gbpusd"})
    oanda_instruments = [repo_to_oanda(instrument) for instrument in repo_instruments]
    instrument_meta = client.get_instruments(oanda_instruments)
    instrument_states = {instrument: InstrumentState(instrument, load_history(Path(args.data_dir), instrument)) for instrument in repo_instruments}
    accumulators: dict[str, MinuteAccumulator] = {}
    summary = summarize_account(client, logger)

    state: dict[str, Any]
    if live_paths.state_path.exists():
        state = json.loads(live_paths.state_path.read_text(encoding="utf-8"))
    else:
        state = {
            "last_transaction_id": str(summary.get("lastTransactionID", "0")),
            "managed_trades": {},
            "pending_signals": {},
        }
    stream_started = time.monotonic()
    price_event_count = 0

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
                timestamp = pd.Timestamp(payload["time"]).tz_convert("UTC")
                minute = timestamp.floor("min")
                if args.dry_run and price_event_count <= 25:
                    log_json(logger, "price_tick", instrument=instrument, bid=bid, ask=ask, timestamp=timestamp.isoformat())

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

                    if state["pending_signals"]:
                        now_iso = completed["timestamp"].isoformat()
                        due_modules = [name for name, item in state["pending_signals"].items() if item["execute_at"] <= now_iso]
                        for module_name in due_modules:
                            signal = state["pending_signals"].pop(module_name)
                            spec = next(spec for spec in specs if spec.name == module_name)
                            side_sign = 1.0 if spec.side == "long" else -1.0
                            latest = instrument_states[spec.instrument].latest()
                            entry_price = float(latest["close_ask"] if spec.side == "long" else latest["close_bid"])
                            account_balance = float(client.account_summary().get("balance", 0.0))
                            open_notional = sum(abs(float(item["entry_price"]) * float(item["remaining_units"])) for item in state["managed_trades"].values())
                            units = calculate_units(
                                spec=spec,
                                entry_price=entry_price,
                                account_balance=account_balance,
                                risk_pct=config.risk_pct / 100.0,
                                max_leverage=config.max_leverage,
                                open_notional=open_notional,
                                instrument_states=instrument_states,
                            )
                            if units <= 0.0:
                                log_json(logger, "signal_skipped", module=spec.name, reason="zero_units")
                                continue
                            broker_instrument = instrument_meta[repo_to_oanda(spec.instrument)]
                            signed_units = side_sign * units
                            stop_loss_price = entry_price - spec.stop_loss_pips * PIP_SIZES[spec.instrument] if spec.side == "long" else entry_price + spec.stop_loss_pips * PIP_SIZES[spec.instrument]
                            if args.dry_run:
                                log_json(
                                    logger,
                                    "dry_run_order_candidate",
                                    module=spec.name,
                                    instrument=spec.instrument,
                                    side=spec.side,
                                    execute_at=signal["execute_at"],
                                    entry_price=entry_price,
                                    stop_loss_price=stop_loss_price,
                                    units=units,
                                    scenario=scenario.name,
                                )
                                continue
                            response = client.place_market_order(
                                instrument=broker_instrument,
                                units=signed_units,
                                stop_loss_price=stop_loss_price,
                                client_id=spec.name,
                                tag="RazerBackLive",
                            )
                            fill_tx = response.get("orderFillTransaction", {})
                            opened = fill_tx.get("tradeOpened", {})
                            trade_id = str(opened.get("tradeID", ""))
                            if not trade_id:
                                log_json(logger, "order_failed", module=spec.name, response=response)
                                continue
                            fill_price = float(fill_tx.get("price", entry_price))
                            filled_units = abs(float(opened.get("units", units)))
                            ladder_ids: list[str] = []
                            for idx, (ladder_pips, fraction) in enumerate(zip(spec.ladder_pips, spec.ladder_fractions), start=1):
                                reduce_units = -side_sign * filled_units * float(fraction)
                                tp_price = fill_price + ladder_pips * PIP_SIZES[spec.instrument] if spec.side == "long" else fill_price - ladder_pips * PIP_SIZES[spec.instrument]
                                try:
                                    limit_response = client.place_limit_order(
                                        instrument=broker_instrument,
                                        units=reduce_units,
                                        price=tp_price,
                                        client_id=f"{spec.name}:tp{idx}",
                                        tag="RazerBackTP",
                                        reduce_only=True,
                                    )
                                    ladder_ids.append(str(limit_response.get("orderCreateTransaction", {}).get("id", "")))
                                except Exception as exc:  # pragma: no cover - network dependent
                                    log_json(logger, "limit_order_failed", module=spec.name, level=idx, error=str(exc))
                            state["managed_trades"][trade_id] = {
                                "module": spec.name,
                                "instrument": spec.instrument,
                                "side": spec.side,
                                "entry_time": fill_tx.get("time", now_iso),
                                "entry_price": fill_price,
                                "initial_units": filled_units,
                                "remaining_units": filled_units,
                                "ladder_order_ids": [value for value in ladder_ids if value],
                                "ttl_at": (pd.Timestamp(fill_tx.get("time", now_iso)) + pd.Timedelta(minutes=spec.ttl_bars)).isoformat(),
                                "trail_pips": spec.trail_stop_pips,
                                "partial_fills": [],
                                "trailing_armed": False,
                            }
                            log_json(logger, "trade_opened", module=spec.name, trade_id=trade_id, entry_price=fill_price, units=filled_units)

                    for spec in specs:
                        active = any(item["module"] == spec.name for item in state["managed_trades"].values())
                        scheduled = spec.name in state["pending_signals"]
                        if active or scheduled or spec.instrument != instrument:
                            continue
                        triggered = evaluate_spec(spec, instrument_states[instrument])
                        if args.dry_run:
                            log_json(
                                logger,
                                "signal_evaluated",
                                module=spec.name,
                                instrument=spec.instrument,
                                timestamp=completed["timestamp"].isoformat(),
                                triggered=triggered,
                            )
                        if triggered:
                            execute_at = pd.Timestamp(completed["timestamp"]) + pd.Timedelta(minutes=1 + scenario.entry_delay_bars)
                            state["pending_signals"][spec.name] = {
                                "execute_at": execute_at.isoformat(),
                                "instrument": spec.instrument,
                            }
                            log_json(
                                logger,
                                "signal_scheduled",
                                module=spec.name,
                                instrument=spec.instrument,
                                execute_at=execute_at.isoformat(),
                                scenario=scenario.name,
                                dry_run=args.dry_run,
                            )

                    if not args.dry_run:
                        now_utc = pd.Timestamp.now(tz="UTC")
                        for trade_id, trade_state in list(state["managed_trades"].items()):
                            if now_utc.isoformat() >= trade_state["ttl_at"]:
                                try:
                                    cancel_pending_orders(client, trade_state, logger)
                                    client.close_trade(trade_id)
                                    log_json(logger, "trade_ttl_close", trade_id=trade_id)
                                except Exception as exc:  # pragma: no cover - network dependent
                                    log_json(logger, "trade_ttl_close_failed", trade_id=trade_id, error=str(exc))

                        sync_completed_transactions(client, state, logger, live_paths.db_path)
                    save_json(live_paths.state_path, state)
                    save_json(
                        live_paths.heartbeat_path,
                        {
                            "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
                            "account_id": client.account_id,
                            "managed_trade_count": len(state["managed_trades"]),
                            "pending_signal_count": len(state["pending_signals"]),
                            "dry_run": args.dry_run,
                            "price_event_count": price_event_count,
                        },
                    )
                    save_json(live_paths.positions_snapshot_path, {"managed_trades": state["managed_trades"]})
                    if args.stream_seconds is not None and (time.monotonic() - stream_started) >= args.stream_seconds:
                        log_json(
                            logger,
                            "stream_complete",
                            dry_run=args.dry_run,
                            seconds=args.stream_seconds,
                            price_event_count=price_event_count,
                        )
                        return
        except Exception as exc:  # pragma: no cover - network dependent
            log_json(logger, "stream_error", error=str(exc))
            save_json(live_paths.state_path, state)
            time.sleep(STREAM_SLEEP_SECONDS)


if __name__ == "__main__":
    main()
