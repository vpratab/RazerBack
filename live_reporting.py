from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


TRADES_SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY,
    entry_time TEXT,
    exit_time TEXT,
    instrument TEXT,
    side TEXT,
    entry_price REAL,
    exit_price REAL,
    pnl_pips REAL,
    pnl_dollars REAL,
    balance_after REAL,
    partial_fills TEXT
)
"""


@dataclass(frozen=True)
class LivePaths:
    root: Path
    logs: Path
    reports_daily: Path
    reports_weekly: Path
    exports: Path
    db_path: Path
    heartbeat_path: Path
    state_path: Path
    positions_snapshot_path: Path


def ensure_live_paths(root: Path) -> LivePaths:
    root = root.expanduser().resolve()
    logs = root / "logs"
    reports_daily = root / "reports" / "daily"
    reports_weekly = root / "reports" / "investor"
    exports = root / "exports"
    for path in (root, logs, reports_daily, reports_weekly, exports):
        path.mkdir(parents=True, exist_ok=True)
    return LivePaths(
        root=root,
        logs=logs,
        reports_daily=reports_daily,
        reports_weekly=reports_weekly,
        exports=exports,
        db_path=root / "trades.db",
        heartbeat_path=root / "engine_heartbeat.json",
        state_path=root / "engine_state.json",
        positions_snapshot_path=root / "positions_snapshot.json",
    )


def connect_trade_db(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(db_path)
    connection.execute(TRADES_SCHEMA)
    connection.commit()
    return connection


def ensure_trade_db(db_path: Path) -> None:
    connection = connect_trade_db(db_path)
    connection.close()


def upsert_trade_record(db_path: Path, row: dict[str, Any]) -> None:
    ensure_trade_db(db_path)
    connection = sqlite3.connect(db_path)
    columns = [
        "id",
        "entry_time",
        "exit_time",
        "instrument",
        "side",
        "entry_price",
        "exit_price",
        "pnl_pips",
        "pnl_dollars",
        "balance_after",
        "partial_fills",
    ]
    payload = {column: row.get(column) for column in columns}
    payload["partial_fills"] = _normalize_partials(payload.get("partial_fills"))
    connection.execute(
        """
        INSERT INTO trades (
            id, entry_time, exit_time, instrument, side, entry_price, exit_price,
            pnl_pips, pnl_dollars, balance_after, partial_fills
        )
        VALUES (
            :id, :entry_time, :exit_time, :instrument, :side, :entry_price, :exit_price,
            :pnl_pips, :pnl_dollars, :balance_after, :partial_fills
        )
        ON CONFLICT(id) DO UPDATE SET
            entry_time=excluded.entry_time,
            exit_time=excluded.exit_time,
            instrument=excluded.instrument,
            side=excluded.side,
            entry_price=excluded.entry_price,
            exit_price=excluded.exit_price,
            pnl_pips=excluded.pnl_pips,
            pnl_dollars=excluded.pnl_dollars,
            balance_after=excluded.balance_after,
            partial_fills=excluded.partial_fills
        """,
        payload,
    )
    connection.commit()
    connection.close()


def _normalize_partials(value: Any) -> str:
    if value is None:
        return "[]"
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=True)


def load_trades(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        return pd.DataFrame(
            columns=[
                "id",
                "entry_time",
                "exit_time",
                "instrument",
                "side",
                "entry_price",
                "exit_price",
                "pnl_pips",
                "pnl_dollars",
                "balance_after",
                "partial_fills",
            ]
        )
    connection = sqlite3.connect(db_path)
    frame = pd.read_sql_query("SELECT * FROM trades ORDER BY exit_time, id", connection)
    connection.close()
    if frame.empty:
        return frame
    frame["entry_time"] = pd.to_datetime(frame["entry_time"], utc=True, errors="coerce")
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True, errors="coerce")
    return frame


def export_trade_ledger(db_path: Path, output_dir: Path) -> tuple[Path, Path]:
    frame = load_trades(db_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "trade_ledger.csv"
    parquet_path = output_dir / "trade_ledger.parquet"
    frame.to_csv(csv_path, index=False)
    frame.to_parquet(parquet_path, index=False)
    return csv_path, parquet_path


def equity_curve(trades: pd.DataFrame, start_balance: float = 0.0) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame([{"timestamp": pd.NaT, "equity": start_balance}])
    frame = trades.sort_values("exit_time").reset_index(drop=True).copy()
    if "balance_after" in frame.columns and frame["balance_after"].notna().any():
        frame["equity"] = frame["balance_after"].astype(float)
    else:
        frame["equity"] = start_balance + frame["pnl_dollars"].astype(float).cumsum()
    return frame[["exit_time", "equity"]].rename(columns={"exit_time": "timestamp"})


def performance_summary(trades: pd.DataFrame, start_balance: float = 0.0) -> dict[str, float]:
    if trades.empty:
        return {
            "trade_count": 0.0,
            "win_rate_pct": 0.0,
            "profit_factor": 0.0,
            "total_pnl_dollars": 0.0,
            "total_pnl_pips": 0.0,
            "max_drawdown_pct": 0.0,
            "sharpe_like": 0.0,
            "sortino_like": 0.0,
        }

    pnl = trades["pnl_dollars"].to_numpy(dtype=float)
    gross_profit = pnl[pnl > 0.0].sum()
    gross_loss = -pnl[pnl < 0.0].sum()
    profit_factor = float(gross_profit / gross_loss) if gross_loss > 0.0 else float("inf")
    equity = np.concatenate(([start_balance], equity_curve(trades, start_balance)["equity"].to_numpy(dtype=float)))
    peak = np.maximum.accumulate(equity)
    drawdown = equity / np.maximum(peak, 1e-12) - 1.0
    returns = pd.Series(pnl)
    sharpe_like = 0.0 if returns.std(ddof=0) <= 1e-12 else float(returns.mean() / returns.std(ddof=0))
    downside = returns[returns < 0.0]
    if downside.empty:
        sortino_like = 0.0
    else:
        downside_std = float(downside.std(ddof=0))
        sortino_like = 0.0 if downside_std <= 1e-12 else float(returns.mean() / downside_std)

    return {
        "trade_count": float(len(trades)),
        "win_rate_pct": float((pnl > 0.0).mean() * 100.0),
        "profit_factor": float(profit_factor),
        "total_pnl_dollars": float(pnl.sum()),
        "total_pnl_pips": float(trades["pnl_pips"].astype(float).sum()),
        "max_drawdown_pct": float(drawdown.min() * 100.0),
        "sharpe_like": sharpe_like,
        "sortino_like": sortino_like,
    }


def daily_pnl(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["date", "pnl_dollars"])
    frame = trades.copy()
    frame["date"] = pd.to_datetime(frame["exit_time"], utc=True).dt.tz_convert("America/New_York").dt.date
    return (
        frame.groupby("date")
        .agg(pnl_dollars=("pnl_dollars", "sum"), trades=("id", "size"))
        .reset_index()
        .sort_values("date")
    )


def monthly_returns(trades: pd.DataFrame, start_balance: float = 0.0) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["month", "pnl_dollars", "return_pct", "trades"])
    frame = trades.copy()
    frame["month"] = pd.to_datetime(frame["exit_time"], utc=True).dt.to_period("M").astype(str)
    grouped = frame.groupby("month").agg(pnl_dollars=("pnl_dollars", "sum"), trades=("id", "size")).reset_index()
    balance = start_balance
    returns: list[float] = []
    for pnl_dollars in grouped["pnl_dollars"].astype(float):
        start_nav = balance
        balance += pnl_dollars
        returns.append(((balance / start_nav) - 1.0) * 100.0 if start_nav > 0 else 0.0)
    grouped["return_pct"] = returns
    return grouped


def attribution_table(trades: pd.DataFrame, column: str) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=[column, "trades", "pnl_dollars", "win_rate_pct"])
    return (
        trades.groupby(column)
        .agg(
            trades=("id", "size"),
            pnl_dollars=("pnl_dollars", "sum"),
            win_rate_pct=("pnl_dollars", lambda values: (values > 0.0).mean() * 100.0),
        )
        .reset_index()
        .sort_values("pnl_dollars", ascending=False)
    )
