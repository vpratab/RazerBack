from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from continuation_core import ContinuationSpec, load_datasets, simulate_module, simulate_portfolio


@dataclass(frozen=True)
class LockedPortfolioConfig:
    name: str
    description: str
    config_path: Path
    data_dir: Path
    output_dir: Path
    start_balance: float
    risk_pct: float
    max_leverage: float
    max_concurrent: int
    modules: list[dict[str, Any]]


def _resolve_path(base_dir: Path, value: str) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def normalize_module_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        spec = ContinuationSpec(**{key: value for key, value in row.items() if key != "name"})
        normalized.append({"name": spec.name, **{key: value for key, value in row.items() if key != "name"}})
    return normalized


def specs_from_rows(rows: list[dict[str, Any]]) -> list[ContinuationSpec]:
    return [ContinuationSpec(**{key: value for key, value in row.items() if key != "name"}) for row in rows]


def load_locked_config(
    config_path: Path,
    data_dir_override: Path | None = None,
    output_dir_override: Path | None = None,
    start_balance: float | None = None,
    risk_pct: float | None = None,
    max_leverage: float | None = None,
    max_concurrent: int | None = None,
) -> LockedPortfolioConfig:
    config_path = config_path.expanduser().resolve()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    base_dir = config_path.parent

    name = str(raw.get("name", config_path.stem))
    description = str(raw.get("description", "")).strip()
    default_data_dir = _resolve_path(base_dir, str(raw.get("data_dir", "../..")))
    data_dir = data_dir_override.expanduser().resolve() if data_dir_override is not None else default_data_dir
    default_output_dir = _resolve_path(base_dir, str(raw.get("output_dir", f"../output/{name}")))
    output_dir = output_dir_override.expanduser().resolve() if output_dir_override is not None else default_output_dir

    modules = normalize_module_rows(list(raw["modules"]))
    return LockedPortfolioConfig(
        name=name,
        description=description,
        config_path=config_path,
        data_dir=data_dir,
        output_dir=output_dir,
        start_balance=float(start_balance if start_balance is not None else raw.get("start_balance", 1500.0)),
        risk_pct=float(risk_pct if risk_pct is not None else raw["risk_pct"]),
        max_leverage=float(max_leverage if max_leverage is not None else raw.get("max_leverage", 35.0)),
        max_concurrent=int(max_concurrent if max_concurrent is not None else raw.get("max_concurrent", 2)),
        modules=modules,
    )


def required_instruments(specs: list[ContinuationSpec]) -> list[str]:
    required = {spec.instrument for spec in specs}
    required.update({"usdjpy", "gbpusd"})
    return sorted(required)


def build_trade_stats(filled_df: pd.DataFrame) -> pd.DataFrame:
    if filled_df.empty:
        return pd.DataFrame(
            [
                {
                    "trade_count": 0,
                    "win_rate_pct": 0.0,
                    "profit_factor": 0.0,
                    "avg_pnl_dollars": 0.0,
                    "median_pnl_dollars": 0.0,
                    "avg_pnl_pips": 0.0,
                    "median_pnl_pips": 0.0,
                    "avg_hold_minutes": 0.0,
                    "median_hold_minutes": 0.0,
                    "p90_hold_minutes": 0.0,
                    "worst_trade_dollars": 0.0,
                    "worst_trade_pips": 0.0,
                    "best_trade_dollars": 0.0,
                    "best_trade_pips": 0.0,
                }
            ]
        )

    hold_minutes = (
        (pd.to_datetime(filled_df["exit_time"], utc=True) - pd.to_datetime(filled_df["entry_time"], utc=True))
        .dt.total_seconds()
        .div(60.0)
        .to_numpy(dtype=float)
    )
    pnl_dollars = filled_df["pnl_dollars"].to_numpy(dtype=float)
    pnl_pips = filled_df["pnl_pips"].to_numpy(dtype=float)
    gross_profit = pnl_dollars[pnl_dollars > 0.0].sum()
    gross_loss = -pnl_dollars[pnl_dollars < 0.0].sum()
    profit_factor = float(gross_profit / gross_loss) if gross_loss > 0.0 else float("inf")

    return pd.DataFrame(
        [
            {
                "trade_count": int(len(filled_df)),
                "win_rate_pct": float((pnl_dollars > 0.0).mean() * 100.0),
                "profit_factor": profit_factor,
                "avg_pnl_dollars": float(np.mean(pnl_dollars)),
                "median_pnl_dollars": float(np.median(pnl_dollars)),
                "avg_pnl_pips": float(np.mean(pnl_pips)),
                "median_pnl_pips": float(np.median(pnl_pips)),
                "avg_hold_minutes": float(np.mean(hold_minutes)),
                "median_hold_minutes": float(np.median(hold_minutes)),
                "p90_hold_minutes": float(np.quantile(hold_minutes, 0.90)),
                "worst_trade_dollars": float(np.min(pnl_dollars)),
                "worst_trade_pips": float(np.min(pnl_pips)),
                "best_trade_dollars": float(np.max(pnl_dollars)),
                "best_trade_pips": float(np.max(pnl_pips)),
            }
        ]
    )


def build_group_table(filled_df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if filled_df.empty:
        return pd.DataFrame(
            columns=[group_col, "trades", "win_rate_pct", "total_pnl_dollars", "avg_pnl_dollars", "avg_pnl_pips"]
        )
    table = (
        filled_df.groupby(group_col)
        .agg(
            trades=("pnl_dollars", "size"),
            win_rate_pct=("pnl_dollars", lambda s: (s > 0.0).mean() * 100.0),
            total_pnl_dollars=("pnl_dollars", "sum"),
            avg_pnl_dollars=("pnl_dollars", "mean"),
            avg_pnl_pips=("pnl_pips", "mean"),
        )
        .reset_index()
        .sort_values("total_pnl_dollars", ascending=False)
    )
    total_abs = float(table["total_pnl_dollars"].abs().sum())
    table["pnl_share_pct"] = table["total_pnl_dollars"].abs() / total_abs * 100.0 if total_abs > 0.0 else 0.0
    return table


def build_max_drawdown_point(filled_df: pd.DataFrame, start_balance: float) -> pd.DataFrame:
    if filled_df.empty:
        return pd.DataFrame([{"timestamp": "", "equity": start_balance, "peak": start_balance, "drawdown_pct": 0.0}])

    ledger = filled_df.sort_values(["exit_time", "entry_time", "module"]).reset_index(drop=True).copy()
    equity = np.concatenate(([start_balance], ledger["balance_after_exit"].to_numpy(dtype=float)))
    peak = np.maximum.accumulate(equity)
    drawdown = equity / peak - 1.0
    min_idx = int(np.argmin(drawdown))
    if min_idx == 0:
        timestamp = pd.NaT
        equity_value = start_balance
        peak_value = start_balance
    else:
        timestamp = ledger.iloc[min_idx - 1]["exit_time"]
        equity_value = float(ledger.iloc[min_idx - 1]["balance_after_exit"])
        peak_value = float(peak[min_idx])
    return pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp(timestamp).isoformat() if pd.notna(timestamp) else "",
                "equity": equity_value,
                "peak": peak_value,
                "drawdown_pct": float(drawdown[min_idx] * 100.0),
            }
        ]
    )


def _simulate_specs_parallel(
    specs: list[ContinuationSpec],
    datasets: dict[str, dict[str, object]],
    workers: int,
) -> pd.DataFrame:
    def run_spec(spec: ContinuationSpec) -> pd.DataFrame:
        return simulate_module(spec, datasets[spec.instrument])

    if workers <= 1 or len(specs) <= 1:
        frames = [run_spec(spec) for spec in specs]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            frames = list(executor.map(run_spec, specs))

    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def build_locked_portfolio_artifacts(
    config: LockedPortfolioConfig,
    workers: int | None = None,
) -> dict[str, Any]:
    specs = specs_from_rows(config.modules)
    instrument_universe = required_instruments(specs)
    datasets = load_datasets(config.data_dir, instrument_universe)
    worker_count = max(1, min(workers or min(len(specs), os.cpu_count() or 1), len(specs) or 1))
    trade_table = _simulate_specs_parallel(specs, datasets, worker_count)
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
        "start_balance": config.start_balance,
        "risk_pct": config.risk_pct,
        "max_leverage": config.max_leverage,
        "max_concurrent": config.max_concurrent,
        "module_count": len(specs),
        "workers": worker_count,
        "instrument_universe": instrument_universe,
        "trade_count_raw": int(len(trade_table)),
        "trade_count_filled": int(len(filled_df)),
    }

    return {
        "config": config,
        "run_config": run_config,
        "specs": specs,
        "selected_modules": normalize_module_rows([asdict(spec) for spec in specs]),
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


def render_locked_portfolio_report(artifacts: dict[str, Any]) -> str:
    config: LockedPortfolioConfig = artifacts["config"]
    summary_row = artifacts["summary"].iloc[0]
    lines = [
        f"# {config.name}",
        "",
        config.description or "Locked portfolio run.",
        "",
        "## Summary",
        "",
        f"- ROI: `{summary_row['roi_pct']:.4f}%`",
        f"- Ending balance: `${summary_row['ending_balance']:.2f}`",
        f"- Max drawdown: `{summary_row['max_drawdown_pct']:.4f}%`",
        f"- Win rate: `{summary_row['portfolio_win_rate_pct']:.4f}%`",
        f"- Profit factor: `{summary_row['portfolio_profit_factor']:.4f}`",
        f"- Annualized weekly Sharpe: `{summary_row['calendar_weekly_sharpe_ann']:.4f}`",
        f"- Annualized weekly Sortino: `{summary_row['calendar_weekly_sortino_ann']:.4f}`",
        f"- Trades: `{int(summary_row['portfolio_trades'])}`",
        "",
        "## Run Config",
        "",
        f"- Config: `{config.config_path}`",
        f"- Data dir: `{config.data_dir}`",
        f"- Start balance: `${config.start_balance:.2f}`",
        f"- Risk per trade: `{config.risk_pct:.2f}%`",
        f"- Max leverage: `{config.max_leverage:.2f}`",
        f"- Max concurrent: `{config.max_concurrent}`",
        f"- Worker threads: `{artifacts['run_config']['workers']}`",
        "",
        "## Artifacts",
        "",
        "- `summary.csv`",
        "- `module_table.csv`",
        "- `weekly_table.csv`",
        "- `monthly_table.csv`",
        "- `yearly_table.csv`",
        "- `trade_stats.csv`",
        "- `instrument_table.csv`",
        "- `side_table.csv`",
        "- `max_drawdown_point.csv`",
        "- `trade_ledger.csv`",
        "- `trade_ledger.parquet`",
        "",
        "## Selected Modules",
        "",
    ]
    lines.extend([f"- `{spec.name}`" for spec in artifacts["specs"]])
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This is the clean locked-portfolio runtime, not the broad candidate search.")
    lines.append("- It uses the corrected cash accounting and NAV-based period returns from the continuation research engine.")
    return "\n".join(lines) + "\n"


def write_locked_portfolio_artifacts(artifacts: dict[str, Any], output_dir: Path | None = None) -> Path:
    config: LockedPortfolioConfig = artifacts["config"]
    target_dir = output_dir.expanduser().resolve() if output_dir is not None else config.output_dir
    target_dir.mkdir(parents=True, exist_ok=True)

    (target_dir / "run_config.json").write_text(json.dumps(artifacts["run_config"], indent=2), encoding="utf-8")
    (target_dir / "selected_modules.json").write_text(
        json.dumps(artifacts["selected_modules"], indent=2),
        encoding="utf-8",
    )
    artifacts["summary"].to_csv(target_dir / "summary.csv", index=False)
    artifacts["module_table"].to_csv(target_dir / "module_table.csv", index=False)
    artifacts["weekly_table"].to_csv(target_dir / "weekly_table.csv", index=False)
    artifacts["monthly_table"].to_csv(target_dir / "monthly_table.csv", index=False)
    artifacts["yearly_table"].to_csv(target_dir / "yearly_table.csv", index=False)
    artifacts["trade_stats"].to_csv(target_dir / "trade_stats.csv", index=False)
    artifacts["instrument_table"].to_csv(target_dir / "instrument_table.csv", index=False)
    artifacts["side_table"].to_csv(target_dir / "side_table.csv", index=False)
    artifacts["max_drawdown_point"].to_csv(target_dir / "max_drawdown_point.csv", index=False)
    artifacts["trade_ledger"].to_csv(target_dir / "trade_ledger.csv", index=False)
    artifacts["trade_ledger"].to_parquet(target_dir / "trade_ledger.parquet", index=False)
    (target_dir / "portfolio_report.md").write_text(render_locked_portfolio_report(artifacts), encoding="utf-8")
    return target_dir


def run_locked_portfolio(
    config_path: Path,
    data_dir_override: Path | None = None,
    output_dir_override: Path | None = None,
    start_balance: float | None = None,
    risk_pct: float | None = None,
    max_leverage: float | None = None,
    max_concurrent: int | None = None,
    workers: int | None = None,
) -> Path:
    config = load_locked_config(
        config_path,
        data_dir_override=data_dir_override,
        output_dir_override=output_dir_override,
        start_balance=start_balance,
        risk_pct=risk_pct,
        max_leverage=max_leverage,
        max_concurrent=max_concurrent,
    )
    artifacts = build_locked_portfolio_artifacts(config, workers=workers)
    return write_locked_portfolio_artifacts(artifacts)


def export_modules_json_pack(
    data_dir: Path,
    modules_json: Path,
    output_dir: Path,
    start_balance: float,
    risk_pct: float,
    max_leverage: float,
    max_concurrent: int,
    workers: int | None = None,
) -> Path:
    modules_json = modules_json.expanduser().resolve()
    raw_modules = json.loads(modules_json.read_text(encoding="utf-8"))
    config = LockedPortfolioConfig(
        name=modules_json.stem,
        description="Exported selected-module pack.",
        config_path=modules_json,
        data_dir=data_dir.expanduser().resolve(),
        output_dir=output_dir.expanduser().resolve(),
        start_balance=float(start_balance),
        risk_pct=float(risk_pct),
        max_leverage=float(max_leverage),
        max_concurrent=int(max_concurrent),
        modules=normalize_module_rows(list(raw_modules)),
    )
    artifacts = build_locked_portfolio_artifacts(config, workers=workers)
    return write_locked_portfolio_artifacts(artifacts)
