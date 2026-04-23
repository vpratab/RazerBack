from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from continuation_core import resolve_execution_profile, simulate_ladder_exit, simulate_portfolio
from locked_portfolio_runtime import build_trade_stats
from realistic_backtest import compute_slippage_pips, load_dataset_bundle
from scripts.run_multifamily_fx_research import BreakoutSpec, simulate_breakout_module
from scripts.run_portfolio_factory_research import breakout_spec_from_row


DEFAULT_DATA_DIR = Path("C:/fx_data/m1")
DEFAULT_MODULES_CSV = Path(
    "C:/Users/saanvi/Documents/Codex/2026-04-20-what-i-need-from-the-new/"
    "wfo_2025_test/train_search_ban3_exact_eval/portfolio_06_exact/selected_candidates.csv"
)
DEFAULT_OUTPUT_DIR = Path(
    "C:/Users/saanvi/Documents/Codex/2026-04-20-what-i-need-from-the-new/"
    "breakout_ladder_exit_research_20260422"
)
DEFAULT_MODULES = [
    "gbpusd_short_bo_h7_rr1.4_dist5_wickmax15_retle-10_z2_sl25_tp38_shield25-5_ttl60",
    "eurjpy_short_bo_h5_rr1.15_dist1_wickmax15_retna_zna_sl40_tp80_shield40-0_ttl60",
    "eurjpy_short_bo_h5_rr1.25_dist2.5_wickmaxna_retle-2.5_z2_sl25_tp50_shield38-0_ttl120",
    "usdjpy_long_bo_h8_rr1.25_dist5_wickmaxna_retna_z1_sl30_tp30_shield20-4_ttl120",
]


@dataclass(frozen=True)
class LadderExitConfig:
    label: str
    ladder_pips: tuple[int, int, int]
    ladder_fractions: tuple[float, float, float]
    trail_stop_pips: int

    @property
    def runner_fraction(self) -> float:
        return max(0.0, 1.0 - sum(self.ladder_fractions))


@dataclass(frozen=True)
class BreakoutLadderShim:
    name: str
    instrument: str
    side: str
    stop_loss_pips: int
    ladder_pips: tuple[int, int, int]
    ladder_fractions: tuple[float, float, float]
    trail_stop_pips: int
    ttl_bars: int


LADDER_CONFIGS = [
    LadderExitConfig("baseline_fixed", (0, 0, 0), (0.0, 0.0, 0.0), 0),
    LadderExitConfig("l153050_f403020_r10_t10", (15, 30, 50), (0.4, 0.3, 0.2), 10),
    LadderExitConfig("l153050_f403020_r10_t15", (15, 30, 50), (0.4, 0.3, 0.2), 15),
    LadderExitConfig("l153050_f303020_r20_t15", (15, 30, 50), (0.3, 0.3, 0.2), 15),
    LadderExitConfig("l153050_f503020_r0_t15", (15, 30, 50), (0.5, 0.3, 0.2), 15),
    LadderExitConfig("l102545_f403020_r10_t15", (10, 25, 45), (0.4, 0.3, 0.2), 15),
    LadderExitConfig("l204060_f403020_r10_t15", (20, 40, 60), (0.4, 0.3, 0.2), 15),
    LadderExitConfig("l153050_f403020_r10_t20", (15, 30, 50), (0.4, 0.3, 0.2), 20),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Test continuation-style ladder exits on breakout modules without changing entry logic, "
            "using 2022-2024 training and untouched 2025 evaluation."
        )
    )
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--modules-csv", default=str(DEFAULT_MODULES_CSV))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--module-names", nargs="+", default=DEFAULT_MODULES)
    parser.add_argument("--train-start", default="2022-01-01")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--test-end", default="2025-12-31")
    parser.add_argument("--start-balance", type=float, default=1500.0)
    parser.add_argument("--risk-pct", type=float, default=2.9)
    parser.add_argument("--max-leverage", type=float, default=35.0)
    parser.add_argument("--max-concurrent", type=int, default=2)
    return parser.parse_args()


def filter_dataset_window(data: dict[str, object], start: pd.Timestamp, end: pd.Timestamp) -> dict[str, object]:
    frame = data["df"]
    timestamp = pd.to_datetime(frame["timestamp"], utc=True)
    keep = ((timestamp >= start) & (timestamp < end)).to_numpy(dtype=bool)

    filtered: dict[str, object] = {}
    for key, value in data.items():
        if key in {"pip", "inferred_bid_ask"}:
            filtered[key] = value
        elif key in {"bid", "ask", "mid"}:
            filtered[key] = {subkey: subvalue[keep] for subkey, subvalue in value.items()}
        elif key == "df":
            filtered[key] = value.loc[keep].reset_index(drop=True)
        elif isinstance(value, np.ndarray):
            filtered[key] = value[keep]
        else:
            filtered[key] = value
    return filtered


def breakout_entry_mask(spec: BreakoutSpec, data: dict[str, object]) -> np.ndarray:
    mid = data["mid"]
    hour = data["hour"]
    weekday = data["weekday"]
    pip = float(data["pip"])

    if spec.side == "long":
        base = (
            (weekday < 5)
            & (hour == spec.hour_utc)
            & np.isfinite(data["prior_high_60"])
            & (mid["high"] > data["prior_high_60"])
            & (mid["close"] > data["prior_high_60"])
            & (mid["close"] >= data["trend_240"])
            & (data["range_60"] >= data["range_240"] * spec.range_ratio_min)
        )
        dist = (mid["close"] - data["prior_high_60"]) / pip
        wick_against = data["upper_wick"]
    else:
        base = (
            (weekday < 5)
            & (hour == spec.hour_utc)
            & np.isfinite(data["prior_low_60"])
            & (mid["low"] < data["prior_low_60"])
            & (mid["close"] < data["prior_low_60"])
            & (mid["close"] <= data["trend_240"])
            & (data["range_60"] >= data["range_240"] * spec.range_ratio_min)
        )
        dist = (data["prior_low_60"] - mid["close"]) / pip
        wick_against = data["lower_wick"]

    mask = base & (dist >= spec.close_dist_min)
    if spec.breakout_wick_max is not None:
        mask &= wick_against <= spec.breakout_wick_max
    if spec.ret15_min is not None:
        mask &= data["ret15"] >= spec.ret15_min
    if spec.ret15_max is not None:
        mask &= data["ret15"] <= spec.ret15_max
    if spec.z_min is not None:
        mask &= np.abs(data["z60"]) >= spec.z_min
    return mask


def simulate_breakout_module_with_ladder(
    spec: BreakoutSpec,
    data: dict[str, object],
    module_name: str,
    exit_config: LadderExitConfig,
) -> pd.DataFrame:
    if exit_config.label == "baseline_fixed":
        return simulate_breakout_module(spec, data, module_name)

    mid = data["mid"]
    pip = float(data["pip"])
    entries = np.flatnonzero(breakout_entry_mask(spec, data))
    trades: list[dict[str, object]] = []
    next_allowed = 0
    base_profile = resolve_execution_profile("base")

    shim = BreakoutLadderShim(
        name=module_name,
        instrument=spec.instrument,
        side=spec.side,
        stop_loss_pips=int(spec.stop_pips),
        ladder_pips=exit_config.ladder_pips,
        ladder_fractions=exit_config.ladder_fractions,
        trail_stop_pips=int(exit_config.trail_stop_pips),
        ttl_bars=int(spec.ttl_bars),
    )

    for idx in entries:
        entry_idx = idx + 1
        if entry_idx >= len(mid["open"]) - 1 or entry_idx < next_allowed:
            continue

        slippage_pips = compute_slippage_pips(
            float(data["range_60"][entry_idx]),
            float(data["range_240"][entry_idx]),
        )
        slippage = slippage_pips * pip

        if spec.side == "long":
            entry_price = float(data["ask"]["open"][entry_idx] + slippage)
            stop_level = entry_price - spec.stop_pips * pip
        else:
            entry_price = float(data["bid"]["open"][entry_idx] - slippage)
            stop_level = entry_price + spec.stop_pips * pip

        trade = simulate_ladder_exit(
            shim,
            data,
            entry_idx,
            entry_price,
            stop_level,
            execution_profile=base_profile,
        )
        if trade is None:
            continue
        trade["module"] = module_name
        trade["entry_slippage_pips"] = slippage_pips
        trade["used_inferred_bid_ask"] = bool(data["inferred_bid_ask"])
        trade["partial_fills"] = json.dumps(trade["partial_fills"])
        trades.append(trade)
        next_allowed = int(trade["exit_idx"]) + 1

    if not trades:
        return pd.DataFrame()
    frame = pd.DataFrame(trades)
    return frame.drop(columns=["exit_idx"])


def evaluate_trade_frame(
    trade_frame: pd.DataFrame,
    datasets: dict[str, dict[str, object]],
    args: argparse.Namespace,
) -> dict[str, Any]:
    summary, module_table, weekly, monthly, yearly, filled_df = simulate_portfolio(
        trade_frame,
        datasets,
        args.start_balance,
        args.risk_pct / 100.0,
        args.max_leverage,
        args.max_concurrent,
    )
    trade_stats = build_trade_stats(filled_df)
    summary_row = summary.iloc[0]
    trade_row = trade_stats.iloc[0]
    sharpe = float(summary_row["calendar_weekly_sharpe_ann"])
    win_rate = float(summary_row["portfolio_win_rate_pct"])
    return {
        "roi_pct": float(summary_row["roi_pct"]),
        "sharpe_ann": sharpe,
        "sortino_ann": float(summary_row["calendar_weekly_sortino_ann"]),
        "max_drawdown_pct": float(summary_row["max_drawdown_pct"]),
        "win_rate_pct": win_rate,
        "profit_factor": float(summary_row["portfolio_profit_factor"]),
        "trade_count": int(summary_row["portfolio_trades"]),
        "positive_calendar_months_pct": float(summary_row["positive_calendar_months_pct"]),
        "avg_trade_dollars": float(trade_row["avg_pnl_dollars"]),
        "avg_trade_pips": float(trade_row["avg_pnl_pips"]),
        "objective_sharpe_x_win": float(sharpe * (win_rate / 100.0)),
        "summary": summary,
        "module_table": module_table,
        "weekly_table": weekly,
        "monthly_table": monthly,
        "yearly_table": yearly,
        "trade_stats": trade_stats,
        "trade_ledger": filled_df,
    }


def pack_path(base: Path, name: str) -> Path:
    target = base / name
    target.mkdir(parents=True, exist_ok=True)
    return target


def write_eval_pack(target_dir: Path, eval_payload: dict[str, Any]) -> None:
    eval_payload["summary"].to_csv(target_dir / "summary.csv", index=False)
    eval_payload["module_table"].to_csv(target_dir / "module_table.csv", index=False)
    eval_payload["weekly_table"].to_csv(target_dir / "weekly_table.csv", index=False)
    eval_payload["monthly_table"].to_csv(target_dir / "monthly_table.csv", index=False)
    eval_payload["yearly_table"].to_csv(target_dir / "yearly_table.csv", index=False)
    eval_payload["trade_stats"].to_csv(target_dir / "trade_stats.csv", index=False)
    eval_payload["trade_ledger"].to_csv(target_dir / "trade_ledger.csv", index=False)


def load_breakout_rows(modules_csv: Path, module_names: list[str]) -> list[dict[str, Any]]:
    frame = pd.read_csv(modules_csv)
    if frame.empty:
        raise SystemExit(f"No rows in {modules_csv}")
    by_module = {str(row["module"]): row for row in frame.to_dict(orient="records")}
    missing = [name for name in module_names if name not in by_module]
    if missing:
        raise SystemExit(f"Missing requested breakout modules in {modules_csv}: {', '.join(missing)}")
    rows = []
    for name in module_names:
        row = dict(by_module[name])
        family = str(row.get("family", "breakout")).strip().lower()
        if family != "breakout":
            raise SystemExit(f"Requested module is not breakout family: {name}")
        rows.append(row)
    return rows


def build_window_datasets(
    datasets: dict[str, dict[str, object]],
    start_date: str,
    end_date: str,
) -> dict[str, dict[str, object]]:
    start = pd.Timestamp(start_date, tz="UTC")
    end = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)
    return {
        instrument: filter_dataset_window(data, start, end)
        for instrument, data in datasets.items()
    }


def key_metrics_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    ordered = []
    for row in rows:
        ordered.append(
            {
                "subject": row["subject"],
                "config": row["config"],
                "window": row["window"],
                "roi_pct": row["roi_pct"],
                "sharpe_ann": row["sharpe_ann"],
                "max_drawdown_pct": row["max_drawdown_pct"],
                "win_rate_pct": row["win_rate_pct"],
                "profit_factor": row["profit_factor"],
                "trade_count": row["trade_count"],
                "objective_sharpe_x_win": row["objective_sharpe_x_win"],
            }
        )
    return pd.DataFrame(ordered)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    selected_rows = load_breakout_rows(Path(args.modules_csv).expanduser().resolve(), args.module_names)
    selected_specs = {str(row["module"]): breakout_spec_from_row(row) for row in selected_rows}

    all_instruments = sorted({spec.instrument for spec in selected_specs.values()} | {"usdjpy", "gbpusd"})
    datasets = load_dataset_bundle(Path(args.data_dir).expanduser().resolve(), all_instruments)
    train_datasets = build_window_datasets(datasets, args.train_start, args.train_end)
    test_datasets = build_window_datasets(datasets, args.test_start, args.test_end)

    module_train_rows: list[dict[str, Any]] = []
    module_test_rows: list[dict[str, Any]] = []
    best_configs: dict[str, LadderExitConfig] = {}

    for module_name, spec in selected_specs.items():
        train_results: list[dict[str, Any]] = []
        for config in LADDER_CONFIGS:
            trade_frame = simulate_breakout_module_with_ladder(spec, train_datasets[spec.instrument], module_name, config)
            dataset_subset = {spec.instrument: train_datasets[spec.instrument], "usdjpy": train_datasets["usdjpy"], "gbpusd": train_datasets["gbpusd"]}
            eval_payload = evaluate_trade_frame(trade_frame, dataset_subset, args)
            result_row = {
                "subject": module_name,
                "config": config.label,
                "window": "train_2022_2024",
                **{k: v for k, v in eval_payload.items() if not isinstance(v, pd.DataFrame)},
            }
            train_results.append(result_row)
            module_train_rows.append(result_row)

        train_frame = pd.DataFrame(train_results).sort_values(
            ["objective_sharpe_x_win", "sharpe_ann", "roi_pct"],
            ascending=False,
        ).reset_index(drop=True)
        best_label = str(train_frame.iloc[0]["config"])
        best_configs[module_name] = next(config for config in LADDER_CONFIGS if config.label == best_label)
        train_frame.to_csv(output_dir / f"{module_name}__train_sweep.csv", index=False)

        baseline_eval = evaluate_trade_frame(
            simulate_breakout_module_with_ladder(spec, test_datasets[spec.instrument], module_name, LADDER_CONFIGS[0]),
            {spec.instrument: test_datasets[spec.instrument], "usdjpy": test_datasets["usdjpy"], "gbpusd": test_datasets["gbpusd"]},
            args,
        )
        best_eval = evaluate_trade_frame(
            simulate_breakout_module_with_ladder(spec, test_datasets[spec.instrument], module_name, best_configs[module_name]),
            {spec.instrument: test_datasets[spec.instrument], "usdjpy": test_datasets["usdjpy"], "gbpusd": test_datasets["gbpusd"]},
            args,
        )

        test_payloads: list[tuple[str, dict[str, Any]]] = [("baseline_fixed", baseline_eval)]
        if best_configs[module_name].label != "baseline_fixed":
            test_payloads.append((best_configs[module_name].label, best_eval))

        for label, payload in test_payloads:
            row = {
                "subject": module_name,
                "config": label,
                "window": "test_2025",
                **{k: v for k, v in payload.items() if not isinstance(v, pd.DataFrame)},
            }
            module_test_rows.append(row)
        write_eval_pack(pack_path(output_dir, f"{module_name}__test_baseline"), baseline_eval)
        write_eval_pack(pack_path(output_dir, f"{module_name}__test_best"), best_eval)

    module_train_frame = key_metrics_frame(module_train_rows)
    module_test_frame = key_metrics_frame(module_test_rows)
    module_train_frame.to_csv(output_dir / "module_train_summary.csv", index=False)
    module_test_frame.to_csv(output_dir / "module_test_summary.csv", index=False)

    baseline_portfolio_trades: list[pd.DataFrame] = []
    ladder_portfolio_trades: list[pd.DataFrame] = []
    for module_name, spec in selected_specs.items():
        baseline_portfolio_trades.append(
            simulate_breakout_module_with_ladder(spec, test_datasets[spec.instrument], module_name, LADDER_CONFIGS[0])
        )
        ladder_portfolio_trades.append(
            simulate_breakout_module_with_ladder(spec, test_datasets[spec.instrument], module_name, best_configs[module_name])
        )

    baseline_portfolio_eval = evaluate_trade_frame(
        pd.concat(baseline_portfolio_trades, ignore_index=True).sort_values(["entry_time", "module"]).reset_index(drop=True),
        {instrument: test_datasets[instrument] for instrument in all_instruments},
        args,
    )
    ladder_portfolio_eval = evaluate_trade_frame(
        pd.concat(ladder_portfolio_trades, ignore_index=True).sort_values(["entry_time", "module"]).reset_index(drop=True),
        {instrument: test_datasets[instrument] for instrument in all_instruments},
        args,
    )
    write_eval_pack(pack_path(output_dir, "portfolio_test_baseline"), baseline_portfolio_eval)
    write_eval_pack(pack_path(output_dir, "portfolio_test_laddered"), ladder_portfolio_eval)

    portfolio_compare = pd.DataFrame(
        [
            {
                "config": "baseline_fixed",
                **{k: v for k, v in baseline_portfolio_eval.items() if not isinstance(v, pd.DataFrame)},
            },
            {
                "config": "laddered_best_by_module",
                **{k: v for k, v in ladder_portfolio_eval.items() if not isinstance(v, pd.DataFrame)},
            },
        ]
    )
    portfolio_compare.to_csv(output_dir / "portfolio_test_comparison.csv", index=False)

    best_config_rows = [
        {
            "module": module_name,
            "best_train_config": config.label,
            "ladder_pips": json.dumps(list(config.ladder_pips)),
            "ladder_fractions": json.dumps(list(config.ladder_fractions)),
            "trail_stop_pips": config.trail_stop_pips,
            "runner_fraction": config.runner_fraction,
        }
        for module_name, config in best_configs.items()
    ]
    best_config_frame = pd.DataFrame(best_config_rows)
    best_config_frame.to_csv(output_dir / "best_configs.csv", index=False)

    report_lines = [
        "# Breakout Ladder Exit Research",
        "",
        "## Setup",
        "",
        f"- Modules csv: `{Path(args.modules_csv).resolve()}`",
        f"- Training window: `{args.train_start}` to `{args.train_end}`",
        f"- Test window: `{args.test_start}` to `{args.test_end}`",
        f"- Selected by train objective: `annualized Sharpe * (win_rate_pct / 100)`",
        "",
        "## Best Config Per Module",
        "",
        best_config_frame.to_string(index=False),
        "",
        "## 2025 Module Comparison",
        "",
        module_test_frame.sort_values(["subject", "window", "config"]).to_string(index=False),
        "",
        "## 2025 Portfolio Comparison",
        "",
        portfolio_compare[
            [
                "config",
                "roi_pct",
                "sharpe_ann",
                "max_drawdown_pct",
                "win_rate_pct",
                "profit_factor",
                "trade_count",
                "objective_sharpe_x_win",
            ]
        ].to_string(index=False),
        "",
    ]

    baseline_row = portfolio_compare.loc[portfolio_compare["config"] == "baseline_fixed"].iloc[0]
    ladder_row = portfolio_compare.loc[portfolio_compare["config"] == "laddered_best_by_module"].iloc[0]
    report_lines.extend(
        [
            "## Delta",
            "",
            f"- ROI delta: `{float(ladder_row['roi_pct']) - float(baseline_row['roi_pct']):.4f}` pct",
            f"- Sharpe delta: `{float(ladder_row['sharpe_ann']) - float(baseline_row['sharpe_ann']):.4f}`",
            f"- Max DD delta: `{float(ladder_row['max_drawdown_pct']) - float(baseline_row['max_drawdown_pct']):.4f}` pct",
            f"- Win-rate delta: `{float(ladder_row['win_rate_pct']) - float(baseline_row['win_rate_pct']):.4f}` pct",
            f"- Profit-factor delta: `{float(ladder_row['profit_factor']) - float(baseline_row['profit_factor']):.4f}`",
            f"- Trade-count delta: `{int(ladder_row['trade_count']) - int(baseline_row['trade_count'])}`",
        ]
    )
    (output_dir / "report.md").write_text("\n".join(report_lines), encoding="utf-8")
    print(output_dir)


if __name__ == "__main__":
    main()
