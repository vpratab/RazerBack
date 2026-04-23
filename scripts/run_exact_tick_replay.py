from __future__ import annotations

import argparse
import json
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

from locked_portfolio_runtime import build_trade_stats
from realistic_backtest import load_dataset_bundle, simulate_reversal_module
from scripts.research_utils import apply_overlay, filter_dataset, load_modules_from_rankings, spec_from_name
from scripts.run_multifamily_fx_research import BreakoutSpec, simulate_breakout_module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay a selected module set against raw Dukascopy ticks.")
    parser.add_argument("--data-dir", default=r"C:\fx_data\m1")
    parser.add_argument("--tick-root", default=r"C:\fx_data\tick")
    parser.add_argument("--variant-rankings")
    parser.add_argument("--label")
    parser.add_argument("--module", action="append", default=[])
    parser.add_argument("--year", type=int, default=2025)
    parser.add_argument("--overlay", default="annual6_month4")
    parser.add_argument("--annual-stop", type=float, default=-6.0)
    parser.add_argument("--monthly-stop", type=float, default=-4.0)
    parser.add_argument("--output-dir", default="output/research/exact_tick_replay")
    return parser.parse_args()


def resolve_modules(args: argparse.Namespace) -> list[str]:
    modules = list(args.module)
    if args.variant_rankings and args.label:
        modules.extend(load_modules_from_rankings(Path(args.variant_rankings), args.label))
    modules = list(dict.fromkeys(modules))
    if not modules:
        raise SystemExit("Provide --module entries or --variant-rankings with --label.")
    return modules


def date_range(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]:
    current = start.normalize()
    last = end.normalize()
    dates = []
    while current <= last:
        dates.append(current)
        current += pd.Timedelta(days=1)
    return dates


@lru_cache(maxsize=1024)
def load_tick_day(tick_root: str, instrument: str, day: str) -> pd.DataFrame:
    ts = pd.Timestamp(day)
    path = Path(tick_root) / instrument.upper() / str(ts.year) / f"{ts.date().isoformat()}.parquet"
    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_parquet(path, columns=["timestamp", "bid", "ask"])
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    return frame.sort_values("timestamp").reset_index(drop=True)


def load_ticks(tick_root: str, instrument: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    frames = [load_tick_day(tick_root, instrument, day.date().isoformat()) for day in date_range(start, end)]
    frames = [frame for frame in frames if not frame.empty]
    if not frames:
        return pd.DataFrame()
    ticks = pd.concat(frames, ignore_index=True)
    mask = (ticks["timestamp"] >= start) & (ticks["timestamp"] <= end)
    return ticks.loc[mask].reset_index(drop=True)


def tick_replay_trade(row: pd.Series, tick_root: str) -> dict[str, object]:
    spec = spec_from_name(str(row["module"]))
    instrument = spec.instrument
    side = spec.side
    pip = float(row["pip_size"])
    entry_time = pd.Timestamp(row["entry_time"]).tz_convert("UTC")
    ttl_end = entry_time + pd.Timedelta(minutes=int(spec.ttl_bars))
    entry_price = float(row["entry_price"])

    if side == "long":
        stop_loss = entry_price - spec.stop_pips * pip
        take_profit = entry_price + spec.target_pips * pip
        shield_trigger = entry_price + spec.shield_trigger_pips * pip
        shield_lock = entry_price + spec.shield_lock_pips * pip
    else:
        stop_loss = entry_price + spec.stop_pips * pip
        take_profit = entry_price - spec.target_pips * pip
        shield_trigger = entry_price - spec.shield_trigger_pips * pip
        shield_lock = entry_price - spec.shield_lock_pips * pip

    ticks = load_ticks(tick_root, instrument, entry_time, ttl_end)
    if ticks.empty:
        out = row.to_dict()
        out.update(
            {
                "exact_status": "missing_ticks",
                "exact_exit_time": pd.NaT,
                "exact_exit_price": np.nan,
                "exact_pnl_pips": np.nan,
                "exact_exit_reason": "missing_ticks",
                "exact_shielded": False,
            }
        )
        return out

    shielded = False
    exit_time = ticks.iloc[-1]["timestamp"]
    exit_price = float(ticks.iloc[-1]["bid"] if side == "long" else ticks.iloc[-1]["ask"])
    exit_reason = "ttl"

    for tick in ticks.itertuples(index=False):
        bid = float(tick.bid)
        ask = float(tick.ask)
        ts = pd.Timestamp(tick.timestamp)

        if side == "long":
            if not shielded and ask >= shield_trigger:
                shielded = True
                stop_loss = shield_lock
            if bid <= stop_loss:
                exit_time = ts
                exit_price = float(stop_loss)
                exit_reason = "stop"
                break
            if bid >= take_profit:
                exit_time = ts
                exit_price = float(take_profit)
                exit_reason = "target"
                break
        else:
            if not shielded and bid <= shield_trigger:
                shielded = True
                stop_loss = shield_lock
            if ask >= stop_loss:
                exit_time = ts
                exit_price = float(stop_loss)
                exit_reason = "stop"
                break
            if ask <= take_profit:
                exit_time = ts
                exit_price = float(take_profit)
                exit_reason = "target"
                break

    exact_pnl = (exit_price - entry_price) / pip if side == "long" else (entry_price - exit_price) / pip
    out = row.to_dict()
    out.update(
        {
            "exact_status": "ok",
            "exact_exit_time": exit_time,
            "exact_exit_price": exit_price,
            "exact_pnl_pips": float(exact_pnl),
            "exact_exit_reason": exit_reason,
            "exact_shielded": bool(shielded),
            "m1_pnl_pips": float(row["pnl_pips"]),
            "m1_exit_time": row["exit_time"],
            "m1_exit_price": row["exit_price"],
            "pnl_pips_delta": float(exact_pnl - float(row["pnl_pips"])),
        }
    )
    out["exit_time"] = exit_time
    out["exit_price"] = exit_price
    out["pnl_pips"] = float(exact_pnl)
    out["shielded"] = bool(shielded)
    return out


def build_raw_trades(modules: list[str], data_dir: Path, year: int) -> tuple[pd.DataFrame, dict[str, dict[str, object]]]:
    instruments = sorted({spec_from_name(module).instrument for module in modules} | {"gbpusd", "usdjpy"})
    datasets = load_dataset_bundle(data_dir, instruments)
    year_datasets = {instrument: filter_dataset(data, year) for instrument, data in datasets.items()}
    frames = []
    for module in modules:
        spec = spec_from_name(module)
        data = year_datasets[spec.instrument]
        if len(data["timestamp"]) < 300:
            continue
        if isinstance(spec, BreakoutSpec):
            frame = simulate_breakout_module(spec, data, module)
        else:
            frame = simulate_reversal_module(spec, data, module)
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame(), year_datasets
    raw = pd.concat(frames, ignore_index=True).sort_values(["entry_time", "module"]).reset_index(drop=True)
    return raw, year_datasets


def write_report(
    output_dir: Path,
    modules: list[str],
    label: str,
    overlay: str,
    m1_summary: pd.DataFrame,
    exact_summary: pd.DataFrame,
    replayed_raw: pd.DataFrame,
    exact_filled: pd.DataFrame,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    m1_summary.to_csv(output_dir / "m1_summary.csv", index=False)
    exact_summary.to_csv(output_dir / "exact_tick_summary.csv", index=False)
    replayed_raw.to_csv(output_dir / "exact_tick_raw_trades.csv", index=False)
    exact_filled.to_csv(output_dir / "exact_tick_filled_trades.csv", index=False)

    m1 = m1_summary.iloc[0]
    exact = exact_summary.iloc[0]
    ok_count = int((replayed_raw["exact_status"] == "ok").sum()) if not replayed_raw.empty else 0
    missing_count = int((replayed_raw["exact_status"] != "ok").sum()) if not replayed_raw.empty else 0
    lines = [
        "# Exact Tick Replay",
        "",
        f"- Portfolio label: `{label}`",
        f"- Overlay: `{overlay}`",
        f"- Raw trades replayed: `{len(replayed_raw)}`",
        f"- Replay OK: `{ok_count}`",
        f"- Missing tick windows: `{missing_count}`",
        "",
        "## Modules",
        "",
    ]
    lines.extend([f"- `{module}`" for module in modules])
    lines.extend(
        [
            "",
            "## M1 vs Exact Tick Summary",
            "",
            pd.DataFrame(
                [
                    {
                        "engine": "M1 path",
                        "roi_pct": m1["roi_pct"],
                        "sharpe": m1["calendar_weekly_sharpe_ann"],
                        "max_dd": m1["max_drawdown_pct"],
                        "win_rate": m1["portfolio_win_rate_pct"],
                        "profit_factor": m1["portfolio_profit_factor"],
                        "trades": m1["portfolio_trades"],
                    },
                    {
                        "engine": "exact_tick",
                        "roi_pct": exact["roi_pct"],
                        "sharpe": exact["calendar_weekly_sharpe_ann"],
                        "max_dd": exact["max_drawdown_pct"],
                        "win_rate": exact["portfolio_win_rate_pct"],
                        "profit_factor": exact["portfolio_profit_factor"],
                        "trades": exact["portfolio_trades"],
                    },
                ]
            ).to_markdown(index=False),
            "",
            "## Replay Delta",
            "",
        ]
    )
    if replayed_raw.empty:
        lines.append("No trades were replayed.")
    else:
        delta = replayed_raw.loc[replayed_raw["exact_status"] == "ok", "pnl_pips_delta"]
        lines.extend(
            [
                f"- Mean exact-minus-M1 pips: `{delta.mean():.4f}`",
                f"- Median exact-minus-M1 pips: `{delta.median():.4f}`",
                f"- Worst exact-minus-M1 pips: `{delta.min():.4f}`",
                f"- Best exact-minus-M1 pips: `{delta.max():.4f}`",
            ]
        )
    (output_dir / "exact_tick_replay_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    args = parse_args()
    modules = resolve_modules(args)
    label = args.label or "module_list"
    raw_trades, year_datasets = build_raw_trades(modules, Path(args.data_dir), args.year)
    m1_summary, _, _, _, _, _ = apply_overlay(raw_trades, year_datasets, args.annual_stop, args.monthly_stop)

    exact_rows = [tick_replay_trade(row, args.tick_root) for _, row in raw_trades.iterrows()]
    exact_raw = pd.DataFrame(exact_rows)
    exact_raw_ok = exact_raw.loc[exact_raw["exact_status"] == "ok"].copy()
    exact_summary, _, _, _, _, exact_filled = apply_overlay(exact_raw_ok, year_datasets, args.annual_stop, args.monthly_stop)

    write_report(Path(args.output_dir), modules, label, args.overlay, m1_summary, exact_summary, exact_raw, exact_filled)
    print(Path(args.output_dir))
    print((Path(args.output_dir) / "exact_tick_replay_report.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
