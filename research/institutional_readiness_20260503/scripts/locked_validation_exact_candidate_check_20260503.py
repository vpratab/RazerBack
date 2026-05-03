from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(r"C:\Users\saanvi\Documents\Codex\2026-04-20-what-i-need-from-the-new")
REPO_ROOT = Path(r"C:\Users\saanvi\Documents\GitHub\RazerBack")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import build_rigorous_validation_appendix_20260426 as appendix
import locked_validation_realism_proxy_gauntlet_20260502 as gauntlet
import locked_validation_usdjpy_followup_overlay_20260502 as overlay
import scripts.evaluation_framework as ef


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_status(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_weighted_raw_book() -> pd.DataFrame:
    book = json.loads((ROOT / "locked_validation_slippage_reweight_probe_20260502" / "best_execution_robust_candidate_book.json").read_text(encoding="utf-8"))
    source_rows = {src.alias: overlay.load_row(src) for src in [overlay.EURGBP_A, overlay.EURGBP_B, overlay.USDJPY_BASE, *overlay.USDJPY_FOLLOWUPS]}
    ledgers = overlay.build_validation_ledgers(source_rows)
    parts: list[pd.DataFrame] = []
    for module_info in book["modules"]:
        alias = str(module_info["alias"])
        weight = float(module_info["weight"])
        ledger = ledgers[alias].copy()
        ledger["trade_weight"] = weight
        ledger["alias"] = alias
        ledger["trade_key"] = (
            ledger["module"].astype(str)
            + "|"
            + pd.to_datetime(ledger["entry_time"], utc=True).astype(str)
            + "|"
            + ledger["instrument"].astype(str)
        )
        parts.append(ledger)
    out = pd.concat(parts, ignore_index=True)
    return out.sort_values(["entry_time", "module"]).reset_index(drop=True)


def summarize_weighted_ledger(ledger: pd.DataFrame) -> tuple[dict[str, Any], pd.DataFrame]:
    summary = ef.summarize_module_trade_history(
        ledger,
        timestamp_col="exit_time",
        pnl_col="pnl_dollars",
        starting_capital=1500.0,
    )
    frame = ledger.copy()
    frame["exit_time"] = pd.to_datetime(frame["exit_time"], utc=True)
    frame["year"] = frame["exit_time"].dt.year
    yearly = (
        frame.groupby("year", as_index=False)
        .agg(
            trades=("pnl_dollars", "size"),
            pnl_dollars=("pnl_dollars", "sum"),
            pnl_pips=("pnl_pips", "sum"),
        )
        .sort_values("year")
        .reset_index(drop=True)
    )
    payload = {
        "roi_pct": ((1500.0 + float(frame["pnl_dollars"].sum())) / 1500.0 - 1.0) * 100.0 if not frame.empty else 0.0,
        "sharpe_ann": None if pd.isna(summary["sharpe_ann"]) else float(summary["sharpe_ann"]),
        "max_drawdown_pct": None if pd.isna(summary["max_drawdown"]) else float(summary["max_drawdown"]),
        "profit_factor": None if pd.isna(summary["profit_factor"]) else float(summary["profit_factor"]),
        "trades": int(summary["trade_count"]),
        "win_rate_pct": None if pd.isna(summary["win_rate"]) else float(summary["win_rate"]) * 100.0,
        "total_pnl_dollars": float(frame["pnl_dollars"].sum()),
        "positive_years": int((yearly["pnl_dollars"] > 0.0).sum()) if not yearly.empty else 0,
        "year_count": int(len(yearly)),
    }
    return payload, yearly


def exact_weighted_ledger(raw_subset: pd.DataFrame, entry_delay_seconds: int = 0, extra_spread_pips: float = 0.0) -> pd.DataFrame:
    exact_raw = appendix.build_exact_tick_portfolio(raw_subset, {}, entry_delay_seconds=entry_delay_seconds, extra_spread_pips=extra_spread_pips)
    if exact_raw.empty:
        return exact_raw
    ref = raw_subset.copy()
    ref["dollar_per_pip"] = (
        ref["pnl_dollars"].astype(float).abs()
        / ref["pnl_pips"].astype(float).abs().replace(0.0, pd.NA)
    ).fillna(0.0)
    ref = ref[["trade_key", "dollar_per_pip"]]
    merged = exact_raw.merge(ref, on="trade_key", how="left")
    merged["pnl_dollars"] = merged["pnl_pips"].astype(float) * merged["dollar_per_pip"].astype(float) * merged["trade_weight"].astype(float)
    return merged


def main() -> None:
    output_dir = ROOT / "locked_validation_exact_candidate_check_20260503"
    output_dir.mkdir(parents=True, exist_ok=True)
    status_path = output_dir / "status.json"

    write_status(
        status_path,
        {
            "timestamp": now_iso(),
            "stage": "build_weighted_raw_book",
            "message": "Building weighted Validation raw book for exact candidate check.",
        },
    )
    weighted_raw = build_weighted_raw_book()
    weighted_raw.to_csv(output_dir / "weighted_validation_raw_book.csv", index=False)

    rows: list[dict[str, Any]] = []

    write_status(
        status_path,
        {
            "timestamp": now_iso(),
            "stage": "baseline_m1",
            "message": "Running weighted M1 summary for exact candidate check.",
        },
    )
    m1_metrics, m1_yearly = summarize_weighted_ledger(weighted_raw)
    m1_yearly.to_csv(output_dir / "m1_yearly.csv", index=False)
    rows.append({"scenario": "m1_weighted", **m1_metrics})

    exact_scenarios = [
        ("exact_tick", 0, 0.0),
        ("exact_tick_delay_2s", 2, 0.0),
        ("exact_tick_spread_0.1", 0, 0.1),
        ("exact_tick_spread_0.3", 0, 0.3),
    ]
    for name, delay, spread in exact_scenarios:
        write_status(
            status_path,
            {
                "timestamp": now_iso(),
                "stage": "exact_replay",
                "message": f"Running {name}.",
                "delay_seconds": delay,
                "extra_spread_pips": spread,
            },
        )
        exact_weighted = exact_weighted_ledger(weighted_raw, entry_delay_seconds=delay, extra_spread_pips=spread)
        exact_weighted.to_csv(output_dir / f"{name}_raw.csv", index=False)
        metrics, yearly = summarize_weighted_ledger(exact_weighted)
        yearly.to_csv(output_dir / f"{name}_yearly.csv", index=False)
        rows.append({"scenario": name, **metrics})

    frame = pd.DataFrame(rows)
    frame.to_csv(output_dir / "scenario_metrics.csv", index=False)

    summary = {
        "generated_at": now_iso(),
        "scenario_count": int(len(frame)),
        "m1_weighted": frame.loc[frame["scenario"] == "m1_weighted"].iloc[0].to_dict(),
        "exact_tick": frame.loc[frame["scenario"] == "exact_tick"].iloc[0].to_dict(),
        "exact_tick_delay_2s": frame.loc[frame["scenario"] == "exact_tick_delay_2s"].iloc[0].to_dict(),
        "exact_tick_spread_0.1": frame.loc[frame["scenario"] == "exact_tick_spread_0.1"].iloc[0].to_dict(),
        "exact_tick_spread_0.3": frame.loc[frame["scenario"] == "exact_tick_spread_0.3"].iloc[0].to_dict(),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines = [
        "# Locked Validation Exact Candidate Check",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        "## Scenario Metrics",
        "",
    ]
    for row in rows:
        lines.extend(
            [
                f"### {row['scenario']}",
                "",
                f"- ROI: `{row['roi_pct']}`",
                f"- Sharpe: `{row['sharpe_ann']}`",
                f"- Max drawdown: `{row['max_drawdown_pct']}`",
                f"- Profit factor: `{row['profit_factor']}`",
                f"- Trades: `{row['trades']}`",
                f"- Win rate: `{row['win_rate_pct']}`",
                "",
            ]
        )
    (output_dir / "exact_candidate_report.md").write_text("\n".join(lines), encoding="utf-8")

    write_status(
        status_path,
        {
            "timestamp": now_iso(),
            "stage": "completed",
            "message": "Locked Validation exact candidate check completed.",
            **summary,
        },
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
