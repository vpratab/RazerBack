from __future__ import annotations

import argparse
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
import locked_discovery_gate_top_by_pair_20260501 as gate
import scripts.evaluation_framework as ef
import scripts.run_multifamily_fx_research as multi


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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


def build_final_test_ledgers(modules: list[dict[str, Any]], dataset_root: Path) -> dict[str, pd.DataFrame]:
    datasets = multi.load_dataset_bundle(
        dataset_root,
        instruments=list(gate.cont.DEFAULT_INSTRUMENTS),
    )
    ledgers: dict[str, pd.DataFrame] = {}
    for module_info in modules:
        alias = str(module_info["alias"])
        row = dict(module_info["source_row"])
        spec = gate.build_multifamily_spec(row)
        result = multi.run_portfolio_from_rows(
            pd.DataFrame([row]),
            {spec.name: spec},
            datasets,
            start_balance=1500.0,
            risk_pct=2.9,
            max_leverage=35.0,
            max_concurrent=2,
        )
        ledger = result["trade_ledger"].copy()
        ledger["alias"] = alias
        ledgers[alias] = ledger
    return ledgers


def build_weighted_raw_book(modules: list[dict[str, Any]], ledgers: dict[str, pd.DataFrame]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for module_info in modules:
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
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts, ignore_index=True)
    return out.sort_values(["entry_time", "module"]).reset_index(drop=True)


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


def run_final_test(package_path: Path, output_dir: Path) -> dict[str, Any]:
    package = json.loads(package_path.read_text(encoding="utf-8"))
    modules = list(package["modules"])
    dataset_root = Path(package["snapshots"]["final_test"]["root"])

    status_path = output_dir / "status.json"
    write_json(
        status_path,
        {
            "timestamp": now_iso(),
            "stage": "build_final_test_ledgers",
            "message": "Building final-test ledgers from frozen candidate package.",
            "package_path": str(package_path),
        },
    )
    ledgers = build_final_test_ledgers(modules, dataset_root)

    write_json(
        status_path,
        {
            "timestamp": now_iso(),
            "stage": "build_weighted_raw_book",
            "message": "Building weighted final-test raw book.",
        },
    )
    weighted_raw = build_weighted_raw_book(modules, ledgers)
    weighted_raw.to_csv(output_dir / "weighted_final_test_raw_book.csv", index=False)

    rows: list[dict[str, Any]] = []
    m1_metrics, m1_yearly = summarize_weighted_ledger(weighted_raw)
    rows.append({"scenario": "m1_weighted", **m1_metrics})
    m1_yearly.to_csv(output_dir / "m1_yearly.csv", index=False)

    exact_scenarios = [
        ("exact_tick", 0, 0.0),
        ("exact_tick_delay_2s", 2, 0.0),
        ("exact_tick_spread_0.3", 0, 0.3),
    ]
    for name, delay, spread in exact_scenarios:
        write_json(
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
        rows.append({"scenario": name, **metrics})
        yearly.to_csv(output_dir / f"{name}_yearly.csv", index=False)

    metrics_frame = pd.DataFrame(rows)
    metrics_frame.to_csv(output_dir / "scenario_metrics.csv", index=False)

    summary = {
        "generated_at": now_iso(),
        "package_id": package["candidate_package_id"],
        "candidate_book_path": str(package["candidate_book_path"]),
        "scenario_count": int(len(metrics_frame)),
        "m1_weighted": metrics_frame.loc[metrics_frame["scenario"] == "m1_weighted"].iloc[0].to_dict(),
        "exact_tick": metrics_frame.loc[metrics_frame["scenario"] == "exact_tick"].iloc[0].to_dict(),
        "exact_tick_delay_2s": metrics_frame.loc[metrics_frame["scenario"] == "exact_tick_delay_2s"].iloc[0].to_dict(),
        "exact_tick_spread_0.3": metrics_frame.loc[metrics_frame["scenario"] == "exact_tick_spread_0.3"].iloc[0].to_dict(),
    }
    write_json(output_dir / "summary.json", summary)

    lines = [
        "# Locked Final Test One-Shot Report",
        "",
        f"Generated: {summary['generated_at']}",
        f"Package: `{summary['package_id']}`",
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
                f"- Positive years: `{row['positive_years']}/{row['year_count']}`",
                "",
            ]
        )
    (output_dir / "final_test_report.md").write_text("\n".join(lines), encoding="utf-8")

    write_json(
        status_path,
        {
            "timestamp": now_iso(),
            "stage": "completed",
            "message": "Locked Final Test one-shot run completed.",
            **summary,
        },
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="One-shot Final Test runner for the frozen RazerBack candidate package.")
    parser.add_argument(
        "--package",
        default=str(ROOT / "locked_pre_final_test_candidate_package_20260503" / "candidate_package.json"),
        help="Path to the frozen candidate package JSON.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(ROOT / "locked_final_test_one_shot_20260503"),
        help="Directory for final-test outputs.",
    )
    parser.add_argument(
        "--execute-final-test",
        action="store_true",
        help="Actually execute the untouched Final Test. Without this flag the runner only arms and exits.",
    )
    args = parser.parse_args()

    package_path = Path(args.package)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    marker_path = output_dir / "final_test_execution_marker.json"
    status_path = output_dir / "status.json"

    if marker_path.exists():
        raise SystemExit(f"Final Test already executed. Marker present at {marker_path}")

    if not args.execute_final_test:
        write_json(
            status_path,
            {
                "timestamp": now_iso(),
                "stage": "armed",
                "message": "Runner prepared but Final Test remains untouched. Re-run with --execute-final-test to consume the holdout exactly once.",
                "package_path": str(package_path),
                "output_dir": str(output_dir),
                "marker_path": str(marker_path),
            },
        )
        print(json.dumps(json.loads(status_path.read_text(encoding="utf-8")), indent=2))
        return

    write_json(
        marker_path,
        {
            "timestamp": now_iso(),
            "event": "final_test_execution_started",
            "package_path": str(package_path),
            "output_dir": str(output_dir),
        },
    )
    summary = run_final_test(package_path, output_dir)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
