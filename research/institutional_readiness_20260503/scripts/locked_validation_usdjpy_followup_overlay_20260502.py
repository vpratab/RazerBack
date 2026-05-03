from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations, product
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(r"C:\Users\saanvi\Documents\Codex\2026-04-20-what-i-need-from-the-new")
REPO_ROOT = Path(r"C:\Users\saanvi\Documents\GitHub\RazerBack")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import locked_discovery_gate_top_by_pair_20260501 as gate
import scripts.evaluation_framework as ef
import scripts.run_multifamily_fx_research as multi


@dataclass(frozen=True)
class ModuleSource:
    alias: str
    module: str
    csv_path: Path


EURGBP_A = ModuleSource(
    alias="eurgbp_a",
    module="eurgbp_long_bo_h8_rr1.25_dist5_wickmax5_retge20_z2_sl20_tp15_shield11-6_ttl60",
    csv_path=ROOT / "locked_validation_full_survivor_sweep_20260502" / "validation_profitable_candidates.csv",
)
EURGBP_B = ModuleSource(
    alias="eurgbp_b",
    module="eurgbp_long_bo_h8_rr1.25_dist5_wickmax5_retge20_z2_sl15_tp11_shield6-3_ttl60",
    csv_path=ROOT / "locked_validation_full_survivor_sweep_20260502" / "validation_profitable_candidates.csv",
)
USDJPY_BASE = ModuleSource(
    alias="usdjpy_base",
    module="usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z1_sl30_tp45_shield22-7_ttl60",
    csv_path=ROOT / "usdjpy_regime_filter_probe_tmp_20260502.csv",
)
USDJPY_FOLLOWUPS = [
    ModuleSource(
        alias="usdjpy_z15_t60",
        module="usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z1.5_sl30_tp45_shield22-7_ttl60",
        csv_path=ROOT / "locked_discovery_usdjpy_survivor_followup_20260502" / "validation_results.csv",
    ),
    ModuleSource(
        alias="usdjpy_z1_t180",
        module="usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z1_sl30_tp45_shield22-7_ttl180",
        csv_path=ROOT / "locked_discovery_usdjpy_survivor_followup_20260502" / "validation_results.csv",
    ),
    ModuleSource(
        alias="usdjpy_z05_t60",
        module="usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z0.5_sl30_tp45_shield22-7_ttl60",
        csv_path=ROOT / "locked_discovery_usdjpy_survivor_followup_20260502" / "validation_results.csv",
    ),
    ModuleSource(
        alias="usdjpy_z2_t60",
        module="usdjpy_short_bo_h14_rr0.85_dist5_wickmax15_retle-10_z2_sl30_tp45_shield22-7_ttl60",
        csv_path=ROOT / "locked_discovery_usdjpy_survivor_followup_20260502" / "validation_results.csv",
    ),
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_status(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_row(source: ModuleSource) -> dict[str, Any]:
    frame = pd.read_csv(source.csv_path)
    row = frame.loc[frame["module"] == source.module]
    if row.empty:
        raise SystemExit(f"Could not find {source.module} in {source.csv_path}")
    payload = row.iloc[0].to_dict()
    payload["module"] = source.module
    return payload


def build_validation_ledgers(rows: dict[str, dict[str, Any]]) -> dict[str, pd.DataFrame]:
    validation_datasets = multi.load_dataset_bundle(
        ROOT / "phase1_validation_snapshot_full_20260501",
        instruments=list(gate.cont.DEFAULT_INSTRUMENTS),
    )
    ledgers: dict[str, pd.DataFrame] = {}
    for alias, row in rows.items():
        spec = gate.build_multifamily_spec(row)
        result = multi.run_portfolio_from_rows(
            pd.DataFrame([row]),
            {spec.name: spec},
            validation_datasets,
            start_balance=1500.0,
            risk_pct=2.9,
            max_leverage=35.0,
            max_concurrent=2,
        )
        ledger = result["trade_ledger"].copy()
        ledger["alias"] = alias
        ledgers[alias] = ledger
    return ledgers


def scale_ledger(ledger: pd.DataFrame, weight: float, alias: str) -> pd.DataFrame:
    scaled = ledger.copy()
    scaled["pnl_dollars"] = scaled["pnl_dollars"].astype(float) * weight
    # Pips are a market-path outcome, not a position-size outcome.
    # Keep pips unchanged so downstream slippage studies can convert pips
    # to dollars using the weighted notional rather than double-scaling cost.
    scaled["pnl_pips"] = scaled["pnl_pips"].astype(float)
    scaled["weighted_alias"] = alias
    return scaled


def summarize_scaled_ledger(ledger: pd.DataFrame) -> dict[str, Any]:
    if ledger.empty:
        return {
            "trade_count": 0,
            "sharpe_ann": None,
            "max_drawdown": 0.0,
            "profit_factor": None,
            "win_rate": None,
            "total_pnl_dollars": 0.0,
            "years_positive": 0,
            "year_count": 0,
            "pnl_2020": 0.0,
            "pnl_2021": 0.0,
            "pnl_2022": 0.0,
        }
    summary = ef.summarize_module_trade_history(
        ledger,
        timestamp_col="exit_time",
        pnl_col="pnl_dollars",
        starting_capital=1500.0,
    )
    ledger = ledger.copy()
    ledger["exit_time"] = pd.to_datetime(ledger["exit_time"], utc=True)
    ledger["year"] = ledger["exit_time"].dt.year
    yearly = ledger.groupby("year")["pnl_dollars"].sum()
    payload = {
        "trade_count": int(summary["trade_count"]),
        "sharpe_ann": None if pd.isna(summary["sharpe_ann"]) else float(summary["sharpe_ann"]),
        "max_drawdown": None if pd.isna(summary["max_drawdown"]) else float(summary["max_drawdown"]),
        "profit_factor": None if pd.isna(summary["profit_factor"]) else float(summary["profit_factor"]),
        "win_rate": None if pd.isna(summary["win_rate"]) else float(summary["win_rate"]),
        "total_pnl_dollars": float(ledger["pnl_dollars"].sum()),
        "years_positive": int((yearly > 0.0).sum()),
        "year_count": int(len(yearly)),
        "pnl_2020": float(yearly.get(2020, 0.0)),
        "pnl_2021": float(yearly.get(2021, 0.0)),
        "pnl_2022": float(yearly.get(2022, 0.0)),
    }
    return payload


def main() -> None:
    output_dir = ROOT / "locked_validation_usdjpy_followup_overlay_20260502"
    output_dir.mkdir(parents=True, exist_ok=True)
    status_path = output_dir / "status.json"

    module_sources = [EURGBP_A, EURGBP_B, USDJPY_BASE, *USDJPY_FOLLOWUPS]
    module_rows = {source.alias: load_row(source) for source in module_sources}

    write_status(
        status_path,
        {
            "timestamp": now_iso(),
            "stage": "build_validation_ledgers",
            "message": "Building locked Validation ledgers for EURGBP and USDJPY follow-up overlay probe.",
            "module_count": len(module_sources),
        },
    )
    ledgers = build_validation_ledgers(module_rows)
    corr = ef.daily_pnl_correlation_matrix(ledgers, timestamp_col="exit_time", pnl_col="pnl_dollars")
    corr.to_csv(output_dir / "module_correlation_matrix.csv")

    usdjpy_aliases = [USDJPY_BASE.alias] + [source.alias for source in USDJPY_FOLLOWUPS]
    structures: list[tuple[str, list[str]]] = []
    for alias in usdjpy_aliases:
        structures.append((f"trio_{alias}", [EURGBP_A.alias, EURGBP_B.alias, alias]))
    for combo in combinations(usdjpy_aliases, 2):
        structures.append((f"quartet_{combo[0]}__{combo[1]}", [EURGBP_A.alias, EURGBP_B.alias, *combo]))

    weight_grid_eurgbp_a = [0.75, 1.0, 1.25, 1.5, 1.75, 2.0]
    weight_grid_eurgbp_b = [0.25, 0.5, 0.75, 1.0, 1.25]
    weight_grid_usdjpy = [0.05, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5, 0.75, 1.0]

    all_rows: list[dict[str, Any]] = []
    best_rows: list[dict[str, Any]] = []
    feasible_rows: list[dict[str, Any]] = []

    for idx, (structure_name, aliases) in enumerate(structures, start=1):
        write_status(
            status_path,
            {
                "timestamp": now_iso(),
                "stage": "probe_structure",
                "message": f"Evaluating structure {idx}/{len(structures)}.",
                "structure": structure_name,
            },
        )
        usdjpy_in_structure = [alias for alias in aliases if alias.startswith("usdjpy_")]
        best_row: dict[str, Any] | None = None
        weight_products = product(weight_grid_eurgbp_a, weight_grid_eurgbp_b, weight_grid_usdjpy, weight_grid_usdjpy if len(usdjpy_in_structure) == 2 else [None])
        for wa, wb, wu1, wu2 in weight_products:
            weight_map = {EURGBP_A.alias: wa, EURGBP_B.alias: wb, usdjpy_in_structure[0]: wu1}
            if len(usdjpy_in_structure) == 2:
                weight_map[usdjpy_in_structure[1]] = float(wu2)
            combined = pd.concat(
                [scale_ledger(ledgers[alias], weight_map[alias], alias) for alias in aliases],
                ignore_index=True,
            )
            metrics = summarize_scaled_ledger(combined)
            row = {
                "structure": structure_name,
                "modules": "|".join(aliases),
                "wa": wa,
                "wb": wb,
                "wu1_alias": usdjpy_in_structure[0],
                "wu1": wu1,
                "wu2_alias": usdjpy_in_structure[1] if len(usdjpy_in_structure) == 2 else "",
                "wu2": float(wu2) if wu2 is not None else None,
                **metrics,
            }
            row["feasible"] = bool(
                row["max_drawdown"] is not None
                and row["max_drawdown"] >= -0.08
                and row["profit_factor"] is not None
                and row["profit_factor"] >= 1.8
            )
            all_rows.append(row)
            if row["feasible"]:
                feasible_rows.append(row)
            if best_row is None:
                best_row = row
                continue
            best_key = (
                row["feasible"],
                row["years_positive"],
                row["pnl_2021"] > 0.0,
                row["pnl_2021"],
                -9999.0 if row["sharpe_ann"] is None else row["sharpe_ann"],
                row["total_pnl_dollars"],
            )
            prior_key = (
                best_row["feasible"],
                best_row["years_positive"],
                best_row["pnl_2021"] > 0.0,
                best_row["pnl_2021"],
                -9999.0 if best_row["sharpe_ann"] is None else best_row["sharpe_ann"],
                best_row["total_pnl_dollars"],
            )
            if best_key > prior_key:
                best_row = row
        if best_row is not None:
            best_rows.append(best_row)

    all_frame = pd.DataFrame(all_rows)
    best_frame = pd.DataFrame(best_rows).sort_values(
        ["feasible", "years_positive", "pnl_2021", "sharpe_ann", "total_pnl_dollars"],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)
    feasible_frame = pd.DataFrame(feasible_rows).sort_values(
        ["years_positive", "pnl_2021", "sharpe_ann", "total_pnl_dollars"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)

    all_frame.to_csv(output_dir / "overlay_grid.csv", index=False)
    best_frame.to_csv(output_dir / "best_by_structure.csv", index=False)
    feasible_frame.to_csv(output_dir / "overlay_feasible.csv", index=False)

    summary: dict[str, Any] = {
        "generated_at": now_iso(),
        "structure_count": int(len(structures)),
        "grid_count": int(len(all_frame)),
        "feasible_count": int(len(feasible_frame)),
    }
    if not best_frame.empty:
        top = best_frame.iloc[0].to_dict()
        summary["best_structure"] = top
    if not feasible_frame.empty:
        topf = feasible_frame.iloc[0].to_dict()
        summary["best_feasible"] = topf

    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    report_lines = [
        "# USDJPY Follow-Up Overlay Probe",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        f"- Structures tested: `{summary['structure_count']}`",
        f"- Overlay grid rows: `{summary['grid_count']}`",
        f"- Feasible overlays (DD <= 8%, PF >= 1.8): `{summary['feasible_count']}`",
    ]
    if "best_structure" in summary:
        report_lines.extend(
            [
                "",
                "## Best Structure",
                "",
                f"- Structure: `{summary['best_structure']['structure']}`",
                f"- Modules: `{summary['best_structure']['modules']}`",
                f"- Positive years: `{summary['best_structure']['years_positive']}/{summary['best_structure']['year_count']}`",
                f"- 2021 PnL: `{summary['best_structure']['pnl_2021']:.4f}`",
                f"- Sharpe: `{summary['best_structure']['sharpe_ann']}`",
                f"- Max drawdown: `{summary['best_structure']['max_drawdown']}`",
                f"- Profit factor: `{summary['best_structure']['profit_factor']}`",
                f"- Total PnL dollars: `{summary['best_structure']['total_pnl_dollars']:.4f}`",
            ]
        )
    if "best_feasible" in summary:
        report_lines.extend(
            [
                "",
                "## Best Feasible Overlay",
                "",
                f"- Structure: `{summary['best_feasible']['structure']}`",
                f"- Modules: `{summary['best_feasible']['modules']}`",
                f"- Positive years: `{summary['best_feasible']['years_positive']}/{summary['best_feasible']['year_count']}`",
                f"- 2021 PnL: `{summary['best_feasible']['pnl_2021']:.4f}`",
                f"- Sharpe: `{summary['best_feasible']['sharpe_ann']}`",
                f"- Max drawdown: `{summary['best_feasible']['max_drawdown']}`",
                f"- Profit factor: `{summary['best_feasible']['profit_factor']}`",
                f"- Total PnL dollars: `{summary['best_feasible']['total_pnl_dollars']:.4f}`",
            ]
        )
    (output_dir / "overlay_report.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    write_status(
        status_path,
        {
            "timestamp": now_iso(),
            "stage": "completed",
            "message": "USDJPY follow-up overlay probe completed.",
            **summary,
        },
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
