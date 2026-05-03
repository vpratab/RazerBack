from __future__ import annotations

import json
import sys
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

import locked_validation_realism_proxy_gauntlet_20260502 as ga
import locked_validation_usdjpy_followup_overlay_20260502 as ov


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_status(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def main() -> None:
    output_dir = ROOT / "locked_validation_slippage_reweight_probe_20260502"
    output_dir.mkdir(parents=True, exist_ok=True)
    status_path = output_dir / "status.json"

    rows = {src.alias: ov.load_row(src) for src in [ov.EURGBP_A, ov.EURGBP_B, ov.USDJPY_BASE, *ov.USDJPY_FOLLOWUPS]}
    ledgers = ov.build_validation_ledgers(rows)

    candidates = ["eurgbp_a", "eurgbp_b", "usdjpy_z15_t60", "usdjpy_z1_t180", "usdjpy_base", "usdjpy_z05_t60"]
    structures: list[tuple[str, ...]] = []
    for r in [2, 3, 4]:
        for combo in combinations(candidates, r):
            if not any(alias.startswith("eurgbp") for alias in combo):
                continue
            if not any(alias.startswith("usdjpy") for alias in combo):
                continue
            structures.append(combo)

    weight_options = {
        "eurgbp_a": [0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5],
        "eurgbp_b": [0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5],
        "usdjpy_z15_t60": [0.05, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5, 0.75, 1.0],
        "usdjpy_z1_t180": [0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5],
        "usdjpy_base": [0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5],
        "usdjpy_z05_t60": [0.0, 0.05, 0.1, 0.15, 0.2, 0.25, 0.35, 0.5],
    }

    all_rows: list[dict[str, Any]] = []
    feasible_rows: list[dict[str, Any]] = []
    evaluated = 0
    total_estimated = 0
    for combo in structures:
        count = 1
        for alias in combo:
            count *= len(weight_options[alias])
        total_estimated += count

    for idx, combo in enumerate(structures, start=1):
        write_status(
            status_path,
            {
                "timestamp": now_iso(),
                "stage": "probe_structure",
                "message": f"Evaluating slippage-aware reweight structure {idx}/{len(structures)}.",
                "structure": "|".join(combo),
                "evaluated": evaluated,
                "estimated_total": total_estimated,
            },
        )
        grids = [weight_options[alias] for alias in combo]
        for vals in product(*grids):
            weights = dict(zip(combo, vals))
            if any(v == 0 for v in vals):
                continue
            combined = pd.concat([ov.scale_ledger(ledgers[alias], weights[alias], alias) for alias in combo], ignore_index=True)
            adjusted = ga.apply_proxy_costs(combined, slip_per_side_pips=0.5, blowout=1.0)
            metrics = ga.summarize_ledger(adjusted)
            evaluated += 1
            row = {
                "modules": "|".join(combo),
                **weights,
                **metrics,
                "passes_shape_gate": bool(
                    metrics["max_drawdown"] is not None
                    and metrics["max_drawdown"] >= -0.08
                    and metrics["profit_factor"] is not None
                    and metrics["profit_factor"] >= 1.8
                    and metrics["positive_years"] == metrics["year_count"]
                ),
            }
            all_rows.append(row)
            if row["passes_shape_gate"]:
                feasible_rows.append(row)

    frame = pd.DataFrame(all_rows).sort_values(
        ["passes_shape_gate", "profit_factor", "sharpe_ann", "pnl_2021"],
        ascending=[False, False, False, False],
    ).reset_index(drop=True)
    feasible = frame.loc[frame["passes_shape_gate"] == True].reset_index(drop=True)
    frame.to_csv(output_dir / "probe_results.csv", index=False)
    feasible.to_csv(output_dir / "probe_feasible.csv", index=False)

    summary: dict[str, Any] = {
        "generated_at": now_iso(),
        "structure_count": int(len(structures)),
        "evaluated_rows": int(len(frame)),
        "feasible_count": int(len(feasible)),
    }
    if not frame.empty:
        summary["best_row"] = frame.iloc[0].to_dict()
    if not feasible.empty:
        summary["best_feasible"] = feasible.iloc[0].to_dict()

    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    lines = [
        "# Locked Validation Slippage Reweight Probe",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        f"- Structures tested: `{summary['structure_count']}`",
        f"- Evaluated rows: `{summary['evaluated_rows']}`",
        f"- Feasible rows at `0.5` pip per side slippage: `{summary['feasible_count']}`",
        "",
    ]
    if "best_row" in summary:
        best = summary["best_row"]
        lines.extend(
            [
                "## Best row",
                "",
                f"- Modules: `{best['modules']}`",
                f"- Sharpe: `{best['sharpe_ann']}`",
                f"- Max drawdown: `{best['max_drawdown']}`",
                f"- Profit factor: `{best['profit_factor']}`",
                f"- Positive years: `{best['positive_years']}/{best['year_count']}`",
                f"- 2021 PnL: `{best['pnl_2021']}`",
                f"- Passes shape gate: `{best['passes_shape_gate']}`",
            ]
        )
    (output_dir / "probe_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    write_status(
        status_path,
        {
            "timestamp": now_iso(),
            "stage": "completed",
            "message": "Locked Validation slippage-aware reweight probe completed.",
            **summary,
        },
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
