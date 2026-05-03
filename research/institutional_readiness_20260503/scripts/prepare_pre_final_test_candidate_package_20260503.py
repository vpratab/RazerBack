from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(r"C:\Users\saanvi\Documents\Codex\2026-04-20-what-i-need-from-the-new")

PACKAGE_DIR = ROOT / "locked_pre_final_test_candidate_package_20260503"
RUNNER_PATH = ROOT / "locked_final_test_one_shot_runner_20260503.py"
BOOK_PATH = ROOT / "locked_validation_slippage_reweight_probe_20260502" / "best_execution_robust_candidate_book.json"
EXACT_SUMMARY_PATH = ROOT / "locked_validation_exact_candidate_check_20260503" / "summary.json"
VALIDATION_MANIFEST_PATH = ROOT / "institutionalization_phase1_20260501" / "validation_snapshot_manifest.json"
DISCOVERY_MANIFEST_PATH = ROOT / "institutionalization_phase1_20260501" / "discovery_snapshot_manifest.json"
FINAL_TEST_MANIFEST_PATH = ROOT / "institutionalization_phase1_20260501" / "final_test_snapshot_manifest.json"
EURGBP_SOURCE_PATH = ROOT / "locked_validation_full_survivor_sweep_20260502" / "validation_profitable_candidates.csv"
USDJPY_SOURCE_PATH = ROOT / "locked_discovery_usdjpy_survivor_followup_20260502" / "validation_results.csv"
PHASE_DOC_PATH = ROOT / "institutionalization_phase1_20260501" / "pre_final_test_freeze_package_milestone_20260503.md"
PLATFORM_HEALTH_PATH = ROOT / "platform_health.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_module_row(csv_path: Path, module_name: str) -> dict[str, Any]:
    frame = pd.read_csv(csv_path)
    row = frame.loc[frame["module"] == module_name]
    if row.empty:
        raise SystemExit(f"Could not find module {module_name} in {csv_path}")
    payload = row.iloc[0].to_dict()
    payload["module"] = module_name
    return payload


def clean_jsonish(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): clean_jsonish(val) for key, val in value.items()}
    if isinstance(value, list):
        return [clean_jsonish(item) for item in value]
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


def select_source_csv(module_name: str) -> Path:
    if module_name.startswith("eurgbp_"):
        return EURGBP_SOURCE_PATH
    if module_name.startswith("usdjpy_"):
        return USDJPY_SOURCE_PATH
    raise SystemExit(f"No source CSV mapping for module {module_name}")


def main() -> None:
    PACKAGE_DIR.mkdir(parents=True, exist_ok=True)

    status_path = PACKAGE_DIR / "status.json"
    status_path.write_text(
        json.dumps(
            {
                "timestamp": now_iso(),
                "stage": "build_package",
                "message": "Building frozen pre-Final-Test candidate package.",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    book = load_json(BOOK_PATH)
    exact_summary = load_json(EXACT_SUMMARY_PATH)
    discovery_manifest = load_json(DISCOVERY_MANIFEST_PATH)
    validation_manifest = load_json(VALIDATION_MANIFEST_PATH)
    final_test_manifest = load_json(FINAL_TEST_MANIFEST_PATH)

    modules: list[dict[str, Any]] = []
    for module_info in book["modules"]:
        alias = str(module_info["alias"])
        module_name = str(module_info["module"])
        weight = float(module_info["weight"])
        source_csv = select_source_csv(module_name)
        source_row = load_module_row(source_csv, module_name)
        modules.append(
            {
                "alias": alias,
                "module": module_name,
                "weight": weight,
                "source_csv": str(source_csv),
                "source_csv_sha256": sha256_file(source_csv),
                "source_row": clean_jsonish(source_row),
                "discovery_gate": {
                    "trade_count": int(source_row.get("discovery_trade_count", source_row.get("trades", 0))),
                    "walk_windows_total": int(source_row.get("walk_windows_total", 0)),
                    "walk_windows_with_trades": int(source_row.get("walk_windows_with_trades", 0)),
                    "walk_positive_windows": int(source_row.get("walk_positive_windows", 0)),
                    "walk_robust": bool(source_row.get("walk_robust", False)),
                    "dsr": float(source_row.get("dsr", 0.0)),
                    "discovery_sharpe_ann_recalc": float(source_row.get("discovery_sharpe_ann_recalc", source_row.get("sharpe_ann", 0.0))),
                    "discovery_profit_factor_recalc": float(source_row.get("discovery_profit_factor_recalc", source_row.get("profit_factor", 0.0))),
                    "discovery_max_drawdown_recalc": float(source_row.get("discovery_max_drawdown_recalc", source_row.get("max_drawdown_pct", 0.0))),
                },
                "validation_metrics": {
                    "trade_count": int(source_row.get("validation_trade_count", 0)),
                    "validation_sharpe_ann": float(source_row.get("validation_sharpe_ann", 0.0)),
                    "validation_profit_factor": float(source_row.get("validation_profit_factor", 0.0)),
                    "validation_max_drawdown": float(source_row.get("validation_max_drawdown", 0.0)),
                    "validation_total_pnl_dollars": float(source_row.get("validation_total_pnl_dollars", 0.0)),
                },
            }
        )

    package = {
        "candidate_package_id": "locked_pre_final_test_candidate_package_20260503",
        "generated_at": now_iso(),
        "holdout_status": "untouched",
        "candidate_book_path": str(BOOK_PATH),
        "candidate_book_sha256": sha256_file(BOOK_PATH),
        "runner_path": str(RUNNER_PATH),
        "runner_sha256": sha256_file(RUNNER_PATH),
        "final_test_output_dir": str(ROOT / "locked_final_test_one_shot_20260503"),
        "modules": modules,
        "snapshots": {
            "discovery": {
                "snapshot_id": discovery_manifest["snapshot_id"],
                "root": discovery_manifest["root"],
                "manifest_path": str(DISCOVERY_MANIFEST_PATH),
                "manifest_sha256": sha256_file(DISCOVERY_MANIFEST_PATH),
            },
            "validation": {
                "snapshot_id": validation_manifest["snapshot_id"],
                "root": validation_manifest["root"],
                "manifest_path": str(VALIDATION_MANIFEST_PATH),
                "manifest_sha256": sha256_file(VALIDATION_MANIFEST_PATH),
            },
            "final_test": {
                "snapshot_id": final_test_manifest["snapshot_id"],
                "root": final_test_manifest["root"],
                "manifest_path": str(FINAL_TEST_MANIFEST_PATH),
                "manifest_sha256": sha256_file(FINAL_TEST_MANIFEST_PATH),
            },
        },
        "validation_proxy_realism": book["proxy_realism_metrics"],
        "validation_proxy_event15": book["proxy_event15_metrics"],
        "validation_exact_check": {
            "source_summary_path": str(EXACT_SUMMARY_PATH),
            "source_summary_sha256": sha256_file(EXACT_SUMMARY_PATH),
            "exact_tick": exact_summary["exact_tick"],
            "exact_tick_delay_2s": exact_summary["exact_tick_delay_2s"],
            "exact_tick_spread_0.3": exact_summary["exact_tick_spread_0.3"],
        },
    }

    package_path = PACKAGE_DIR / "candidate_package.json"
    package_path.write_text(json.dumps(clean_jsonish(package), indent=2, allow_nan=False), encoding="utf-8")

    md_lines = [
        "# Frozen Pre-Final-Test Candidate Package",
        "",
        f"Generated: {package['generated_at']}",
        f"Candidate package id: `{package['candidate_package_id']}`",
        "",
        "## Frozen Book",
        "",
        f"- Candidate book: `{BOOK_PATH}`",
        f"- Validation snapshot: `{validation_manifest['snapshot_id']}`",
        f"- Final Test snapshot: `{final_test_manifest['snapshot_id']}`",
        f"- Final Test runner: `{RUNNER_PATH}`",
        f"- Holdout status: `{package['holdout_status']}`",
        "",
        "## Modules",
        "",
    ]
    for module in modules:
        md_lines.extend(
            [
                f"### {module['alias']}",
                "",
                f"- Module: `{module['module']}`",
                f"- Weight: `{module['weight']}`",
                f"- Discovery gate: `{module['discovery_gate']['walk_positive_windows']}/{module['discovery_gate']['walk_windows_total']}` windows, DSR `{module['discovery_gate']['dsr']}`",
                f"- Validation: Sharpe `{module['validation_metrics']['validation_sharpe_ann']}`, PF `{module['validation_metrics']['validation_profit_factor']}`, trades `{module['validation_metrics']['trade_count']}`",
                "",
            ]
        )
    md_lines.extend(
        [
            "## Validation Execution Evidence",
            "",
            f"- Proxy realism Sharpe: `{book['proxy_realism_metrics']['sharpe_ann']}`",
            f"- Proxy realism PF: `{book['proxy_realism_metrics']['profit_factor']}`",
            f"- Proxy realism positive years: `{book['proxy_realism_metrics']['positive_years']}/{book['proxy_realism_metrics']['year_count']}`",
            f"- Exact Validation Sharpe: `{exact_summary['exact_tick']['sharpe_ann']}`",
            f"- Exact Validation PF: `{exact_summary['exact_tick']['profit_factor']}`",
            f"- Exact Validation positive years: `{exact_summary['exact_tick']['positive_years']}/{exact_summary['exact_tick']['year_count']}`",
            f"- Exact +2s delay PF: `{exact_summary['exact_tick_delay_2s']['profit_factor']}`",
            f"- Exact +0.3 pip spread PF: `{exact_summary['exact_tick_spread_0.3']['profit_factor']}`",
            "",
            "## Final Test Control",
            "",
            "- The Final Test snapshot remains untouched.",
            "- The one-shot runner is armed but requires `--execute-final-test` and will refuse any second run once the marker file exists.",
            "",
        ]
    )
    (PACKAGE_DIR / "candidate_package.md").write_text("\n".join(md_lines), encoding="utf-8")

    phase_lines = [
        "# Pre-Final-Test Freeze Package Milestone",
        "",
        f"Generated: {package['generated_at']}",
        "",
        "A frozen pre-Final-Test candidate package is now on disk and ties together:",
        "",
        f"- the locked Validation execution-robust book at `{BOOK_PATH}`",
        f"- the stronger locked Validation exact-tick check at `{EXACT_SUMMARY_PATH}`",
        f"- explicit snapshot lineage for Discovery `{discovery_manifest['snapshot_id']}`, Validation `{validation_manifest['snapshot_id']}`, and Final Test `{final_test_manifest['snapshot_id']}`",
        f"- an armed one-shot Final Test runner at `{RUNNER_PATH}` that keeps the holdout untouched until explicit execution",
        "",
        "This package does not touch the Final Test data path itself. It freezes the current best candidate and the evidence chain needed for the final untouched run.",
        "",
    ]
    PHASE_DOC_PATH.write_text("\n".join(phase_lines), encoding="utf-8")

    if PLATFORM_HEALTH_PATH.exists():
        health = load_json(PLATFORM_HEALTH_PATH)
        health.setdefault("portfolio_quality", {})
        health.setdefault("execution_realism", {})
        health["portfolio_quality"]["pre_final_test_candidate_package_present"] = True
        health["portfolio_quality"]["pre_final_test_candidate_package_path"] = str(package_path)
        health["execution_realism"]["final_test_runner_present"] = True
        health["execution_realism"]["final_test_runner_path"] = str(RUNNER_PATH)
        health["execution_realism"]["final_test_holdout_status"] = "untouched"
        PLATFORM_HEALTH_PATH.write_text(json.dumps(health, indent=2), encoding="utf-8")

    status_path.write_text(
        json.dumps(
            {
                "timestamp": now_iso(),
                "stage": "completed",
                "message": "Frozen pre-Final-Test candidate package created.",
                "package_path": str(package_path),
                "phase_doc_path": str(PHASE_DOC_PATH),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(json.dumps({"package_path": str(package_path), "phase_doc_path": str(PHASE_DOC_PATH)}, indent=2))


if __name__ == "__main__":
    main()
