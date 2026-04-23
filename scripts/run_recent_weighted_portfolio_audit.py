from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from continuation_core import load_datasets, resolve_execution_profile, simulate_module, simulate_portfolio
from locked_portfolio_runtime import build_trade_stats
from scripts.run_multifamily_fx_research import simulate_breakout_module, simulate_reversal_module
from scripts.run_portfolio_factory_research import (
    breakout_spec_from_row,
    continuation_spec_from_row,
    reversal_spec_from_row,
)


DEFAULT_DATA_DIR = Path("C:/fx_data/m1")
DEFAULT_SEARCH_ROOT = Path("C:/Users/saanvi/Documents/Codex/2026-04-20-what-i-need-from-the-new")
DEFAULT_OUTPUT_DIR = DEFAULT_SEARCH_ROOT / "recent_weighted_full_surface"
DEFAULT_YEARS = [2011, 2012, 2021, 2022, 2023, 2024, 2025]
DEFAULT_YEAR_WEIGHTS = {
    2011: 0.5,
    2012: 0.75,
    2021: 1.0,
    2022: 2.0,
    2023: 3.0,
    2024: 4.0,
    2025: 5.0,
}
REQUIRED_CONVERSION_INSTRUMENTS = {"usdjpy", "gbpusd"}
EXCLUDED_DISCOVERY_PARTS = {
    "pre2020_eval",
    "wfo_2025_test",
    "recent_weighted_full_surface",
    "data_audit_full",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Audit all discovered selected-candidate portfolio packs across full-year windows, "
            "favoring recent years while penalizing catastrophic older losses."
        )
    )
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--search-root", default=str(DEFAULT_SEARCH_ROOT))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--scenario", choices=("base", "conservative", "hard"), default="hard")
    parser.add_argument("--years", nargs="+", type=int, default=DEFAULT_YEARS)
    parser.add_argument("--min-days", type=int, default=200)
    parser.add_argument("--start-balance", type=float, default=1500.0)
    parser.add_argument("--risk-pct", type=float, default=2.9)
    parser.add_argument("--max-leverage", type=float, default=35.0)
    parser.add_argument("--max-concurrent", type=int, default=2)
    parser.add_argument("--top-n", type=int, default=15)
    return parser.parse_args()


def normalize_family(value: Any, module_name: str) -> str:
    family = str(value).strip().lower()
    if family and family != "nan":
        return family
    if "_bo_" in module_name:
        return "breakout"
    if "_rev_" in module_name:
        return "reversal"
    return "continuation"


def normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["module"] = str(normalized["module"]).strip()
    normalized["instrument"] = str(normalized["instrument"]).strip().lower()
    normalized["side"] = str(normalized["side"]).strip().lower()
    normalized["family"] = normalize_family(normalized.get("family"), normalized["module"])
    return normalized


def discover_portfolios(search_root: Path) -> tuple[list[dict[str, Any]], int]:
    discovered: list[dict[str, Any]] = []
    seen_module_sets: dict[tuple[str, ...], str] = {}
    duplicate_count = 0

    for csv_path in sorted(search_root.rglob("selected_candidates.csv")):
        if any(part in EXCLUDED_DISCOVERY_PARTS for part in csv_path.parts):
            continue
        try:
            frame = pd.read_csv(csv_path)
        except Exception:
            continue
        if frame.empty or "module" not in frame.columns or "instrument" not in frame.columns:
            continue

        rows = [normalize_row(row) for row in frame.to_dict(orient="records")]
        module_set = tuple(sorted(str(row["module"]) for row in rows))
        if not module_set:
            continue
        if module_set in seen_module_sets:
            duplicate_count += 1
            continue

        seen_module_sets[module_set] = str(csv_path.parent)
        discovered.append(
            {
                "label": str(csv_path.parent.relative_to(search_root)),
                "path": csv_path.parent,
                "selected_csv": csv_path,
                "rows": rows,
                "modules": module_set,
            }
        )

    return discovered, duplicate_count


def annual_day_counts(dataset: dict[str, object]) -> dict[int, int]:
    frame = dataset["df"]
    timestamp = pd.to_datetime(frame["timestamp"], utc=True)
    day_frame = pd.DataFrame(
        {
            "year": timestamp.dt.year.astype(int),
            "day": timestamp.dt.normalize(),
        }
    )
    if day_frame.empty:
        return {}
    return day_frame.groupby("year")["day"].nunique().astype(int).to_dict()


def filter_dataset_window(data: dict[str, object], start: pd.Timestamp, end: pd.Timestamp) -> dict[str, object]:
    frame = data["df"]
    timestamp = pd.to_datetime(frame["timestamp"], utc=True)
    mask = (timestamp >= start) & (timestamp < end)
    keep = mask.to_numpy(dtype=bool)

    filtered: dict[str, object] = {}
    for key, value in data.items():
        if key in {"pip", "inferred_bid_ask"}:
            filtered[key] = value
        elif key in {"bid", "ask", "mid"}:
            filtered[key] = {subkey: subvalue[keep] for subkey, subvalue in value.items()}
        elif key == "df":
            filtered[key] = value.loc[keep].reset_index(drop=True)
        elif key == "enriched_df":
            filtered[key] = value.loc[keep].reset_index(drop=True)
        elif isinstance(value, np.ndarray):
            filtered[key] = value[keep]
        else:
            filtered[key] = value
    return filtered


def module_trades_for_row(
    row: dict[str, Any],
    window_dataset: dict[str, object],
    execution_profile_name: str,
) -> pd.DataFrame:
    family = str(row["family"])
    if family == "continuation":
        spec = continuation_spec_from_row(row)
        return simulate_module(spec, window_dataset, execution_profile=resolve_execution_profile(execution_profile_name))
    if family == "breakout":
        spec = breakout_spec_from_row(row)
        return simulate_breakout_module(spec, window_dataset, str(row["module"]))
    if family == "reversal":
        spec = reversal_spec_from_row(row)
        return simulate_reversal_module(spec, window_dataset, str(row["module"]))
    raise ValueError(f"Unsupported family: {family}")


def year_score(metrics: dict[str, Any]) -> float:
    sharpe = float(metrics["sharpe_ann"])
    roi = float(metrics["roi_pct"])
    drawdown = abs(float(metrics["max_drawdown_pct"]))
    win_rate = float(metrics["win_rate_pct"])
    profit_factor = min(float(metrics["profit_factor"]), 5.0)
    trades = int(metrics["trade_count"])

    score = 0.0
    score += min(sharpe / 1.5, 2.0) * 3.0
    score += min(max(roi, 0.0) / 25.0, 2.0) * 2.5
    score += min(win_rate / 65.0, 1.5) * 1.5
    score += min(profit_factor / 1.5, 1.5) * 1.0
    score += min(trades / 25.0, 1.5) * 1.0

    if drawdown > 10.0:
        score -= (drawdown - 10.0) * 0.6
    if roi < 0.0:
        score -= abs(roi) * 0.2
    if sharpe < 0.0:
        score -= abs(sharpe) * 3.0
    if win_rate < 55.0:
        score -= (55.0 - win_rate) * 0.05
    if trades < 10:
        score -= (10 - trades) * 0.3
    return float(score)


def aggregate_portfolio_score(frame: pd.DataFrame, year_weights: dict[int, float]) -> dict[str, Any]:
    by_year = frame.sort_values("year").reset_index(drop=True)
    recent_mask = by_year["year"].isin([2022, 2023, 2024, 2025])
    recent = by_year.loc[recent_mask]
    usable = by_year.loc[by_year["usable_window"]]

    weighted_score = 0.0
    total_weight = 0.0
    catastrophic_years = 0
    for row in usable.to_dict(orient="records"):
        weight = float(year_weights.get(int(row["year"]), 1.0))
        weighted_score += weight * float(row["year_score"])
        total_weight += weight
        if float(row["roi_pct"]) <= -20.0 or float(row["max_drawdown_pct"]) <= -15.0:
            catastrophic_years += 1

    recent_avg_roi = float(recent["roi_pct"].mean()) if not recent.empty else float("nan")
    recent_avg_sharpe = float(recent["sharpe_ann"].mean()) if not recent.empty else float("nan")
    recent_avg_win = float(recent["win_rate_pct"].mean()) if not recent.empty else float("nan")
    recent_avg_pf = float(recent["profit_factor"].mean()) if not recent.empty else float("nan")
    recent_avg_trades = float(recent["trade_count"].mean()) if not recent.empty else float("nan")
    worst_year_roi = float(usable["roi_pct"].min()) if not usable.empty else float("nan")
    worst_year_dd = float(usable["max_drawdown_pct"].min()) if not usable.empty else float("nan")
    best_year_roi = float(usable["roi_pct"].max()) if not usable.empty else float("nan")

    normalized_score = weighted_score / total_weight if total_weight > 0 else float("-inf")
    penalty = 0.0
    if not math.isnan(worst_year_roi):
        penalty += max(-worst_year_roi - 10.0, 0.0) * 0.4
    if not math.isnan(worst_year_dd):
        penalty += max(abs(worst_year_dd) - 10.0, 0.0) * 0.75
    penalty += catastrophic_years * 8.0

    return {
        "weighted_score": float(weighted_score),
        "normalized_score": float(normalized_score),
        "catastrophic_years": int(catastrophic_years),
        "penalty": float(penalty),
        "objective": float(normalized_score - penalty),
        "recent_avg_roi_pct": recent_avg_roi,
        "recent_avg_sharpe": recent_avg_sharpe,
        "recent_avg_win_rate_pct": recent_avg_win,
        "recent_avg_profit_factor": recent_avg_pf,
        "recent_avg_trade_count": recent_avg_trades,
        "worst_year_roi_pct": worst_year_roi,
        "worst_year_drawdown_pct": worst_year_dd,
        "best_year_roi_pct": best_year_roi,
        "usable_years": int(len(usable)),
    }


def markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows_"

    printable = frame.copy()
    for column in printable.columns:
        if pd.api.types.is_float_dtype(printable[column]):
            printable[column] = printable[column].map(lambda value: "" if pd.isna(value) else f"{value:.3f}")
    lines = [
        "| " + " | ".join(str(column) for column in printable.columns) + " |",
        "| " + " | ".join(["---"] * len(printable.columns)) + " |",
    ]
    for _, row in printable.iterrows():
        lines.append("| " + " | ".join(str(row[column]) for column in printable.columns) + " |")
    return "\n".join(lines)


def build_report(
    rankings: pd.DataFrame,
    yearly_metrics: pd.DataFrame,
    coverage_frame: pd.DataFrame,
    top_n: int,
    args: argparse.Namespace,
) -> str:
    top = rankings.head(top_n).copy()
    report_lines = [
        "# Recent-Weighted Portfolio Audit",
        "",
        "## Objective",
        "",
        "Rank existing portfolio packs by overweighting recent years while punishing catastrophic full-year losses.",
        "",
        f"- Scenario: `{args.scenario}`",
        f"- Data dir: `{Path(args.data_dir).resolve()}`",
        f"- Search root: `{Path(args.search_root).resolve()}`",
        f"- Years scored: `{', '.join(str(year) for year in args.years)}`",
        f"- Minimum unique days per usable full-year window: `{args.min_days}`",
        "",
        "## Instrument Coverage By Year",
        "",
        markdown_table(coverage_frame),
        "",
        "## Top Portfolios",
        "",
        markdown_table(
            top[
                [
                    "rank",
                    "label",
                    "objective",
                    "recent_avg_roi_pct",
                    "recent_avg_sharpe",
                    "recent_avg_win_rate_pct",
                    "recent_avg_profit_factor",
                    "worst_year_roi_pct",
                    "worst_year_drawdown_pct",
                    "catastrophic_years",
                    "usable_years",
                    "module_count",
                ]
            ]
        ),
        "",
    ]

    if not top.empty:
        best_label = str(top.iloc[0]["label"])
        best_years = yearly_metrics.loc[yearly_metrics["label"] == best_label].copy()
        report_lines.extend(
            [
                "## Best Portfolio Year Table",
                "",
                markdown_table(
                    best_years[
                        [
                            "year",
                            "roi_pct",
                            "sharpe_ann",
                            "max_drawdown_pct",
                            "win_rate_pct",
                            "profit_factor",
                            "trade_count",
                            "usable_window",
                            "module_count",
                        ]
                    ]
                ),
                "",
                "## Best Portfolio Modules",
                "",
                *[f"- `{module}`" for module in json.loads(str(top.iloc[0]["modules_json"]))],
                "",
            ]
        )
    return "\n".join(report_lines)


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir).expanduser().resolve()
    search_root = Path(args.search_root).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    portfolios, duplicate_count = discover_portfolios(search_root)
    if not portfolios:
        raise SystemExit(f"No portfolio packs discovered under {search_root}")

    all_instruments = sorted(
        {
            str(row["instrument"]).strip().lower()
            for portfolio in portfolios
            for row in portfolio["rows"]
        }
        | REQUIRED_CONVERSION_INSTRUMENTS
    )
    datasets = load_datasets(data_dir, all_instruments)

    coverage_rows: list[dict[str, Any]] = []
    day_counts_by_instrument: dict[str, dict[int, int]] = {}
    for instrument in all_instruments:
        counts = annual_day_counts(datasets[instrument])
        day_counts_by_instrument[instrument] = counts
        coverage_row = {"instrument": instrument}
        for year in args.years:
            coverage_row[f"days_{year}"] = int(counts.get(year, 0))
        coverage_rows.append(coverage_row)
    coverage_frame = pd.DataFrame(coverage_rows).sort_values("instrument").reset_index(drop=True)
    coverage_frame.to_csv(output_dir / "instrument_year_coverage.csv", index=False)

    year_weights = {int(year): float(DEFAULT_YEAR_WEIGHTS.get(int(year), 1.0)) for year in args.years}
    module_rows: dict[str, dict[str, Any]] = {}
    for portfolio in portfolios:
        for row in portfolio["rows"]:
            module_rows.setdefault(str(row["module"]), row)

    window_datasets: dict[int, dict[str, dict[str, object]]] = {}
    for year in args.years:
        start = pd.Timestamp(year=year, month=1, day=1, tz="UTC")
        end = pd.Timestamp(year=year + 1, month=1, day=1, tz="UTC")
        window_datasets[year] = {
            instrument: filter_dataset_window(data, start, end)
            for instrument, data in datasets.items()
        }

    module_trade_cache: dict[tuple[int, str], pd.DataFrame] = {}
    for year in args.years:
        for module_name, row in module_rows.items():
            instrument = str(row["instrument"])
            if day_counts_by_instrument[instrument].get(year, 0) < args.min_days:
                module_trade_cache[(year, module_name)] = pd.DataFrame()
                continue
            module_trade_cache[(year, module_name)] = module_trades_for_row(
                row,
                window_datasets[year][instrument],
                args.scenario,
            )

    yearly_rows: list[dict[str, Any]] = []
    rankings_rows: list[dict[str, Any]] = []
    for portfolio in portfolios:
        modules = list(portfolio["modules"])
        instruments = sorted({str(row["instrument"]) for row in portfolio["rows"]} | REQUIRED_CONVERSION_INSTRUMENTS)

        portfolio_year_rows: list[dict[str, Any]] = []
        for year in args.years:
            min_days = min(day_counts_by_instrument[instrument].get(year, 0) for instrument in instruments)
            usable_window = min_days >= args.min_days

            if usable_window:
                frames = [module_trade_cache[(year, module)] for module in modules]
                nonempty_frames = [frame for frame in frames if not frame.empty]
                if nonempty_frames:
                    trade_frame = (
                        pd.concat(nonempty_frames, ignore_index=True)
                        .sort_values(["entry_time", "module"])
                        .reset_index(drop=True)
                    )
                else:
                    trade_frame = pd.DataFrame()

                dataset_subset = {instrument: window_datasets[year][instrument] for instrument in instruments}
                summary, _, _, _, _, filled_df = simulate_portfolio(
                    trade_frame,
                    dataset_subset,
                    args.start_balance,
                    args.risk_pct / 100.0,
                    args.max_leverage,
                    args.max_concurrent,
                )
                trade_stats = build_trade_stats(filled_df)
                summary_row = summary.iloc[0]
                trade_row = trade_stats.iloc[0]

                row = {
                    "label": portfolio["label"],
                    "year": int(year),
                    "usable_window": True,
                    "min_instrument_days": int(min_days),
                    "roi_pct": float(summary_row["roi_pct"]),
                    "sharpe_ann": float(summary_row["calendar_weekly_sharpe_ann"]),
                    "sortino_ann": float(summary_row["calendar_weekly_sortino_ann"]),
                    "max_drawdown_pct": float(summary_row["max_drawdown_pct"]),
                    "win_rate_pct": float(summary_row["portfolio_win_rate_pct"]),
                    "profit_factor": float(summary_row["portfolio_profit_factor"]),
                    "trade_count": int(summary_row["portfolio_trades"]),
                    "avg_trade_dollars": float(trade_row["avg_pnl_dollars"]),
                    "module_count": int(len(modules)),
                    "modules_json": json.dumps(modules),
                }
                row["year_score"] = year_score(row)
            else:
                row = {
                    "label": portfolio["label"],
                    "year": int(year),
                    "usable_window": False,
                    "min_instrument_days": int(min_days),
                    "roi_pct": float("nan"),
                    "sharpe_ann": float("nan"),
                    "sortino_ann": float("nan"),
                    "max_drawdown_pct": float("nan"),
                    "win_rate_pct": float("nan"),
                    "profit_factor": float("nan"),
                    "trade_count": 0,
                    "avg_trade_dollars": float("nan"),
                    "module_count": int(len(modules)),
                    "modules_json": json.dumps(modules),
                    "year_score": float("nan"),
                }
            portfolio_year_rows.append(row)
            yearly_rows.append(row)

        portfolio_year_frame = pd.DataFrame(portfolio_year_rows)
        aggregate = aggregate_portfolio_score(portfolio_year_frame, year_weights)
        rankings_rows.append(
            {
                "label": portfolio["label"],
                "path": str(portfolio["path"]),
                "module_count": len(modules),
                "modules_json": json.dumps(modules),
                **aggregate,
            }
        )

    yearly_metrics = pd.DataFrame(yearly_rows).sort_values(["label", "year"]).reset_index(drop=True)
    rankings = (
        pd.DataFrame(rankings_rows)
        .sort_values(["objective", "recent_avg_sharpe", "recent_avg_roi_pct"], ascending=False)
        .reset_index(drop=True)
    )
    rankings.insert(0, "rank", np.arange(1, len(rankings) + 1))

    yearly_metrics.to_csv(output_dir / "portfolio_yearly_metrics.csv", index=False)
    rankings.to_csv(output_dir / "portfolio_rankings.csv", index=False)

    report = build_report(rankings, yearly_metrics, coverage_frame, args.top_n, args)
    (output_dir / "audit_report.md").write_text(report, encoding="utf-8")

    manifest = {
        "data_dir": str(data_dir),
        "search_root": str(search_root),
        "output_dir": str(output_dir),
        "scenario": args.scenario,
        "years": [int(year) for year in args.years],
        "min_days": int(args.min_days),
        "discovered_portfolios": int(len(portfolios)),
        "deduplicated_duplicates": int(duplicate_count),
        "unique_modules": int(len(module_rows)),
    }
    (output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(output_dir)


if __name__ == "__main__":
    main()
