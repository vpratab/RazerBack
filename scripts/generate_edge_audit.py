from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = Path("C:/fx_data/m1")
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "institutional_edge_audit"
CORE_PAIRS = [
    "eurusd",
    "gbpusd",
    "usdjpy",
    "audusd",
    "usdcad",
    "usdchf",
    "eurjpy",
    "eurgbp",
    "eurchf",
    "audjpy",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate an honest institutional-readiness audit from the current FX research surface."
    )
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--recent-start-year", type=int, default=2020)
    parser.add_argument("--recent-end-year", type=int, default=2025)
    return parser.parse_args()


def coverage_status(recent_days: int) -> str:
    if recent_days <= 0:
        return "none"
    if recent_days < 10:
        return "token"
    if recent_days < 60:
        return "thin"
    if recent_days < 250:
        return "partial"
    return "usable"


def audit_recent_coverage(data_dir: Path, start_year: int, end_year: int) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for pair in CORE_PAIRS:
        path = data_dir / f"{pair}_5yr_m1_bid_ask.parquet"
        if not path.exists():
            rows.append(
                {
                    "pair": pair,
                    "total_rows": 0,
                    "unique_days": 0,
                    "recent_rows": 0,
                    "recent_unique_days": 0,
                    "recent_years_present": 0,
                    "recent_status": "missing",
                }
            )
            continue

        timestamps = pd.to_datetime(pd.read_parquet(path, columns=["timestamp"])["timestamp"], utc=True)
        years = timestamps.dt.year
        day_keys = timestamps.dt.normalize()
        recent_mask = (years >= start_year) & (years <= end_year)

        row: dict[str, object] = {
            "pair": pair,
            "total_rows": int(len(timestamps)),
            "unique_days": int(day_keys.nunique()),
            "recent_rows": int(recent_mask.sum()),
            "recent_unique_days": int(day_keys[recent_mask].nunique()),
            "recent_years_present": int(years[recent_mask].nunique()),
        }
        for year in range(start_year, end_year + 1):
            row[f"rows_{year}"] = int((years == year).sum())
        row["recent_status"] = coverage_status(int(row["recent_unique_days"]))
        rows.append(row)

    return pd.DataFrame(rows).sort_values("pair").reset_index(drop=True)


def safe_read_csv(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def build_seed_shortlist(repo_root: Path, coverage: pd.DataFrame) -> pd.DataFrame:
    shortlist_frames: list[pd.DataFrame] = []

    candidate_pool = safe_read_csv(repo_root / "output" / "portfolio_factory_research" / "candidate_pool.csv")
    if not candidate_pool.empty:
        top_candidates = (
            candidate_pool.sort_values(["heuristic", "sharpe_ann", "roi_pct"], ascending=False)
            .head(12)
            .copy()
        )
        top_candidates["source_table"] = "portfolio_factory_candidates"
        shortlist_frames.append(
            top_candidates[
                [
                    "source_table",
                    "module",
                    "instrument",
                    "family",
                    "side",
                    "roi_pct",
                    "sharpe_ann",
                    "max_drawdown_pct",
                    "win_rate_pct",
                    "profit_factor",
                    "trades",
                    "trades_per_week",
                    "last_trade_gap_years",
                ]
            ]
        )

    vsentinel = safe_read_csv(repo_root / "output" / "v_sentinel_module_scan.csv")
    if not vsentinel.empty:
        if "family" not in vsentinel.columns:
            vsentinel["family"] = "v_sentinel"
        top_vsentinel = (
            vsentinel.loc[(vsentinel["trades"] >= 8) & ((vsentinel["roi_pct"] > 0.0) | (vsentinel["sharpe_ann"] > 0.0))]
            .sort_values(["sharpe_ann", "roi_pct"], ascending=False)
            .head(6)
            .copy()
        )
        if not top_vsentinel.empty:
            top_vsentinel["source_table"] = "v_sentinel_module_scan"
            shortlist_frames.append(
                top_vsentinel[
                    [
                        "source_table",
                        "module",
                        "instrument",
                        "family",
                        "side",
                        "roi_pct",
                        "sharpe_ann",
                        "max_drawdown_pct",
                        "win_rate_pct",
                        "profit_factor",
                        "trades",
                    ]
                ]
            )

    refined_combo = safe_read_csv(repo_root / "output" / "refined_usdjpy_combo_scan.csv")
    if not refined_combo.empty:
        top_combo = refined_combo.sort_values("score", ascending=False).head(8).copy()
        top_combo["source_table"] = "refined_usdjpy_combo_scan"
        top_combo["instrument"] = "multi"
        top_combo["family"] = "mixed"
        top_combo["side"] = "mixed"
        top_combo["module"] = top_combo["combo"]
        top_combo["trades_per_week"] = top_combo["trades_per_active_week"]
        shortlist_frames.append(
            top_combo[
                [
                    "source_table",
                    "module",
                    "instrument",
                    "family",
                    "side",
                    "roi_pct",
                    "sharpe_ann",
                    "max_drawdown_pct",
                    "win_rate_pct",
                    "profit_factor",
                    "trades",
                    "trades_per_week",
                ]
            ]
        )

    if not shortlist_frames:
        return pd.DataFrame()

    shortlist = pd.concat(shortlist_frames, ignore_index=True, sort=False)
    coverage_cols = coverage[["pair", "recent_rows", "recent_unique_days", "recent_years_present", "recent_status"]].copy()
    coverage_cols = coverage_cols.rename(columns={"pair": "instrument"})
    shortlist["instrument"] = shortlist["instrument"].astype(str).str.lower()
    shortlist = shortlist.merge(coverage_cols, on="instrument", how="left")
    shortlist["recent_rows"] = shortlist["recent_rows"].fillna(0).astype(int)
    shortlist["recent_unique_days"] = shortlist["recent_unique_days"].fillna(0).astype(int)
    shortlist["recent_years_present"] = shortlist["recent_years_present"].fillna(0).astype(int)
    shortlist["recent_status"] = shortlist["recent_status"].fillna("mixed")
    shortlist["institutional_gate"] = shortlist.apply(classify_shortlist_row, axis=1)
    return shortlist


def classify_shortlist_row(row: pd.Series) -> str:
    recent_days = int(row.get("recent_unique_days", 0))
    sharpe = float(row.get("sharpe_ann", 0.0))
    drawdown = abs(float(row.get("max_drawdown_pct", 0.0)))
    win_rate = float(row.get("win_rate_pct", 0.0))
    trades = float(row.get("trades", 0.0))

    if recent_days < 250 and str(row.get("instrument", "")) != "multi":
        return "blocked_recent_data"
    if trades < 20:
        return "thin_sample"
    if sharpe < 1.0 or drawdown > 10.0 or win_rate < 60.0:
        return "fails_target"
    return "review"


def read_top_portfolio(repo_root: Path) -> dict[str, object]:
    rankings = safe_read_csv(repo_root / "output" / "portfolio_factory_research" / "portfolio_rankings.csv")
    if rankings.empty:
        return {}
    row = rankings.iloc[0].to_dict()
    return row


def read_vsentinel_summary(repo_root: Path) -> dict[str, object]:
    summary = safe_read_csv(repo_root / "output" / "v_sentinel_full" / "summary.csv")
    if summary.empty:
        return {}
    return summary.iloc[0].to_dict()


def read_refined_seed(repo_root: Path) -> dict[str, object]:
    refined = safe_read_csv(repo_root / "output" / "refined_usdjpy_combo_scan.csv")
    if refined.empty:
        return {}
    return refined.iloc[0].to_dict()


def format_metric(value: object, digits: int = 2, suffix: str = "") -> str:
    try:
        numeric = float(value)
    except Exception:
        return "n/a"
    if pd.isna(numeric):
        return "n/a"
    return f"{numeric:.{digits}f}{suffix}"


def build_report(
    coverage: pd.DataFrame,
    shortlist: pd.DataFrame,
    top_portfolio: dict[str, object],
    refined_seed: dict[str, object],
    vsentinel_summary: dict[str, object],
    start_year: int,
    end_year: int,
) -> str:
    total_recent_rows = int(coverage["recent_rows"].sum()) if not coverage.empty else 0
    total_recent_days = int(coverage["recent_unique_days"].sum()) if not coverage.empty else 0
    year_row_totals = {
        year: int(coverage.get(f"rows_{year}", pd.Series(dtype="int64")).sum())
        for year in range(start_year, end_year + 1)
    }
    year_day_coverage = {}
    for year in range(start_year, end_year + 1):
        row_col = f"rows_{year}"
        if row_col in coverage.columns:
            year_day_coverage[year] = int((coverage[row_col] > 0).sum())
        else:
            year_day_coverage[year] = 0

    available_years = [year for year, total in year_row_totals.items() if total > 0]
    if available_years:
        year_coverage_text = ", ".join(
            f"`{year}`: {year_row_totals[year]} rows across {year_day_coverage[year]} pairs"
            for year in available_years
        )
    else:
        year_coverage_text = "none"

    if shortlist.empty:
        shortlist_text = "No shortlist rows available."
    else:
        shortlist_text = shortlist[
            [
                "source_table",
                "module",
                "instrument",
                "family",
                "side",
                "roi_pct",
                "sharpe_ann",
                "max_drawdown_pct",
                "win_rate_pct",
                "profit_factor",
                "trades",
                "recent_unique_days",
                "recent_status",
                "institutional_gate",
            ]
        ].head(20).to_string(index=False)

    lines = [
        "# Institutional Edge Audit",
        "",
        "## Bottom Line",
        "",
        f"- Recent M1 coverage across the core 10-pair universe in the `{start_year}-{end_year}` window: {year_coverage_text}.",
        f"- Recent data window audited: `{start_year}` through `{end_year}`.",
        f"- Aggregate recent rows across all pairs: `{total_recent_rows}`. Aggregate recent pair-days: `{total_recent_days}`.",
        "- The surface is now usable for a first modern-window check on 2024-2025, but it still cannot support an honest 'last 5-6 years' claim because 2020-2023 are absent.",
        "",
        "## Current Best Leads",
        "",
    ]

    if refined_seed:
        lines.extend(
            [
                f"- Best micro-edge seed: `{refined_seed.get('combo', 'n/a')}` with ROI `{format_metric(refined_seed.get('roi_pct'), suffix='%')}`, Sharpe `{format_metric(refined_seed.get('sharpe_ann'), 3)}`, max drawdown `{format_metric(refined_seed.get('max_drawdown_pct'), suffix='%')}`, win rate `{format_metric(refined_seed.get('win_rate_pct'), suffix='%')}`, trades `{int(refined_seed.get('trades', 0))}`.",
            ]
        )
    if top_portfolio:
        lines.extend(
            [
                f"- Best broad simultaneous portfolio: `{top_portfolio.get('label', 'n/a')}` with ROI `{format_metric(top_portfolio.get('roi_pct'), suffix='%')}`, Sharpe `{format_metric(top_portfolio.get('sharpe_ann'), 3)}`, max drawdown `{format_metric(top_portfolio.get('max_drawdown_pct'), suffix='%')}`, win rate `{format_metric(top_portfolio.get('win_rate_pct'), suffix='%')}`, trades `{int(float(top_portfolio.get('trade_count', 0) or 0))}`.",
                "- That portfolio is still not desk-ready because its score came from the old sparse sample and has not yet been revalidated on the newer 2024-2025 window.",
            ]
        )
    if vsentinel_summary:
        lines.extend(
            [
                f"- V-Sentinel status: rejected. Hard-scenario ROI `{format_metric(vsentinel_summary.get('roi_pct'), suffix='%')}`, Sharpe `{format_metric(vsentinel_summary.get('calendar_weekly_sharpe_ann'), 3)}`, max drawdown `{format_metric(vsentinel_summary.get('max_drawdown_pct'), suffix='%')}`, win rate `{format_metric(vsentinel_summary.get('portfolio_win_rate_pct'), suffix='%')}`.",
            ]
        )

    lines.extend(
        [
            "",
            "## Recent Coverage By Pair",
            "",
            "```text",
            coverage[
                [
                    "pair",
                    "recent_rows",
                    "recent_unique_days",
                    "recent_years_present",
                    "recent_status",
                    f"rows_{start_year}",
                    f"rows_{end_year}",
                ]
            ].to_string(index=False),
            "```",
            "",
            "## Shortlist With Institutional Gate",
            "",
            "```text",
            shortlist_text,
            "```",
            "",
            "## Readout",
            "",
            "- The strongest surviving idea on the current machine is still the refined USDJPY short breakout cluster, not the pair-symmetric V-Sentinel table.",
            "- We now have enough 2024-2025 surface to start a modern-window reality check, but not enough to claim a 5-6 year institutional record.",
            "- The next honest path is to rerun the shortlist and portfolio search on the current 2024-2025 window while the downloader keeps filling 2023 and earlier modern years.",
        ]
    )

    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    data_dir = Path(args.data_dir).expanduser().resolve()
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    coverage = audit_recent_coverage(data_dir, args.recent_start_year, args.recent_end_year)
    shortlist = build_seed_shortlist(REPO_ROOT, coverage)
    top_portfolio = read_top_portfolio(REPO_ROOT)
    refined_seed = read_refined_seed(REPO_ROOT)
    vsentinel_summary = read_vsentinel_summary(REPO_ROOT)

    coverage.to_csv(output_dir / "recent_coverage.csv", index=False)
    shortlist.to_csv(output_dir / "seed_shortlist.csv", index=False)

    report = build_report(
        coverage=coverage,
        shortlist=shortlist,
        top_portfolio=top_portfolio,
        refined_seed=refined_seed,
        vsentinel_summary=vsentinel_summary,
        start_year=args.recent_start_year,
        end_year=args.recent_end_year,
    )
    (output_dir / "edge_audit.md").write_text(report, encoding="utf-8")
    print(output_dir)


if __name__ == "__main__":
    main()
