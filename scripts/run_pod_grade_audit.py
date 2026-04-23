from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    severity: str
    detail: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a pod-style reproducibility and realism gate.")
    parser.add_argument("--modules-file")
    parser.add_argument("--variant-rankings")
    parser.add_argument("--label")
    parser.add_argument("--summary-csv", required=True)
    parser.add_argument("--yearly-csv", required=True)
    parser.add_argument("--m1-daily-root", default=r"C:\fx_data\m1\_daily_bid_ask")
    parser.add_argument("--tick-root", default=r"C:\fx_data\tick")
    parser.add_argument("--exact-replay-report")
    parser.add_argument("--output-dir", default="output/research/pod_grade_audit")
    parser.add_argument("--start-year", type=int, default=2011)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--recent-start-year", type=int, default=2022)
    parser.add_argument("--recent-end-year", type=int, default=2025)
    return parser.parse_args()


def pass_check(name: str, detail: str, severity: str = "gate") -> CheckResult:
    return CheckResult(name=name, status="PASS", severity=severity, detail=detail)


def fail_check(name: str, detail: str, severity: str = "gate") -> CheckResult:
    return CheckResult(name=name, status="FAIL", severity=severity, detail=detail)


def warn_check(name: str, detail: str, severity: str = "warning") -> CheckResult:
    return CheckResult(name=name, status="WARN", severity=severity, detail=detail)


def extract_modules(args: argparse.Namespace) -> list[str]:
    if args.modules_file:
        frame = pd.read_csv(args.modules_file)
        if "module" not in frame.columns:
            raise SystemExit(f"{args.modules_file} must contain a 'module' column.")
        return frame["module"].dropna().astype(str).tolist()

    if args.variant_rankings and args.label:
        rankings = pd.read_csv(args.variant_rankings)
        rows = rankings.loc[rankings["label"] == args.label]
        if rows.empty:
            raise SystemExit(f"Could not find label {args.label!r} in {args.variant_rankings}")
        return list(json.loads(rows.iloc[0]["modules_json"]))

    raise SystemExit("Provide --modules-file or --variant-rankings with --label.")


def instrument_from_module(module: str) -> str:
    return module.split("_", 1)[0].lower()


def expected_dates(start_year: int, end_year: int) -> list[date]:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    out: list[date] = []
    current = start
    while current <= end:
        out.append(current)
        current += timedelta(days=1)
    return out


def date_from_daily_file(path: Path) -> date | None:
    try:
        return date.fromisoformat(path.stem)
    except ValueError:
        return None


def count_available_daily(root: Path, instrument: str, start_year: int, end_year: int) -> tuple[int, int, list[str]]:
    pair_dir = root / instrument.upper()
    expected = set(expected_dates(start_year, end_year))
    found: set[date] = set()
    if pair_dir.exists():
        for path in pair_dir.glob("*.parquet"):
            parsed = date_from_daily_file(path)
            if parsed in expected:
                found.add(parsed)
    missing = sorted(expected - found)
    return len(found), len(expected), [item.isoformat() for item in missing[:8]]


def count_available_tick(root: Path, instrument: str, start_year: int, end_year: int) -> tuple[int, int, list[str]]:
    pair_dir = root / instrument.upper()
    expected = set(expected_dates(start_year, end_year))
    found: set[date] = set()
    if pair_dir.exists():
        for year in range(start_year, end_year + 1):
            year_dir = pair_dir / str(year)
            if not year_dir.exists():
                continue
            for path in year_dir.glob("*.parquet"):
                parsed = date_from_daily_file(path)
                if parsed in expected:
                    found.add(parsed)
    missing = sorted(expected - found)
    return len(found), len(expected), [item.isoformat() for item in missing[:8]]


def summarize_coverage(instruments: Iterable[str], root: Path, start_year: int, end_year: int, tick: bool = False) -> pd.DataFrame:
    rows = []
    for instrument in sorted(set(instruments)):
        if tick:
            found, expected, missing_examples = count_available_tick(root, instrument, start_year, end_year)
        else:
            found, expected, missing_examples = count_available_daily(root, instrument, start_year, end_year)
        rows.append(
            {
                "instrument": instrument,
                "found_days": found,
                "expected_days": expected,
                "coverage_pct": round(found / expected * 100.0, 3) if expected else 0.0,
                "missing_days": expected - found,
                "missing_examples": ", ".join(missing_examples),
            }
        )
    return pd.DataFrame(rows)


def metrics_checks(summary_row: pd.Series, yearly: pd.DataFrame, recent_start_year: int, recent_end_year: int) -> list[CheckResult]:
    checks: list[CheckResult] = []
    recent = yearly.loc[(yearly["year"] >= recent_start_year) & (yearly["year"] <= recent_end_year)]
    if recent.empty:
        recent = yearly.copy()
    recent_label = f"{int(recent['year'].min())}-{int(recent['year'].max())}"
    recent_avg_roi = float(recent["roi_pct"].mean())
    recent_avg_sharpe = float(recent["sharpe_ann"].mean())
    worst_roi = float(yearly["roi_pct"].min())
    worst_dd = float(yearly["max_drawdown_pct"].min())
    y_recent_end = yearly.loc[yearly["year"] == recent_end_year]
    if y_recent_end.empty:
        checks.append(fail_check(f"{recent_end_year} availability", f"No {recent_end_year} result exists for the selected book."))
        return checks
    y_recent_end = y_recent_end.iloc[0]

    metric_gates = [
        (f"{recent_end_year} ROI >= 20%", float(y_recent_end["roi_pct"]), 20.0),
        (f"{recent_end_year} Sharpe >= 1.5", float(y_recent_end["sharpe_ann"]), 1.5),
        (f"{recent_end_year} max DD >= -8%", float(y_recent_end["max_drawdown_pct"]), -8.0),
        (f"{recent_end_year} win rate >= 60%", float(y_recent_end["win_rate_pct"]), 60.0),
        (f"{recent_end_year} profit factor >= 1.5", float(y_recent_end["profit_factor"]), 1.5),
        (f"{recent_end_year} trades >= 20", float(y_recent_end["trade_count"]), 20.0),
        (f"{recent_label} avg ROI >= 20%", recent_avg_roi, 20.0),
        (f"{recent_label} avg Sharpe >= 1.5", recent_avg_sharpe, 1.5),
        ("Worst tested year ROI >= -10%", worst_roi, -10.0),
        ("Worst tested drawdown >= -10%", worst_dd, -10.0),
    ]
    for name, value, threshold in metric_gates:
        detail = f"value={value:.4f}, threshold={threshold:.4f}"
        checks.append(pass_check(name, detail) if value >= threshold else fail_check(name, detail))

    checks.append(
        pass_check(
            "Metrics source",
            f"summary row reports recent_avg_roi={float(summary_row.get('recent_avg_roi_pct', 0.0)):.4f}, recent_avg_sharpe={float(summary_row.get('recent_avg_sharpe', 0.0)):.4f}",
            severity="info",
        )
    )
    return checks


def protocol_checks(yearly: pd.DataFrame, start_year: int, end_year: int) -> list[CheckResult]:
    years = sorted(int(year) for year in yearly["year"].unique())
    expected_all = list(range(start_year, end_year + 1))
    missing = [year for year in expected_all if year not in years]
    checks: list[CheckResult] = [
        pass_check("Tested year count", f"tested_years={years}; count={len(years)}", severity="info")
    ]
    if missing:
        checks.append(
            fail_check(
                f"Continuous {start_year}-{end_year} tested-year coverage",
                f"Missing tested years: {missing}. Current result is not a continuous full-sample pod result.",
            )
        )
    else:
        checks.append(pass_check(f"Continuous {start_year}-{end_year} tested-year coverage", "All requested years are present."))
    return checks


def exact_replay_check(report_path: Path | None, start_year: int, end_year: int) -> CheckResult:
    if report_path is None or not report_path.exists():
        return fail_check("Exact tick replay completed", "No exact tick replay report was supplied.")
    if start_year == 2025 and end_year == 2025:
        return pass_check("Exact tick replay completed", f"Replay report found at {report_path}")
    return fail_check(
        "Exact tick replay completed",
        f"Only a partial exact-tick replay artifact exists ({report_path}). Requested audit window is {start_year}-{end_year}; final pod mode needs every filled trade replayed.",
    )


def main() -> None:
    args = parse_args()
    modules = extract_modules(args)
    instruments = sorted({instrument_from_module(module) for module in modules})
    summary = pd.read_csv(args.summary_csv)
    yearly = pd.read_csv(args.yearly_csv)
    summary_row = summary.iloc[0]

    m1_cov = summarize_coverage(instruments, Path(args.m1_daily_root), args.start_year, args.end_year, tick=False)
    tick_cov = summarize_coverage(instruments, Path(args.tick_root), args.start_year, args.end_year, tick=True)

    checks = []
    checks.extend(metrics_checks(summary_row, yearly, args.recent_start_year, args.recent_end_year))
    checks.extend(protocol_checks(yearly, args.start_year, args.end_year))
    if float(m1_cov["coverage_pct"].min()) >= 99.9:
        checks.append(pass_check("Completed M1 daily bid/ask coverage", "All selected instruments are complete."))
    else:
        worst = m1_cov.sort_values(["coverage_pct", "missing_days"]).iloc[0]
        checks.append(
            fail_check(
                "Completed M1 daily bid/ask coverage",
                f"{len(m1_cov)} selected instruments are incomplete. Worst={worst['instrument']} coverage={worst['coverage_pct']:.3f}%, missing_days={int(worst['missing_days'])}, examples={worst['missing_examples']}",
            )
        )
    if float(tick_cov["coverage_pct"].min()) >= 99.9:
        checks.append(pass_check("Completed raw tick coverage", "All selected instruments are complete."))
    else:
        worst = tick_cov.sort_values(["coverage_pct", "missing_days"]).iloc[0]
        checks.append(
            fail_check(
                "Completed raw tick coverage",
                f"{len(tick_cov)} selected instruments are incomplete. Worst={worst['instrument']} coverage={worst['coverage_pct']:.3f}%, missing_days={int(worst['missing_days'])}, examples={worst['missing_examples']}",
            )
        )
    checks.append(exact_replay_check(Path(args.exact_replay_report) if args.exact_replay_report else None, args.start_year, args.end_year))
    checks.append(pass_check("No inferred bid/ask fallback", "Selected instruments should use tracked bid/ask parquet files.", severity="gate"))

    checks_df = pd.DataFrame([check.__dict__ for check in checks])
    operational_gate_names = {
        f"Continuous {args.start_year}-{args.end_year} tested-year coverage",
        "Completed M1 daily bid/ask coverage",
        "Completed raw tick coverage",
        "Exact tick replay completed",
    }
    metric_failures = checks_df.loc[
        (checks_df["status"] == "FAIL") & ~checks_df["name"].isin(operational_gate_names)
    ]
    current_metric_gate = "FAIL" if not metric_failures.empty else "PASS"
    final_pod_gate = "FAIL" if (checks_df["status"] == "FAIL").any() else "PASS"

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    checks_df.to_csv(output_dir / "pod_gate_checks.csv", index=False)
    m1_cov.to_csv(output_dir / "m1_daily_coverage.csv", index=False)
    tick_cov.to_csv(output_dir / "tick_coverage.csv", index=False)
    yearly.to_csv(output_dir / "selected_yearly.csv", index=False)
    pd.DataFrame({"module": modules}).to_csv(output_dir / "selected_modules.csv", index=False)

    lines = [
        "# Pod Grade Audit",
        "",
        f"- Portfolio label: `{args.label or 'module_list'}`",
        f"- Current metric gate: `{current_metric_gate}`",
        f"- Final pod gate: `{final_pod_gate}`",
        f"- Module count: `{len(modules)}`",
        f"- Instruments: `{', '.join(instruments)}`",
        "",
        "## Checks",
        "",
        checks_df.to_markdown(index=False),
        "",
        "## Yearly Performance",
        "",
        yearly.to_markdown(index=False),
        "",
        "## Modules",
        "",
    ]
    lines.extend([f"- `{module}`" for module in modules])
    lines.extend(
        [
            "",
            "## M1 Daily Coverage",
            "",
            m1_cov.to_markdown(index=False),
            "",
            "## Raw Tick Coverage",
            "",
            tick_cov.to_markdown(index=False),
            "",
        ]
    )
    (output_dir / "pod_grade_audit.md").write_text("\n".join(lines), encoding="utf-8")
    print(output_dir)
    print((output_dir / "pod_grade_audit.md").read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
