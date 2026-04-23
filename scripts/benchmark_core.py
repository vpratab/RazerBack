from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from native_acceleration import (
    FXBACKTEST_CORE_AVAILABLE,
    hawkes_intensity_accelerated,
    hawkes_intensity_reference,
    simulate_trade_path_accelerated,
    simulate_trade_path_reference,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark the locked portfolio runtime with and without Rust acceleration.")
    parser.add_argument("--data-dir", default="C:/fx_data/m1")
    parser.add_argument("--config", default=str(REPO_ROOT / "configs" / "optimal_portfolio.json"))
    parser.add_argument("--output-prefix", default=str(REPO_ROOT / "output" / "benchmark"))
    return parser.parse_args()


def run_once(repo_root: Path, config: str, data_dir: str, output_dir: str, disable_rust: bool) -> float:
    env = os.environ.copy()
    if disable_rust:
        env["FXBACKTEST_DISABLE_RUST"] = "1"
    else:
        env.pop("FXBACKTEST_DISABLE_RUST", None)

    started = time.perf_counter()
    subprocess.run(
        [
            sys.executable,
            str(repo_root / "run_locked_portfolio.py"),
            "--config",
            config,
            "--data-dir",
            data_dir,
            "--output-dir",
            output_dir,
            "--scenario",
            "base",
        ],
        check=True,
        cwd=repo_root,
        env=env,
    )
    return time.perf_counter() - started


def benchmark_hawkes() -> tuple[float, float, float]:
    rng = np.random.default_rng(123)
    shocks = np.abs(rng.normal(0.0, 1.0, size=200_000)).astype(np.float64)
    alpha = 0.15
    loops = 100

    started = time.perf_counter()
    for _ in range(loops):
        hawkes_intensity_reference(shocks, alpha)
    python_seconds = time.perf_counter() - started

    started = time.perf_counter()
    for _ in range(loops):
        hawkes_intensity_accelerated(shocks, alpha)
    rust_seconds = time.perf_counter() - started
    speedup = python_seconds / rust_seconds if rust_seconds > 0 else 0.0
    return python_seconds, rust_seconds, speedup


def benchmark_trade_path() -> tuple[float, float, float]:
    rng = np.random.default_rng(456)
    loops = 25_000
    length = 240
    base_price = 1.1250
    increments = rng.normal(0.0, 0.00015, size=length)
    close = base_price + np.cumsum(increments)
    high = close + np.abs(rng.normal(0.00020, 0.00005, size=length))
    low = close - np.abs(rng.normal(0.00020, 0.00005, size=length))
    open_ = np.concatenate(([base_price], close[:-1]))
    entry_price = float(open_[0])
    stop_loss = entry_price - 0.0025
    ladder = [0.0010, 0.0020, 0.0030]
    fractions = [0.3, 0.3, 0.3]
    trail_delta = 0.0015
    ttl_bars = length - 1

    started = time.perf_counter()
    for _ in range(loops):
        simulate_trade_path_reference(open_, high, low, close, entry_price, stop_loss, ladder, fractions, trail_delta, ttl_bars, "long")
    python_seconds = time.perf_counter() - started

    started = time.perf_counter()
    for _ in range(loops):
        simulate_trade_path_accelerated(open_, high, low, close, entry_price, stop_loss, ladder, fractions, trail_delta, ttl_bars, "long")
    rust_seconds = time.perf_counter() - started
    speedup = python_seconds / rust_seconds if rust_seconds > 0 else 0.0
    return python_seconds, rust_seconds, speedup


def build_lines(
    *,
    full_python_seconds: float | None,
    full_rust_seconds: float | None,
    full_error: str | None,
    hawkes_metrics: tuple[float, float, float] | None,
    trade_metrics: tuple[float, float, float] | None,
) -> list[str]:
    lines = [
        "# Benchmark",
        "",
        f"- Interpreter: `{sys.executable}`",
        f"- Rust extension available in current environment: `{FXBACKTEST_CORE_AVAILABLE}`",
        "",
        "## Hot Path Microbenchmarks",
    ]
    if hawkes_metrics is not None:
        lines.extend(
            [
                f"- Hawkes Python fallback: `{hawkes_metrics[0]:.4f}s`",
                f"- Hawkes Rust-enabled: `{hawkes_metrics[1]:.4f}s`",
                f"- Hawkes speedup: `{hawkes_metrics[2]:.2f}x`",
            ]
        )
    if trade_metrics is not None:
        lines.extend(
            [
                f"- Trade-path Python fallback: `{trade_metrics[0]:.4f}s`",
                f"- Trade-path Rust-enabled: `{trade_metrics[1]:.4f}s`",
                f"- Trade-path speedup: `{trade_metrics[2]:.2f}x`",
            ]
        )
    lines.extend(
        [
            "",
            "## Full Run",
        ]
    )
    if full_error:
        lines.extend(
            [
                f"- Full backtest benchmark failed: `{full_error}`",
                "- Ensure the midpoint parquet files are enriched before rerunning this benchmark.",
            ]
        )
    else:
        lines.extend(
            [
                f"- Full backtest Python fallback: `{full_python_seconds:.2f}s`",
                f"- Full backtest Rust-enabled: `{full_rust_seconds:.2f}s`",
                f"- Full backtest speedup: `{(full_python_seconds / full_rust_seconds) if full_rust_seconds and full_rust_seconds > 0 else 0.0:.2f}x`",
            ]
        )
    lines.extend(
        [
            "",
            "## Notes",
            "- The trade-path benchmark is the cleanest measure of the Rust hot path because it isolates repeated exit simulation work.",
            "- The full-run benchmark reflects current sample data and the public runtime structure, so end-to-end speedup is usually smaller than the microbenchmark speedup.",
            "- `rolling_gmm_nodes` is currently routed through the Rust extension with a parity-safe bridge to the Python reference logic.",
        ]
    )
    return lines


def main() -> None:
    args = parse_args()
    full_python_seconds: float | None = None
    full_rust_seconds: float | None = None
    full_error: str | None = None
    hawkes_metrics: tuple[float, float, float] | None = None
    trade_metrics: tuple[float, float, float] | None = None

    if FXBACKTEST_CORE_AVAILABLE:
        hawkes_metrics = benchmark_hawkes()
        trade_metrics = benchmark_trade_path()

    try:
        full_python_seconds = run_once(REPO_ROOT, args.config, args.data_dir, f"{args.output_prefix}_python", disable_rust=True)
        full_rust_seconds = run_once(REPO_ROOT, args.config, args.data_dir, f"{args.output_prefix}_rust", disable_rust=False)
    except subprocess.CalledProcessError as exc:
        full_error = str(exc)

    lines = build_lines(
        full_python_seconds=full_python_seconds,
        full_rust_seconds=full_rust_seconds,
        full_error=full_error,
        hawkes_metrics=hawkes_metrics,
        trade_metrics=trade_metrics,
    )
    benchmark_path = REPO_ROOT / "BENCHMARK.md"
    benchmark_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(benchmark_path)


if __name__ == "__main__":
    main()
