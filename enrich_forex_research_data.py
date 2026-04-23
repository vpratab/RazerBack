from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from native_acceleration import hawkes_intensity_accelerated, rolling_gmm_nodes_accelerated
from realistic_backtest import PIP_SIZES, load_dataset_bundle


REQUIRED_OUTPUT_COLUMNS = [
    "ask_lambda",
    "bid_lambda",
    "imbalance_ratio",
    "node_mean",
    "distance_to_node_pips",
    "local_sigma",
]

GMM_COMPONENTS = 4
GMM_LOOKBACK_HOURS = 720
GMM_REFIT_HOURS = 4
LOCAL_SIGMA_BARS = 240  # 4 hours on M1


def main() -> None:
    parser = argparse.ArgumentParser(description="Enrich OANDA M1 parquet files with Hawkes, node, and local sigma columns.")
    parser.add_argument("--data-dir", required=True)
    parser.add_argument(
        "--instruments",
        nargs="+",
        default=["eurusd", "gbpusd", "usdjpy", "gbpjpy"],
        choices=sorted(PIP_SIZES.keys()),
    )
    parser.add_argument("--hawkes-alpha", type=float, default=0.15)
    parser.add_argument("--gmm-components", type=int, default=GMM_COMPONENTS)
    parser.add_argument("--gmm-lookback-hours", type=int, default=GMM_LOOKBACK_HOURS)
    parser.add_argument("--gmm-refit-hours", type=int, default=GMM_REFIT_HOURS)
    args = parser.parse_args()

    data_dir = Path(args.data_dir).expanduser().resolve()
    datasets = load_dataset_bundle(data_dir, args.instruments)

    for instrument in args.instruments:
        path = data_dir / f"{instrument}_5yr_m1.parquet"
        frame = pd.read_parquet(path)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.sort_values("timestamp").reset_index(drop=True)
        enriched = enrich_instrument(
            datasets[instrument],
            hawkes_alpha=args.hawkes_alpha,
            gmm_components=args.gmm_components,
            gmm_lookback_hours=args.gmm_lookback_hours,
            gmm_refit_hours=args.gmm_refit_hours,
        )
        for column in REQUIRED_OUTPUT_COLUMNS:
            frame[column] = enriched[column]
        frame.to_parquet(path, index=False)
        print(path)


def enrich_instrument(
    data: dict[str, object],
    hawkes_alpha: float,
    gmm_components: int,
    gmm_lookback_hours: int,
    gmm_refit_hours: int,
) -> pd.DataFrame:
    timestamp = pd.to_datetime(data["timestamp"], utc=True)
    bid_close = np.asarray(data["bid"]["close"], dtype=np.float64)
    ask_close = np.asarray(data["ask"]["close"], dtype=np.float64)
    mid_close = np.asarray(data["mid"]["close"], dtype=np.float64)
    pip = float(data["pip"])

    ask_diff_pips = np.diff(ask_close, prepend=ask_close[0]) / pip
    bid_diff_pips = np.diff(bid_close, prepend=bid_close[0]) / pip
    ask_shock = np.clip(ask_diff_pips, a_min=0.0, a_max=None)
    bid_shock = np.clip(-bid_diff_pips, a_min=0.0, a_max=None)

    ask_lambda = hawkes_intensity(ask_shock, hawkes_alpha)
    bid_lambda = hawkes_intensity(bid_shock, hawkes_alpha)
    weaker = np.minimum(ask_lambda, bid_lambda)
    stronger = np.maximum(ask_lambda, bid_lambda)
    imbalance_ratio = np.where(weaker > 1e-12, stronger / weaker, np.inf)

    local_sigma = pd.Series(mid_close).rolling(LOCAL_SIGMA_BARS).std(ddof=0).to_numpy()
    node_mean = rolling_hourly_gmm_nodes(
        timestamp=timestamp,
        mid_close=mid_close,
        components=gmm_components,
        lookback_hours=gmm_lookback_hours,
        refit_hours=gmm_refit_hours,
    )
    distance_to_node_pips = np.abs(mid_close - node_mean) / pip

    return pd.DataFrame(
        {
            "ask_lambda": ask_lambda,
            "bid_lambda": bid_lambda,
            "imbalance_ratio": imbalance_ratio,
            "node_mean": node_mean,
            "distance_to_node_pips": distance_to_node_pips,
            "local_sigma": local_sigma,
        }
    )


def hawkes_intensity(shocks: np.ndarray, alpha: float) -> np.ndarray:
    return hawkes_intensity_accelerated(shocks, alpha)


def rolling_hourly_gmm_nodes(
    timestamp: pd.Series,
    mid_close: np.ndarray,
    components: int,
    lookback_hours: int,
    refit_hours: int,
) -> np.ndarray:
    return rolling_gmm_nodes_accelerated(timestamp, mid_close, components, lookback_hours, refit_hours)


if __name__ == "__main__":
    main()
