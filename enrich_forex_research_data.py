from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture

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
    out = np.zeros(len(shocks), dtype=np.float64)
    current = 0.0
    decay = float(np.exp(-alpha))
    for idx, shock in enumerate(shocks):
        current = current * decay + float(shock)
        out[idx] = current
    return out


def rolling_hourly_gmm_nodes(
    timestamp: pd.Series,
    mid_close: np.ndarray,
    components: int,
    lookback_hours: int,
    refit_hours: int,
) -> np.ndarray:
    frame = pd.DataFrame({"timestamp": timestamp, "mid_close": mid_close})
    hourly = (
        frame.set_index("timestamp")
        .resample("1H")
        .agg(mid_close=("mid_close", "last"))
        .dropna()
        .reset_index()
    )

    node_by_hour = np.full(len(hourly), np.nan, dtype=np.float64)
    values = hourly["mid_close"].to_numpy(dtype=np.float64)
    refit_step = max(1, refit_hours)

    for idx in range(len(hourly)):
        if idx < lookback_hours:
            continue
        if idx % refit_step != 0 and np.isfinite(node_by_hour[idx - 1]):
            node_by_hour[idx] = node_by_hour[idx - 1]
            continue

        sample = values[idx - lookback_hours : idx].reshape(-1, 1)
        model = GaussianMixture(n_components=components, covariance_type="full", random_state=42)
        model.fit(sample)
        means = np.sort(model.means_.flatten())
        current_price = values[idx]
        nearest_idx = int(np.argmin(np.abs(means - current_price)))
        node_by_hour[idx] = means[nearest_idx]

    node_hourly = pd.DataFrame({"timestamp": hourly["timestamp"], "node_mean": node_by_hour})
    node_hourly["node_mean"] = node_hourly["node_mean"].ffill().bfill()
    merged = (
        frame.assign(hour=lambda df: df["timestamp"].dt.floor("1H"))
        .merge(
            node_hourly.rename(columns={"timestamp": "hour"}),
            on="hour",
            how="left",
        )
        .drop(columns=["hour"])
    )
    merged["node_mean"] = merged["node_mean"].ffill().bfill()
    return merged["node_mean"].to_numpy(dtype=np.float64)


if __name__ == "__main__":
    main()
