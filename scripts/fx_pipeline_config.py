from __future__ import annotations

from pathlib import Path


DATA_ROOT = Path("C:/fx_data")
TICK_ROOT = DATA_ROOT / "tick"
M1_ROOT = DATA_ROOT / "m1"
MODELS_ROOT = DATA_ROOT / "models"
LOG_ROOT = DATA_ROOT / "logs"
STATE_ROOT = DATA_ROOT / "state"
DOWNLOAD_STATE_ROOT = STATE_ROOT / "downloads"
AGGREGATION_STATE_ROOT = STATE_ROOT / "aggregation"

REQUESTED_UNIVERSE = [
    "EURUSD",
    "GBPUSD",
    "USDJPY",
    "AUDUSD",
    "USDCAD",
    "USDCHF",
    "EURJPY",
    "EURGBP",
    "EURCHF",
    "AUDJPY",
]

# The locked continuation portfolio currently also requires GBPJPY.
PORTFOLIO_REQUIRED_PAIRS = ["GBPJPY"]

ALL_PAIRS = sorted(dict.fromkeys([*REQUESTED_UNIVERSE, *PORTFOLIO_REQUIRED_PAIRS]))
PAIR_LOWER = {pair: pair.lower() for pair in ALL_PAIRS}
PIP_SIZES = {
    "EURUSD": 0.0001,
    "GBPUSD": 0.0001,
    "USDJPY": 0.01,
    "GBPJPY": 0.01,
    "AUDUSD": 0.0001,
    "USDCAD": 0.0001,
    "USDCHF": 0.0001,
    "EURJPY": 0.01,
    "EURGBP": 0.0001,
    "EURCHF": 0.0001,
    "AUDJPY": 0.01,
}
PRICE_DIVISORS = {
    pair: (1000.0 if pair.endswith("JPY") else 100000.0)
    for pair in ALL_PAIRS
}


def ensure_pipeline_dirs() -> None:
    for path in (
        DATA_ROOT,
        TICK_ROOT,
        M1_ROOT,
        MODELS_ROOT,
        LOG_ROOT,
        STATE_ROOT,
        DOWNLOAD_STATE_ROOT,
        AGGREGATION_STATE_ROOT,
        M1_ROOT / "_daily_bid_ask",
    ):
        path.mkdir(parents=True, exist_ok=True)
