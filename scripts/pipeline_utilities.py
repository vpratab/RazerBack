from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Iterable

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.fx_pipeline_config import ALL_PAIRS


@dataclass(frozen=True)
class DataSplit:
    name: str
    start: pd.Timestamp
    end: pd.Timestamp


DISCOVERY = DataSplit(
    name="discovery",
    start=pd.Timestamp("2011-01-01 00:00:00", tz="UTC"),
    end=pd.Timestamp("2019-12-31 23:59:59", tz="UTC"),
)
VALIDATION = DataSplit(
    name="validation",
    start=pd.Timestamp("2020-01-01 00:00:00", tz="UTC"),
    end=pd.Timestamp("2022-12-31 23:59:59", tz="UTC"),
)
FINAL_TEST = DataSplit(
    name="final_test",
    start=pd.Timestamp("2023-01-01 00:00:00", tz="UTC"),
    end=pd.Timestamp("2025-12-31 23:59:59", tz="UTC"),
)
LIVE_FORWARD = DataSplit(
    name="live_forward",
    start=pd.Timestamp("2026-01-01 00:00:00", tz="UTC"),
    end=pd.Timestamp("2026-12-31 23:59:59", tz="UTC"),
)

LOCKED_SPLITS: dict[str, DataSplit] = {
    DISCOVERY.name: DISCOVERY,
    VALIDATION.name: VALIDATION,
    FINAL_TEST.name: FINAL_TEST,
    LIVE_FORWARD.name: LIVE_FORWARD,
}

DEFAULT_M1_ROOT = Path("C:/fx_data/m1")
DEFAULT_INSTRUMENTS = [pair.lower() for pair in ALL_PAIRS]


def canonical_instrument(instrument: str) -> str:
    return instrument.replace("/", "").replace("-", "").lower()


def get_split(split_name: str) -> DataSplit:
    key = split_name.strip().lower()
    if key not in LOCKED_SPLITS:
        raise KeyError(f"Unknown split '{split_name}'. Available: {sorted(LOCKED_SPLITS)}")
    return LOCKED_SPLITS[key]


def normalize_timestamp_series(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, utc=True)


def period_mask(
    timestamps: pd.Series | pd.Index,
    split_name: str,
) -> pd.Series:
    split = get_split(split_name)
    normalized = pd.to_datetime(timestamps, utc=True)
    return (normalized >= split.start) & (normalized <= split.end)


def filter_frame_by_split(
    frame: pd.DataFrame,
    split_name: str,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    if timestamp_col not in frame.columns:
        raise KeyError(f"Timestamp column '{timestamp_col}' not present in frame.")
    mask = period_mask(frame[timestamp_col], split_name)
    return frame.loc[mask].copy()


def infer_m1_path(
    instrument: str,
    data_dir: Path = DEFAULT_M1_ROOT,
    bid_ask: bool = False,
) -> Path:
    symbol = canonical_instrument(instrument)
    candidates = (
        [data_dir / f"{symbol}_5yr_m1_bid_ask.parquet", data_dir / f"{symbol}_5yr_m1_ba.parquet"]
        if bid_ask
        else [data_dir / f"{symbol}_5yr_m1.parquet"]
    )
    for candidate in candidates:
        if candidate.exists():
            return candidate
    kind = "bid/ask" if bid_ask else "midpoint"
    raise FileNotFoundError(f"Could not locate {kind} M1 parquet for {instrument} in {data_dir}")


def load_m1_frame(
    instrument: str,
    split_name: str | None = None,
    data_dir: Path = DEFAULT_M1_ROOT,
    bid_ask: bool = False,
    columns: list[str] | None = None,
    timestamp_col: str = "timestamp",
) -> pd.DataFrame:
    path = infer_m1_path(instrument=instrument, data_dir=data_dir, bid_ask=bid_ask)
    frame = pd.read_parquet(path, columns=columns)
    if timestamp_col not in frame.columns:
        raise KeyError(f"Timestamp column '{timestamp_col}' not present in {path}")
    frame[timestamp_col] = normalize_timestamp_series(frame[timestamp_col])
    frame = frame.sort_values(timestamp_col).reset_index(drop=True)
    if split_name is not None:
        frame = filter_frame_by_split(frame, split_name=split_name, timestamp_col=timestamp_col)
    return frame


def split_row_summary(
    instrument: str,
    data_dir: Path = DEFAULT_M1_ROOT,
    bid_ask: bool = False,
) -> dict[str, object]:
    path = infer_m1_path(instrument=instrument, data_dir=data_dir, bid_ask=bid_ask)
    frame = pd.read_parquet(path, columns=["timestamp"])
    frame["timestamp"] = normalize_timestamp_series(frame["timestamp"])
    row: dict[str, object] = {
        "instrument": canonical_instrument(instrument),
        "file": str(path),
        "rows_total": int(len(frame)),
        "timestamp_min": frame["timestamp"].min().isoformat() if not frame.empty else "",
        "timestamp_max": frame["timestamp"].max().isoformat() if not frame.empty else "",
    }
    for split_name, split in LOCKED_SPLITS.items():
        mask = (frame["timestamp"] >= split.start) & (frame["timestamp"] <= split.end)
        split_frame = frame.loc[mask]
        row[f"{split_name}_rows"] = int(len(split_frame))
        row[f"{split_name}_min"] = split_frame["timestamp"].min().isoformat() if not split_frame.empty else ""
        row[f"{split_name}_max"] = split_frame["timestamp"].max().isoformat() if not split_frame.empty else ""
        row[f"{split_name}_nonempty"] = bool(not split_frame.empty)
    return row


def summarize_split_coverage(
    instruments: Iterable[str] | None = None,
    data_dir: Path = DEFAULT_M1_ROOT,
    bid_ask: bool = False,
) -> pd.DataFrame:
    rows = [
        split_row_summary(instrument=instrument, data_dir=data_dir, bid_ask=bid_ask)
        for instrument in (instruments or DEFAULT_INSTRUMENTS)
    ]
    return pd.DataFrame(rows).sort_values("instrument").reset_index(drop=True)
