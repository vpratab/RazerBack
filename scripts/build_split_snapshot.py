from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.fx_pipeline_config import ALL_PAIRS
from scripts.pipeline_utilities import LOCKED_SPLITS, infer_m1_path, load_m1_frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize a locked-period M1 snapshot for downstream research.")
    parser.add_argument("--data-dir", default="C:/fx_data/m1")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--split", required=True, choices=sorted(LOCKED_SPLITS))
    parser.add_argument("--instruments", nargs="+", default=[pair.lower() for pair in ALL_PAIRS])
    parser.add_argument("--skip-bid-ask", action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def write_snapshot(
    *,
    instrument: str,
    source_dir: Path,
    output_dir: Path,
    split_name: str,
    bid_ask: bool,
    overwrite: bool,
) -> None:
    source_path = infer_m1_path(instrument=instrument, data_dir=source_dir, bid_ask=bid_ask)
    suffix = source_path.name.replace(f"{instrument}_5yr_", "")
    target_path = output_dir / f"{instrument}_5yr_{suffix}"
    if target_path.exists() and not overwrite:
        print(f"SKIP {target_path}")
        return
    frame = load_m1_frame(
        instrument=instrument,
        split_name=split_name,
        data_dir=source_dir,
        bid_ask=bid_ask,
    )
    frame.to_parquet(target_path, index=False)
    print(f"WROTE {target_path} rows={len(frame)}")


def main() -> None:
    args = parse_args()
    source_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    for instrument in args.instruments:
        write_snapshot(
            instrument=instrument,
            source_dir=source_dir,
            output_dir=output_dir,
            split_name=args.split,
            bid_ask=False,
            overwrite=bool(args.overwrite),
        )
        if not args.skip_bid_ask:
            write_snapshot(
                instrument=instrument,
                source_dir=source_dir,
                output_dir=output_dir,
                split_name=args.split,
                bid_ask=True,
                overwrite=bool(args.overwrite),
            )


if __name__ == "__main__":
    main()
