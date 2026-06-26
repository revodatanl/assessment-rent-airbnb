#!/usr/bin/env python3
"""Split rentals.json into one JSON file per record for structured streaming (L4).

Usage:
    python scripts/prepare_streaming_source.py
    python scripts/prepare_streaming_source.py --limit 100
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare JSONL-style streaming source directory")
    parser.add_argument(
        "--input",
        default="data/input/rentals.json",
        help="Source Kamernet JSON array file",
    )
    parser.add_argument(
        "--output-dir",
        default="data/input/streaming/rentals",
        help="Directory watched by Spark streaming (one record per file)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Max records to export (0 = all)")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    with open(input_path) as fh:
        records = json.load(fh)

    if args.limit > 0:
        records = records[: args.limit]

    for idx, record in enumerate(records):
        out = output_dir / f"rental_{idx:06d}.json"
        out.write_text(json.dumps(record))

    print(f"Wrote {len(records)} streaming files to {output_dir}")


if __name__ == "__main__":
    main()
