"""Shared I/O helpers."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame


def write_parquet(df: DataFrame, path: str) -> None:
    """Write Parquet, optionally coalescing partitions for small local outputs."""
    partitions = os.getenv("OUTPUT_COALESCE_PARTITIONS")
    if partitions:
        df = df.coalesce(int(partitions))
    df.write.mode("overwrite").parquet(path)
