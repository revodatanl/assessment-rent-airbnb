"""Bronze layer: raw ingestion of Airbnb CSV and Kamernet JSON.

No business logic here — we land data as-is, cast to strings only,
and tag every record with ingestion metadata. This gives us a full
audit trail and makes re-processing trivial.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .io import write_parquet
from .metrics import log_row_count
from .schemas import AIRBNB_BRONZE_SCHEMA

logger = logging.getLogger(__name__)

_RENTALS_ARRAY_SCALAR_FIELDS = (
    "_id",
    "crawledAt",
    "firstSeenAt",
    "lastSeenAt",
    "detailsCrawledAt",
)


# ---------------------------------------------------------------------------
# Airbnb
# ---------------------------------------------------------------------------

def ingest_airbnb_bronze(
    spark: SparkSession,
    csv_path: str,
    output_path: str | None = None,
) -> DataFrame:
    """Read Airbnb CSV into a Bronze DataFrame.

    All columns land as strings; we add _ingested_at and _source_file
    metadata columns so every row is fully traceable.

    Args:
        spark: Active SparkSession.
        csv_path: Path to airbnb.csv (local or GCS/S3/ABFS URI).
        output_path: If set, writes Parquet to this location.

    Returns:
        Bronze Airbnb DataFrame.
    """
    logger.info("Ingesting Airbnb bronze from %s", csv_path)

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")      # keep bad rows — we'll quarantine in silver
        .schema(AIRBNB_BRONZE_SCHEMA)
        .csv(csv_path)
    )

    df = _add_metadata(df, source_file=csv_path, layer="bronze", dataset="airbnb")

    log_row_count(df, "Airbnb bronze row count: %d")

    if output_path:
        logger.info("Writing bronze Parquet to %s", output_path)
        write_parquet(df, output_path)

    return df


# ---------------------------------------------------------------------------
# Kamernet / Rentals
# ---------------------------------------------------------------------------

def ingest_rentals_bronze(
    spark: SparkSession,
    json_path: str,
    output_path: str | None = None,
) -> DataFrame:
    """Read Kamernet rentals JSON into a Bronze DataFrame.

    The JSON is an array of objects. Nested list-valued fields (_id,
    crawledAt, firstSeenAt, lastSeenAt, detailsCrawledAt) are flattened
    to their first element since they're always single-element arrays
    in this dataset.

    Args:
        spark: Active SparkSession.
        json_path: Path to rentals.json.
        output_path: If set, writes Parquet to this location.

    Returns:
        Bronze rentals DataFrame.
    """
    logger.info("Ingesting Rentals bronze from %s", json_path)

    df = spark.read.option("multiLine", True).json(json_path)
    df = normalize_rentals_bronze_df(df, source_file=json_path, dataset="rentals")

    log_row_count(df, "Rentals bronze row count: %d")

    if output_path:
        logger.info("Writing bronze Parquet to %s", output_path)
        write_parquet(df, output_path)

    return df


# ---------------------------------------------------------------------------
# Streaming variant for rentals (Level 4 stretch goal)
# ---------------------------------------------------------------------------

def ingest_rentals_bronze_streaming(
    spark: SparkSession,
    stream_dir: str,
    checkpoint_path: str,
    output_path: str,
    *,
    use_cloud_files: bool | None = None,
) -> None:
    """Bronze-only streaming — delegates to streaming module (L4)."""
    from .streaming import ingest_rentals_bronze_streaming as _stream

    _stream(
        spark,
        stream_dir,
        checkpoint_path,
        output_path,
        use_cloud_files=use_cloud_files,
    )


def normalize_rentals_bronze_df(
    df: DataFrame,
    *,
    source_file: str,
    dataset: str = "rentals",
) -> DataFrame:
    """Flatten array fields and cast to string — shared by batch and streaming paths."""
    for field in _RENTALS_ARRAY_SCALAR_FIELDS:
        if field not in df.columns:
            continue
        if dict(df.dtypes).get(field, "").startswith("array"):
            df = df.withColumn(
                field,
                F.when(
                    F.col(field).isNotNull() & (F.size(F.col(field)) > 0),
                    F.element_at(F.col(field), 1).cast("string"),
                ).otherwise(F.lit(None).cast("string")),
            )

    df = df.select(*[F.col(c).cast("string").alias(c) for c in df.columns])
    return _add_metadata(df, source_file=source_file, layer="bronze", dataset=dataset)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _add_metadata(df: DataFrame, source_file: str, layer: str, dataset: str) -> DataFrame:
    """Attach lineage metadata columns to any DataFrame."""
    return (
        df
        .withColumn("_ingested_at", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_layer", F.lit(layer))
        .withColumn("_dataset", F.lit(dataset))
    )
