"""Level 4 stretch: stream rentals one record at a time and refresh Gold incrementally."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .bronze import normalize_rentals_bronze_df
from .pipeline import PipelineConfig, get_or_create_spark, run_gold
from .silver import clean_rentals_silver

logger = logging.getLogger(__name__)


def refresh_gold_from_silver(spark: SparkSession, config: PipelineConfig) -> None:
    """Recompute all gold tables from current silver Parquet."""
    run_gold(spark, config)


def process_rentals_stream_batch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    config: PipelineConfig,
) -> None:
    """foreachBatch handler: append bronze rentals, rebuild silver rentals, refresh gold."""
    if batch_df.isEmpty():
        return

    logger.info("Streaming batch %s — %d new rental record(s)", batch_id, batch_df.count())

    bronze_batch = normalize_rentals_bronze_df(
        batch_df,
        source_file=config.rentals_stream_dir,
        dataset="rentals_stream",
    )
    bronze_batch.write.mode("append").parquet(config.bronze_rentals_path)

    rentals_bronze = spark.read.parquet(config.bronze_rentals_path)
    clean_rentals_silver(
        spark,
        rentals_bronze,
        output_path=config.silver_rentals_path,
        quarantine_path=config.silver_rentals_quarantine_path if config.write_quarantine else None,
    )

    refresh_gold_from_silver(spark, config)
    logger.info("Gold refreshed after batch %s", batch_id)


def run_rentals_streaming_pipeline(
    config: PipelineConfig | None = None,
    *,
    terminate_after_ms: int | None = None,
) -> None:
    """Stream rentals JSON files and update gold after each micro-batch.

    Prerequisites:
    1. Batch pipeline already ran (Airbnb bronze/silver exist).
    2. Streaming source prepared: `python scripts/prepare_streaming_source.py --limit 50`

    Args:
        config: Pipeline configuration.
        terminate_after_ms: Stop after N ms (for tests/demo). None = run until killed.
    """
    if config is None:
        config = PipelineConfig.from_env()

    stream_dir = Path(config.rentals_stream_dir)
    if not stream_dir.exists() or not any(stream_dir.glob("*.json")):
        raise FileNotFoundError(
            f"No streaming files in {stream_dir}. "
            "Run: python scripts/prepare_streaming_source.py"
        )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    spark = get_or_create_spark(config)
    checkpoint = config.streaming_checkpoint_path

    stream_df = (
        spark.readStream
        .format("json")
        .option("maxFilesPerTrigger", 1)
        .load(str(stream_dir))
    )

    def _handler(batch_df: DataFrame, batch_id: int) -> None:
        process_rentals_stream_batch(batch_df, batch_id, spark, config)

    writer = (
        stream_df.writeStream
        .foreachBatch(_handler)
        .option("checkpointLocation", checkpoint)
    )

    if terminate_after_ms is not None:
        query = writer.trigger(processingTime=f"{max(1, terminate_after_ms // 1000)} seconds").start()
        query.awaitTermination(timeout=terminate_after_ms + 5000)
    else:
        query = writer.start()
        query.awaitTermination()


def ingest_rentals_bronze_streaming(
    spark: SparkSession,
    stream_dir: str,
    checkpoint_path: str,
    output_path: str,
    *,
    use_cloud_files: bool | None = None,
) -> None:
    """Bronze-only streaming ingestion.

    Uses Databricks Auto Loader (cloudFiles) when available; otherwise local JSON stream.
    """
    use_cloud = use_cloud_files
    if use_cloud is None:
        use_cloud = os.getenv("USE_CLOUD_FILES", "").lower() in ("1", "true", "yes")

    logger.info("Starting rentals bronze stream from %s (cloudFiles=%s)", stream_dir, use_cloud)

    if use_cloud:
        stream_df = (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", checkpoint_path + "/schema")
            .load(stream_dir)
        )
    else:
        stream_df = (
            spark.readStream
            .format("json")
            .option("maxFilesPerTrigger", 1)
            .load(stream_dir)
        )

    stream_df = normalize_rentals_bronze_df(
        stream_df, source_file=stream_dir, dataset="rentals_stream"
    )

    query = (
        stream_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .start(output_path)
    )
    query.awaitTermination()
