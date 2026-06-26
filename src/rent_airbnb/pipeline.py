"""Pipeline orchestrator: runs Bronze → Silver → Gold end-to-end.

Entry point for both local runs and Databricks job submissions.
Configure via environment variables or pass a PipelineConfig dataclass.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from .bronze import ingest_airbnb_bronze, ingest_rentals_bronze
from .gold import (
    aggregate_airbnb_by_pc4,
    aggregate_rentals_by_pc4,
    build_investment_comparison,
    export_airbnb_listing_revenue,
    export_rental_listing_revenue,
    top_investment_opportunities,
)
from .io import write_parquet
from .silver import clean_airbnb_silver, clean_rentals_silver

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    """All paths and toggles for the pipeline.

    Defaults point to the repo-relative data/ directory so the pipeline
    works out-of-the-box locally. Override for GCS/S3/ABFS in Databricks.
    """
    # Input paths
    airbnb_csv_path: str = "data/input/airbnb.csv"
    rentals_json_path: str = "data/input/rentals.json"
    post_codes_geojson_path: str | None = "data/input/geo/post_codes.geojson"

    # Output paths  (Parquet)
    output_base: str = "data/output"

    # Streaming (L4)
    rentals_stream_dir: str = "data/input/streaming/rentals"
    streaming_checkpoint_path: str = "data/output/_checkpoints/rentals_stream"

    # Feature flags
    geo_enrich: bool = True           # use geojson for missing PC4
    write_quarantine: bool = True     # write bad records to _quarantine/

    # Spark config overrides (applied before session creation)
    spark_configs: dict = field(default_factory=lambda: {
        "spark.sql.shuffle.partitions": "8",     # sensible for ~10k rows
        "spark.sql.adaptive.enabled": "true",
    })

    @property
    def bronze_airbnb_path(self) -> str:
        return f"{self.output_base}/bronze/airbnb"

    @property
    def bronze_rentals_path(self) -> str:
        return f"{self.output_base}/bronze/rentals"

    @property
    def silver_airbnb_path(self) -> str:
        return f"{self.output_base}/silver/airbnb"

    @property
    def silver_rentals_path(self) -> str:
        return f"{self.output_base}/silver/rentals"

    @property
    def silver_airbnb_quarantine_path(self) -> str:
        return f"{self.output_base}/silver/airbnb/_quarantine"

    @property
    def silver_rentals_quarantine_path(self) -> str:
        return f"{self.output_base}/silver/rentals/_quarantine"

    @property
    def gold_airbnb_pc4_path(self) -> str:
        return f"{self.output_base}/gold/airbnb_by_pc4"

    @property
    def gold_rentals_pc4_path(self) -> str:
        return f"{self.output_base}/gold/rentals_by_pc4"

    @property
    def gold_airbnb_listings_path(self) -> str:
        return f"{self.output_base}/gold/airbnb_listing_revenue"

    @property
    def gold_rentals_listings_path(self) -> str:
        return f"{self.output_base}/gold/rentals_listing_revenue"

    @property
    def gold_comparison_path(self) -> str:
        return f"{self.output_base}/gold/investment_comparison"

    @property
    def gold_top_opportunities_path(self) -> str:
        return f"{self.output_base}/gold/top_opportunities"

    @classmethod
    def from_env(cls) -> PipelineConfig:
        """Build config from environment variables (useful in Databricks jobs)."""
        return cls(
            airbnb_csv_path=os.getenv("AIRBNB_CSV_PATH", "data/input/airbnb.csv"),
            rentals_json_path=os.getenv("RENTALS_JSON_PATH", "data/input/rentals.json"),
            post_codes_geojson_path=os.getenv("POST_CODES_GEOJSON_PATH", "data/input/geo/post_codes.geojson"),
            output_base=os.getenv("OUTPUT_BASE", "data/output"),
            rentals_stream_dir=os.getenv("RENTALS_STREAM_DIR", "data/input/streaming/rentals"),
            streaming_checkpoint_path=os.getenv(
                "STREAMING_CHECKPOINT_PATH", "data/output/_checkpoints/rentals_stream"
            ),
            geo_enrich=os.getenv("GEO_ENRICH", "true").lower() == "true",
        )


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

def get_or_create_spark(config: PipelineConfig) -> SparkSession:
    """Return an existing SparkSession or create a new local one.

    On Databricks, `SparkSession.builder.getOrCreate()` returns the
    cluster session; locally it spins up a local[*] session.
    """
    master = os.getenv("SPARK_MASTER", "local[*]")
    builder = (
        SparkSession.builder
        .appName("rent-airbnb-pipeline")
        .master(master)
    )

    # macOS / VPN: hostname may not resolve to a bindable address
    if master.startswith("local"):
        builder = (
            builder
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
        )

    for k, v in config.spark_configs.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession ready — version %s", spark.version)
    return spark


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def run_bronze(spark: SparkSession, config: PipelineConfig) -> tuple:
    """Stage 1: raw ingestion."""
    logger.info("=== BRONZE ===")
    airbnb_bronze = ingest_airbnb_bronze(
        spark, config.airbnb_csv_path, output_path=config.bronze_airbnb_path
    )
    rentals_bronze = ingest_rentals_bronze(
        spark, config.rentals_json_path, output_path=config.bronze_rentals_path
    )
    return airbnb_bronze, rentals_bronze


def run_silver(spark: SparkSession, config: PipelineConfig, airbnb_bronze, rentals_bronze) -> tuple:
    """Stage 2: clean and type-cast."""
    logger.info("=== SILVER ===")
    airbnb_silver = clean_airbnb_silver(
        spark,
        airbnb_bronze,
        post_codes_geojson_path=config.post_codes_geojson_path if config.geo_enrich else None,
        output_path=config.silver_airbnb_path,
        quarantine_path=config.silver_airbnb_quarantine_path if config.write_quarantine else None,
    )
    rentals_silver = clean_rentals_silver(
        spark,
        rentals_bronze,
        output_path=config.silver_rentals_path,
        quarantine_path=config.silver_rentals_quarantine_path if config.write_quarantine else None,
    )
    return airbnb_silver, rentals_silver


def run_gold(spark, config: PipelineConfig, airbnb_silver=None, rentals_silver=None) -> tuple:
    """Stage 3: revenue aggregation and comparison.

    Reloads silver from Parquet to break broadcast-variable lineage from the
    geo-enrichment UDF — prevents 'Block does not exist' errors in Spark 4.x.
    """
    logger.info("=== GOLD ===")
    # Reload from disk — clean lineage, broadcast already destroyed after silver write
    airbnb_silver = spark.read.parquet(config.silver_airbnb_path)
    rentals_silver = spark.read.parquet(config.silver_rentals_path)
    export_airbnb_listing_revenue(airbnb_silver, output_path=config.gold_airbnb_listings_path)
    export_rental_listing_revenue(rentals_silver, output_path=config.gold_rentals_listings_path)
    airbnb_pc4 = aggregate_airbnb_by_pc4(airbnb_silver, output_path=config.gold_airbnb_pc4_path)
    rentals_pc4 = aggregate_rentals_by_pc4(rentals_silver, output_path=config.gold_rentals_pc4_path)
    comparison = build_investment_comparison(
        airbnb_pc4, rentals_pc4, output_path=config.gold_comparison_path
    )
    top = top_investment_opportunities(comparison, n=20)
    write_parquet(top, config.gold_top_opportunities_path)
    return airbnb_pc4, rentals_pc4, comparison, top


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(config: PipelineConfig | None = None) -> dict:
    """Execute the full medallion pipeline and return the gold DataFrames.

    Args:
        config: PipelineConfig instance. Defaults to PipelineConfig.from_env().

    Returns:
        Dict of named DataFrames:
        {
          'airbnb_bronze', 'rentals_bronze',
          'airbnb_silver', 'rentals_silver',
          'airbnb_pc4', 'rentals_pc4',
          'comparison', 'top_opportunities'
        }
    """
    if config is None:
        config = PipelineConfig.from_env()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )

    spark = get_or_create_spark(config)

    airbnb_bronze, rentals_bronze = run_bronze(spark, config)
    airbnb_silver, rentals_silver = run_silver(spark, config, airbnb_bronze, rentals_bronze)
    airbnb_pc4, rentals_pc4, comparison, top = run_gold(spark, config, airbnb_silver, rentals_silver)

    logger.info("=== PIPELINE COMPLETE ===")
    logger.info("Output written to: %s", config.output_base)

    return {
        "airbnb_bronze": airbnb_bronze,
        "rentals_bronze": rentals_bronze,
        "airbnb_silver": airbnb_silver,
        "rentals_silver": rentals_silver,
        "airbnb_pc4": airbnb_pc4,
        "rentals_pc4": rentals_pc4,
        "comparison": comparison,
        "top_opportunities": top,
    }


def print_final_output(top: DataFrame) -> None:
    """Print the pipeline deliverable: top PC4 investment opportunities."""
    cols = [
        "pc4",
        "airbnb_avg_annual_revenue_eur",
        "rental_avg_annual_revenue_eur",
        "revenue_delta_eur",
        "recommendation",
    ]
    print("\nTop investment opportunities:")
    top.select(*cols).show(truncate=False)


if __name__ == "__main__":
    results = run()
    print_final_output(results["top_opportunities"])
