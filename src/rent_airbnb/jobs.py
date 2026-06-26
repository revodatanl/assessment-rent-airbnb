"""Databricks / workflow entry points — one medallion stage per job task."""

from __future__ import annotations

import argparse
import logging
import os

from .pipeline import (
    PipelineConfig,
    get_or_create_spark,
    run_bronze,
    run_gold,
    run_silver,
)

logger = logging.getLogger(__name__)


def _base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--output-base",
        default=os.getenv("OUTPUT_BASE", "data/output"),
        help="Root path for medallion Parquet output",
    )
    parser.add_argument(
        "--airbnb-csv",
        default=os.getenv("AIRBNB_CSV_PATH", "data/input/airbnb.csv"),
        help="Path to Airbnb CSV",
    )
    parser.add_argument(
        "--rentals-json",
        default=os.getenv("RENTALS_JSON_PATH", "data/input/rentals.json"),
        help="Path to Kamernet rentals JSON",
    )
    parser.add_argument(
        "--post-codes-geojson",
        default=os.getenv("POST_CODES_GEOJSON_PATH", "data/input/geo/post_codes.geojson"),
        help="Path to post-code GeoJSON for geo-enrichment",
    )
    return parser


def _config_from_args(args: argparse.Namespace) -> PipelineConfig:
    return PipelineConfig(
        airbnb_csv_path=args.airbnb_csv,
        rentals_json_path=args.rentals_json,
        post_codes_geojson_path=args.post_codes_geojson,
        output_base=args.output_base,
        geo_enrich=os.getenv("GEO_ENRICH", "true").lower() == "true",
    )


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    )


def bronze_job() -> None:
    """Bronze stage entry point for Databricks wheel tasks."""
    _configure_logging()
    args = _base_parser("Bronze ingestion").parse_args()
    config = _config_from_args(args)
    spark = get_or_create_spark(config)
    run_bronze(spark, config)
    logger.info("Bronze complete — output: %s/bronze", config.output_base)


def silver_job() -> None:
    """Silver stage entry point — reads bronze Parquet from output_base."""
    _configure_logging()
    args = _base_parser("Silver transformation").parse_args()
    config = _config_from_args(args)
    spark = get_or_create_spark(config)
    airbnb_bronze = spark.read.parquet(config.bronze_airbnb_path)
    rentals_bronze = spark.read.parquet(config.bronze_rentals_path)
    run_silver(spark, config, airbnb_bronze, rentals_bronze)
    logger.info("Silver complete — output: %s/silver", config.output_base)


def gold_job() -> None:
    """Gold stage entry point — reads silver Parquet from output_base."""
    _configure_logging()
    args = _base_parser("Gold aggregation").parse_args()
    config = _config_from_args(args)
    spark = get_or_create_spark(config)
    run_gold(spark, config)
    logger.info("Gold complete — output: %s/gold", config.output_base)


def pipeline_job() -> None:
    """Full batch medallion pipeline entry point."""
    from .pipeline import print_final_output, run

    _configure_logging()
    args = _base_parser("Full medallion pipeline").parse_args()
    config = _config_from_args(args)
    results = run(config)
    print_final_output(results["top_opportunities"])
    logger.info("Pipeline complete — output: %s", config.output_base)


def streaming_job() -> None:
    """Streaming rentals + live gold refresh entry point (L4)."""
    from .streaming import run_rentals_streaming_pipeline

    _configure_logging()
    parser = _base_parser("Streaming rentals pipeline")
    parser.add_argument(
        "--terminate-after-ms",
        type=int,
        default=int(os.getenv("STREAMING_TERMINATE_MS", "0")),
        help="Stop after N ms (0 = run until killed)",
    )
    parser.add_argument(
        "--rentals-stream-dir",
        default=os.getenv("RENTALS_STREAM_DIR", "data/input/streaming/rentals"),
    )
    args = parser.parse_args()
    config = _config_from_args(args)
    config.rentals_stream_dir = args.rentals_stream_dir
    terminate = args.terminate_after_ms if args.terminate_after_ms > 0 else None
    run_rentals_streaming_pipeline(config, terminate_after_ms=terminate)
