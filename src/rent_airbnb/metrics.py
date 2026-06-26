"""Optional pipeline metrics — avoid eager counts unless explicitly enabled."""

from __future__ import annotations

import logging
import os

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def log_counts_enabled() -> bool:
    return os.getenv("PIPELINE_LOG_COUNTS", "").lower() in ("1", "true", "yes")


def log_row_count(df: DataFrame, message: str) -> None:
    """Log row count only when PIPELINE_LOG_COUNTS=true."""
    if log_counts_enabled():
        logger.info(message, df.count())


def log_valid_quarantine(valid_df: DataFrame, bad_df: DataFrame, label: str) -> None:
    """Log valid/quarantine split only when PIPELINE_LOG_COUNTS=true."""
    if log_counts_enabled():
        logger.info(
            "%s — valid: %d, quarantined: %d",
            label,
            valid_df.count(),
            bad_df.count(),
        )
