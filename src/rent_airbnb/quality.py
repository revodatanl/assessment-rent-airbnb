"""Data quality expectations — shared by Silver transforms and DLT pipeline.

Mirrors DLT @dlt.expect / @dlt.expect_or_drop rules as Spark Column expressions
so the same business rules run locally and on Databricks.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schemas import VALID_PROPERTY_TYPES, VALID_ROOM_TYPES

# ---------------------------------------------------------------------------
# Airbnb silver expectations
# ---------------------------------------------------------------------------

def airbnb_silver_valid() -> Column:
    """Rows passing all Airbnb silver quality rules."""
    return (
        F.col("pc4").isNotNull() & (F.col("pc4") != "")
        & F.col("price_eur_night").isNotNull()
        & F.col("price_eur_night").between(10, 5000)
        & F.col("room_type").isin(*VALID_ROOM_TYPES)
    )


AIRBNB_DLT_EXPECTATIONS: list[tuple[str, str]] = [
    ("valid_pc4", "pc4 IS NOT NULL AND pc4 != ''"),
    ("valid_price", "price_eur_night IS NOT NULL AND price_eur_night BETWEEN 10 AND 5000"),
    ("valid_room_type", "room_type IN ('Entire home/apt', 'Private room', 'Shared room')"),
]


# ---------------------------------------------------------------------------
# Rentals silver expectations
# ---------------------------------------------------------------------------

def rentals_silver_valid() -> Column:
    """Rows passing all rentals silver quality rules."""
    return (
        F.col("rent_eur_month").isNotNull()
        & F.col("rent_eur_month").between(200, 4000)
        & F.col("pc4").isNotNull() & (F.col("pc4") != "")
        & F.col("property_type").isin(*VALID_PROPERTY_TYPES)
    )


RENTALS_DLT_EXPECTATIONS: list[tuple[str, str]] = [
    ("valid_rent", "rent_eur_month IS NOT NULL AND rent_eur_month BETWEEN 200 AND 4000"),
    ("valid_pc4", "pc4 IS NOT NULL AND pc4 != ''"),
    ("valid_property_type", "property_type IN ('Room', 'Apartment', 'Studio', 'Anti-squat', 'Student residence')"),
]


# ---------------------------------------------------------------------------
# Metrics helper (local equivalent of DLT expectation metrics)
# ---------------------------------------------------------------------------

def expectation_summary(df: DataFrame, expectations: list[tuple[str, Column]]) -> dict[str, float]:
    """Return pass-rate per named expectation (0.0–1.0)."""
    total = df.count()
    if total == 0:
        return {name: 1.0 for name, _ in expectations}
    summary = {}
    for name, condition in expectations:
        passed = df.filter(condition).count()
        summary[name] = passed / total
    return summary


def airbnb_expectation_columns() -> list[tuple[str, Column]]:
    return [
        ("valid_pc4", F.col("pc4").isNotNull() & (F.col("pc4") != "")),
        ("valid_price", F.col("price_eur_night").isNotNull() & F.col("price_eur_night").between(10, 5000)),
        ("valid_room_type", F.col("room_type").isin(*VALID_ROOM_TYPES)),
    ]
