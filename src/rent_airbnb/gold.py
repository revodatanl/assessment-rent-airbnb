"""Gold layer: revenue aggregations and investment comparison per PC4.

This is the analytical core of the pipeline. We answer the key question:
*For each Amsterdam postal code, is Airbnb or long-term rental more profitable?*

Revenue model
-------------
Airbnb annual revenue per listing:
    price_eur_night × occupancy_rate × 365 × (1 - platform_fee_pct)

Kamernet annual revenue per listing:
    (rent_eur_month + additional_costs_eur) × occupied_months_per_year

We aggregate both to PC4 level, then produce a comparison table with a
recommendation column.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .io import write_parquet
from .metrics import log_row_count
from .schemas import (
    AIRBNB_NIGHTS_PER_YEAR,
    AIRBNB_OCCUPANCY_RATE,
    AIRBNB_PLATFORM_FEE_PCT,
    RENTAL_OCCUPIED_MONTHS_PER_YEAR,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-listing revenue
# ---------------------------------------------------------------------------

def compute_airbnb_revenue(df: DataFrame) -> DataFrame:
    """Add annual_revenue_eur column to Airbnb silver DataFrame.

    We exclude outlier-priced listings (> 3× IQR) from the revenue calc
    to avoid skewing PC4 averages, but keep them in the table for inspection.

    Formula:
        revenue = price × occupancy × 365 × (1 - fee)
    """
    return df.withColumn(
        "annual_revenue_eur",
        F.when(
            ~F.col("is_price_outlier"),
            F.col("price_eur_night")
            * F.lit(AIRBNB_OCCUPANCY_RATE)
            * F.lit(AIRBNB_NIGHTS_PER_YEAR)
            * F.lit(1 - AIRBNB_PLATFORM_FEE_PCT)
        ).otherwise(F.lit(None).cast("double"))
    ).withColumn("revenue_model", F.lit("airbnb"))


def compute_rental_revenue(df: DataFrame) -> DataFrame:
    """Add annual_revenue_eur column to Rentals silver DataFrame.

    We use only active listings. The total monthly income includes rent +
    service/utility pass-through costs, scaled to 11 months per year to
    account for typical vacancy between tenants.
    """
    return df.withColumn(
        "annual_revenue_eur",
        F.when(
            F.col("is_active") & ~F.col("is_rent_outlier"),
            (F.col("rent_eur_month") + F.col("additional_costs_eur"))
            * F.lit(RENTAL_OCCUPIED_MONTHS_PER_YEAR)
        ).otherwise(F.lit(None).cast("double"))
    ).withColumn("revenue_model", F.lit("rental"))


def export_airbnb_listing_revenue(df: DataFrame, output_path: str | None = None) -> DataFrame:
    """Write per-listing Airbnb revenue to Gold."""
    listing_df = compute_airbnb_revenue(df).select(
        "pc4",
        "room_type",
        "price_eur_night",
        "annual_revenue_eur",
        "is_price_outlier",
        "revenue_model",
    )
    if output_path:
        write_parquet(listing_df, output_path)
    return listing_df


def export_rental_listing_revenue(df: DataFrame, output_path: str | None = None) -> DataFrame:
    """Write per-listing Kamernet revenue to Gold."""
    listing_df = compute_rental_revenue(df).select(
        "listing_id",
        "pc4",
        "property_type",
        "rent_eur_month",
        "additional_costs_eur",
        "annual_revenue_eur",
        "is_active",
        "is_rent_outlier",
        "revenue_model",
    )
    if output_path:
        write_parquet(listing_df, output_path)
    return listing_df


# ---------------------------------------------------------------------------
# PC4-level aggregations
# ---------------------------------------------------------------------------

def aggregate_airbnb_by_pc4(df: DataFrame, output_path: str | None = None) -> DataFrame:
    """Aggregate Airbnb revenue to PC4 level.

    Returns one row per PC4 with:
    - listing_count: number of active listings
    - median_price_eur_night
    - avg_annual_revenue_eur: average over non-outlier listings
    - p25_revenue / p75_revenue: spread
    - avg_review_score
    - room_type breakdown (entire/private/shared pct)
    """
    df_with_rev = compute_airbnb_revenue(df)

    agg_df = df_with_rev.groupBy("pc4").agg(
        F.count("*").alias("listing_count"),
        F.percentile_approx("price_eur_night", 0.5).alias("median_price_eur_night"),
        F.avg("annual_revenue_eur").alias("avg_annual_revenue_eur"),
        F.percentile_approx("annual_revenue_eur", 0.25).alias("p25_annual_revenue_eur"),
        F.percentile_approx("annual_revenue_eur", 0.75).alias("p75_annual_revenue_eur"),
        F.avg("review_score").alias("avg_review_score"),
        F.avg(F.when(F.col("room_type") == "Entire home/apt", 1).otherwise(0)).alias("pct_entire_home"),
        F.avg(F.when(F.col("room_type") == "Private room", 1).otherwise(0)).alias("pct_private_room"),
        F.avg("bedrooms").alias("avg_bedrooms"),
        F.avg("accommodates").alias("avg_accommodates"),
    ).withColumn("source", F.lit("airbnb"))

    log_row_count(agg_df, "Airbnb PC4 gold: %d postal codes")

    if output_path:
        write_parquet(agg_df, output_path)

    return agg_df


def aggregate_rentals_by_pc4(df: DataFrame, output_path: str | None = None) -> DataFrame:
    """Aggregate Kamernet revenue to PC4 level.

    Returns one row per PC4 with median rent, avg annual revenue,
    and property type breakdown.
    """
    df_with_rev = compute_rental_revenue(df)

    agg_df = df_with_rev.groupBy("pc4").agg(
        F.count("*").alias("listing_count"),
        F.percentile_approx("rent_eur_month", 0.5).alias("median_rent_eur_month"),
        F.avg("annual_revenue_eur").alias("avg_annual_revenue_eur"),
        F.percentile_approx("annual_revenue_eur", 0.25).alias("p25_annual_revenue_eur"),
        F.percentile_approx("annual_revenue_eur", 0.75).alias("p75_annual_revenue_eur"),
        F.avg("area_sqm").alias("avg_area_sqm"),
        F.avg(F.when(F.col("property_type") == "Apartment", 1).otherwise(0)).alias("pct_apartment"),
        F.avg(F.when(F.col("property_type") == "Room", 1).otherwise(0)).alias("pct_room"),
        F.avg(F.when(F.col("property_type") == "Studio", 1).otherwise(0)).alias("pct_studio"),
    ).withColumn("source", F.lit("rental"))

    log_row_count(agg_df, "Rentals PC4 gold: %d postal codes")

    if output_path:
        write_parquet(agg_df, output_path)

    return agg_df


# ---------------------------------------------------------------------------
# Investment comparison (the money table)
# ---------------------------------------------------------------------------

def build_investment_comparison(
    airbnb_pc4: DataFrame,
    rentals_pc4: DataFrame,
    output_path: str | None = None,
) -> DataFrame:
    """Join Airbnb and Rental PC4 aggregations into a single comparison table.

    Columns:
    - pc4
    - airbnb_listing_count, airbnb_avg_annual_revenue_eur, airbnb_median_price_night
    - rental_listing_count, rental_avg_annual_revenue_eur, rental_median_rent_month
    - revenue_delta_eur: airbnb - rental (positive = airbnb better)
    - revenue_uplift_pct: percentage uplift of the better option
    - recommendation: 'airbnb' | 'rental' | 'neutral'
    - data_confidence: 'high' (>=10 listings both) | 'medium' | 'low'
    """
    ab = airbnb_pc4.select(
        "pc4",
        F.col("listing_count").alias("airbnb_listing_count"),
        F.col("avg_annual_revenue_eur").alias("airbnb_avg_annual_revenue_eur"),
        F.col("median_price_eur_night").alias("airbnb_median_price_night"),
        F.col("p25_annual_revenue_eur").alias("airbnb_p25_revenue"),
        F.col("p75_annual_revenue_eur").alias("airbnb_p75_revenue"),
        F.col("avg_review_score").alias("airbnb_avg_review_score"),
        F.col("pct_entire_home"),
    )

    rl = rentals_pc4.select(
        "pc4",
        F.col("listing_count").alias("rental_listing_count"),
        F.col("avg_annual_revenue_eur").alias("rental_avg_annual_revenue_eur"),
        F.col("median_rent_eur_month").alias("rental_median_rent_month"),
        F.col("p25_annual_revenue_eur").alias("rental_p25_revenue"),
        F.col("p75_annual_revenue_eur").alias("rental_p75_revenue"),
        F.col("avg_area_sqm"),
    )

    df = ab.join(rl, on="pc4", how="outer")

    # Revenue delta
    df = df.withColumn(
        "revenue_delta_eur",
        F.col("airbnb_avg_annual_revenue_eur") - F.col("rental_avg_annual_revenue_eur")
    )

    # % uplift of winner over loser
    df = df.withColumn(
        "revenue_uplift_pct",
        F.when(
            F.col("airbnb_avg_annual_revenue_eur").isNotNull()
            & F.col("rental_avg_annual_revenue_eur").isNotNull(),
            F.abs(F.col("revenue_delta_eur"))
            / F.least(
                F.col("airbnb_avg_annual_revenue_eur"),
                F.col("rental_avg_annual_revenue_eur")
            ) * 100
        ).otherwise(F.lit(None).cast("double"))
    )

    # Recommendation
    df = df.withColumn(
        "recommendation",
        F.when(
            F.col("airbnb_avg_annual_revenue_eur").isNull(), F.lit("rental_only")
        ).when(
            F.col("rental_avg_annual_revenue_eur").isNull(), F.lit("airbnb_only")
        ).when(
            F.col("revenue_uplift_pct") < 5, F.lit("neutral")          # <5% difference
        ).when(
            F.col("revenue_delta_eur") > 0, F.lit("airbnb")
        ).otherwise(F.lit("rental"))
    )

    # Data confidence
    df = df.withColumn(
        "data_confidence",
        F.when(
            (F.col("airbnb_listing_count") >= 10) & (F.col("rental_listing_count") >= 10),
            F.lit("high")
        ).when(
            (F.col("airbnb_listing_count") >= 5) | (F.col("rental_listing_count") >= 5),
            F.lit("medium")
        ).otherwise(F.lit("low"))
    )

    # Rank PC4s by absolute airbnb revenue (best investment opportunities)
    window = Window.orderBy(F.desc("airbnb_avg_annual_revenue_eur"))
    df = df.withColumn("airbnb_revenue_rank", F.rank().over(window))

    log_row_count(df, "Investment comparison gold: %d PC4 areas")

    if output_path:
        write_parquet(df, output_path)

    return df


# ---------------------------------------------------------------------------
# Top opportunities summary
# ---------------------------------------------------------------------------

def top_investment_opportunities(
    comparison_df: DataFrame,
    n: int = 20,
    confidence: str = "medium",
) -> DataFrame:
    """Return the top N PC4 areas by Airbnb revenue potential.

    Filters to areas with at least `confidence` data quality and where
    Airbnb beats or equals rental income.

    Args:
        comparison_df: Output of build_investment_comparison().
        n: Number of top areas to return.
        confidence: Minimum confidence level ('high', 'medium', 'low').

    Returns:
        Sorted DataFrame of top investment opportunities.
    """
    conf_rank = {"high": 3, "medium": 2, "low": 1}
    rank = conf_rank.get(confidence, 2)

    return (
        comparison_df
        .filter(
            F.col("data_confidence").isin(
                [k for k, v in conf_rank.items() if v >= rank]
            )
        )
        .filter(
            F.col("recommendation").isin(["airbnb", "neutral", "airbnb_only"])
        )
        .orderBy(F.desc("airbnb_avg_annual_revenue_eur"))
        .limit(n)
    )
