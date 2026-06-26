# %% [markdown]
# # Amsterdam Property Investment Pipeline
# **Medallion Architecture: Bronze → Silver → Gold**
#
# Answers: *For each Amsterdam PC4, is Airbnb or long-term rental (Kamernet) more profitable?*
#
# ## Setup

# %%
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from rent_airbnb.pipeline import PipelineConfig, get_or_create_spark, run_bronze, run_silver, run_gold
from pyspark.sql import functions as F

config = PipelineConfig(
    airbnb_csv_path=str(REPO_ROOT / "data/input/airbnb.csv"),
    rentals_json_path=str(REPO_ROOT / "data/input/rentals.json"),
    post_codes_geojson_path=str(REPO_ROOT / "data/input/geo/post_codes.geojson"),
    output_base=str(REPO_ROOT / "data/output"),
    geo_enrich=True,
    write_quarantine=True,
)

spark = get_or_create_spark(config)

# %% [markdown]
# ## Bronze — Raw Ingestion
# Land data as-is. All columns as strings. Full audit trail.

# %%
airbnb_bronze, rentals_bronze = run_bronze(spark, config)

print("Airbnb bronze:", airbnb_bronze.count(), "rows")
print("Rentals bronze:", rentals_bronze.count(), "rows")
airbnb_bronze.show(3)
rentals_bronze.select("_id", "city", "postalCode", "rent", "propertyType").show(3)

# %% [markdown]
# ## Silver — Clean & Type
# Parse, validate, geo-enrich missing zipcodes, quarantine bad records.

# %%
airbnb_silver, rentals_silver = run_silver(spark, config, airbnb_bronze, rentals_bronze)

print("\n=== AIRBNB SILVER ===")
print("Rows:", airbnb_silver.count())
airbnb_silver.printSchema()
airbnb_silver.show(5)

# %%
print("\n=== RENTALS SILVER ===")
print("Rows:", rentals_silver.count())
rentals_silver.printSchema()
rentals_silver.show(5)

# %%
# Data quality checks
print("Airbnb — null pc4:", airbnb_silver.filter(F.col("pc4").isNull()).count())
print("Airbnb — outlier prices:", airbnb_silver.filter(F.col("is_price_outlier")).count())
print("Rentals — null rent:", rentals_silver.filter(F.col("rent_eur_month").isNull()).count())
print("Rentals — rent outliers:", rentals_silver.filter(F.col("is_rent_outlier")).count())

# %%
# PC4 enrichment breakdown
print("\nAirbnb PC4 source breakdown:")
airbnb_silver.groupBy("pc4_source").count().show()

# %% [markdown]
# ## Gold — Revenue Aggregation & Investment Comparison

# %%
airbnb_pc4, rentals_pc4, comparison, top = run_gold(spark, config, airbnb_silver, rentals_silver)

# %%
print("=== AIRBNB BY PC4 ===")
(airbnb_pc4
 .orderBy(F.desc("avg_annual_revenue_eur"))
 .select("pc4", "listing_count", "median_price_eur_night",
         "avg_annual_revenue_eur", "avg_review_score", "pct_entire_home")
 .show(15))

# %%
print("=== RENTALS BY PC4 ===")
(rentals_pc4
 .orderBy(F.desc("avg_annual_revenue_eur"))
 .select("pc4", "listing_count", "median_rent_eur_month",
         "avg_annual_revenue_eur", "avg_area_sqm")
 .show(15))

# %%
print("=== INVESTMENT COMPARISON ===")
(comparison
 .filter(F.col("data_confidence").isin(["high", "medium"]))
 .orderBy(F.desc("airbnb_avg_annual_revenue_eur"))
 .select(
     "pc4",
     "airbnb_listing_count",
     F.round("airbnb_avg_annual_revenue_eur", 0).alias("airbnb_annual_eur"),
     "rental_listing_count",
     F.round("rental_avg_annual_revenue_eur", 0).alias("rental_annual_eur"),
     F.round("revenue_delta_eur", 0).alias("delta_eur"),
     F.round("revenue_uplift_pct", 1).alias("uplift_pct"),
     "recommendation",
     "data_confidence",
 )
 .show(25))

# %%
print("=== TOP 20 INVESTMENT OPPORTUNITIES (Airbnb-first) ===")
top.select(
    "pc4", "airbnb_revenue_rank",
    F.round("airbnb_avg_annual_revenue_eur", 0).alias("airbnb_annual_eur"),
    F.round("rental_avg_annual_revenue_eur", 0).alias("rental_annual_eur"),
    "recommendation", "data_confidence",
).show(20)

# %% [markdown]
# ## Summary statistics

# %%
print("Recommendation breakdown across all PC4s with medium+ confidence:")
(comparison
 .filter(F.col("data_confidence").isin(["high", "medium"]))
 .groupBy("recommendation")
 .agg(
     F.count("*").alias("pc4_count"),
     F.round(F.avg("revenue_uplift_pct"), 1).alias("avg_uplift_pct"),
 )
 .orderBy(F.desc("pc4_count"))
 .show())

# %%
# Revenue model assumptions
from rent_airbnb.schemas import (
    AIRBNB_OCCUPANCY_RATE, AIRBNB_PLATFORM_FEE_PCT,
    AIRBNB_NIGHTS_PER_YEAR, RENTAL_OCCUPIED_MONTHS_PER_YEAR
)
print("Revenue model assumptions:")
print(f"  Airbnb occupancy rate: {AIRBNB_OCCUPANCY_RATE*100:.0f}%")
print(f"  Airbnb host fee: {AIRBNB_PLATFORM_FEE_PCT*100:.0f}%")
print(f"  Airbnb nights/year: {AIRBNB_NIGHTS_PER_YEAR}")
print(f"  Rental occupied months/year: {RENTAL_OCCUPIED_MONTHS_PER_YEAR}")

print("\nOutput Parquet files:")
print(f"  Bronze: {config.bronze_airbnb_path}")
print(f"  Bronze: {config.bronze_rentals_path}")
print(f"  Silver: {config.silver_airbnb_path}")
print(f"  Silver: {config.silver_rentals_path}")
print(f"  Gold:   {config.gold_comparison_path}")
print(f"  Gold:   {config.gold_top_opportunities_path}")
