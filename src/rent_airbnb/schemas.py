"""Canonical schemas and constants for the rent-airbnb pipeline."""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Bronze schemas (raw, minimal coercion)
# ---------------------------------------------------------------------------

AIRBNB_BRONZE_SCHEMA = StructType([
    StructField("zipcode", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("room_type", StringType(), True),
    StructField("accommodates", StringType(), True),
    StructField("bedrooms", StringType(), True),
    StructField("price", StringType(), True),
    StructField("review_scores_value", StringType(), True),
])

RENTALS_BRONZE_SCHEMA = StructType([
    StructField("_id", StringType(), True),
    StructField("additionalCostsRaw", StringType(), True),
    StructField("areaSqm", StringType(), True),
    StructField("city", StringType(), True),
    StructField("crawlStatus", StringType(), True),
    StructField("crawledAt", StringType(), True),
    StructField("deposit", StringType(), True),
    StructField("descriptionTranslated", StringType(), True),
    StructField("energyLabel", StringType(), True),
    StructField("firstSeenAt", StringType(), True),
    StructField("furnish", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("internet", StringType(), True),
    StructField("isRoomActive", StringType(), True),
    StructField("kitchen", StringType(), True),
    StructField("lastSeenAt", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("postalCode", StringType(), True),
    StructField("propertyType", StringType(), True),
    StructField("rent", StringType(), True),
    StructField("title", StringType(), True),
    StructField("source", StringType(), True),
])

# ---------------------------------------------------------------------------
# Silver schemas (typed, clean)
# ---------------------------------------------------------------------------

AIRBNB_SILVER_SCHEMA = StructType([
    StructField("zipcode_raw", StringType(), True),
    StructField("pc4", StringType(), True),           # 4-digit postal code
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("room_type", StringType(), True),
    StructField("accommodates", IntegerType(), True),
    StructField("bedrooms", FloatType(), True),
    StructField("price_eur_night", DoubleType(), True),
    StructField("review_score", FloatType(), True),
    StructField("pc4_source", StringType(), True),    # "raw" | "geo_enriched"
])

RENTALS_SILVER_SCHEMA = StructType([
    StructField("listing_id", StringType(), True),
    StructField("pc4", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("property_type", StringType(), True),
    StructField("area_sqm", IntegerType(), True),
    StructField("rent_eur_month", DoubleType(), True),
    StructField("additional_costs_eur", DoubleType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("furnish", StringType(), True),
    StructField("energy_label", StringType(), True),
    StructField("city", StringType(), True),
])

# ---------------------------------------------------------------------------
# Revenue assumptions
# ---------------------------------------------------------------------------

# Airbnb: average occupancy rate for Amsterdam (source: AirDNA / Inside Airbnb)
AIRBNB_OCCUPANCY_RATE = 0.65          # 65% of nights booked
AIRBNB_PLATFORM_FEE_PCT = 0.03        # 3% host service fee
AIRBNB_NIGHTS_PER_YEAR = 365

# Kamernet: typical lease has minimal vacancy (assume 11 months revenue/year)
RENTAL_OCCUPIED_MONTHS_PER_YEAR = 11

# Canonical Amsterdam PC4 range
AMSTERDAM_PC4_MIN = 1000
AMSTERDAM_PC4_MAX = 1109

VALID_ROOM_TYPES = {"Entire home/apt", "Private room", "Shared room"}
VALID_PROPERTY_TYPES = {"Room", "Apartment", "Studio", "Anti-squat", "Student residence"}
