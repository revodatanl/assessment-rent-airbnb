# Delta Live Tables pipeline (L3) — runs on Databricks only.
# Deploy via: databricks bundle deploy && databricks bundle run rent_airbnb_dlt
#
# Expectations mirror src/rent_airbnb/quality.py

import dlt

# Pipeline configuration (set in resources/dlt_pipeline.yml)
AIRBNB_CSV = spark.conf.get("airbnb_csv", "data/input/airbnb.csv")
RENTALS_JSON = spark.conf.get("rentals_json", "data/input/rentals.json")
POST_CODES_GEO = spark.conf.get("post_codes_geojson", "data/input/geo/post_codes.geojson")
CATALOG = spark.conf.get("catalog", "main")
SCHEMA = spark.conf.get("schema", "rent_airbnb")


@dlt.table(
    name="bronze_airbnb",
    comment="Raw Airbnb listings — string-typed with lineage metadata",
)
def bronze_airbnb():
    from rent_airbnb.bronze import ingest_airbnb_bronze

    return ingest_airbnb_bronze(spark, AIRBNB_CSV)


@dlt.table(
    name="bronze_rentals",
    comment="Raw Kamernet rentals JSON",
)
def bronze_rentals():
    from rent_airbnb.bronze import ingest_rentals_bronze

    return ingest_rentals_bronze(spark, RENTALS_JSON)


@dlt.table(
    name="silver_airbnb",
    comment="Cleaned Airbnb listings with geo-enriched PC4",
)
@dlt.expect_or_drop("valid_pc4", "pc4 IS NOT NULL AND pc4 != ''")
@dlt.expect_or_drop("valid_price", "price_eur_night IS NOT NULL AND price_eur_night BETWEEN 10 AND 5000")
@dlt.expect_or_drop(
    "valid_room_type",
    "room_type IN ('Entire home/apt', 'Private room', 'Shared room')",
)
def silver_airbnb():
    from rent_airbnb.silver import clean_airbnb_silver

    bronze = dlt.read("bronze_airbnb")
    return clean_airbnb_silver(
        spark,
        bronze,
        post_codes_geojson_path=POST_CODES_GEO,
        quarantine_path=None,
    )


@dlt.table(
    name="silver_rentals",
    comment="Cleaned Amsterdam Kamernet rentals",
)
@dlt.expect_or_drop("valid_rent", "rent_eur_month IS NOT NULL AND rent_eur_month BETWEEN 200 AND 4000")
@dlt.expect_or_drop("valid_pc4", "pc4 IS NOT NULL AND pc4 != ''")
@dlt.expect_or_drop(
    "valid_property_type",
    "property_type IN ('Room', 'Apartment', 'Studio', 'Anti-squat', 'Student residence')",
)
def silver_rentals():
    from rent_airbnb.silver import clean_rentals_silver

    bronze = dlt.read("bronze_rentals")
    return clean_rentals_silver(spark, bronze, quarantine_path=None)


@dlt.table(
    name="gold_investment_comparison",
    comment="Airbnb vs rental revenue comparison per PC4",
)
def gold_investment_comparison():
    from rent_airbnb.gold import (
        aggregate_airbnb_by_pc4,
        aggregate_rentals_by_pc4,
        build_investment_comparison,
    )

    airbnb_silver = dlt.read("silver_airbnb")
    rentals_silver = dlt.read("silver_rentals")
    airbnb_pc4 = aggregate_airbnb_by_pc4(airbnb_silver)
    rentals_pc4 = aggregate_rentals_by_pc4(rentals_silver)
    return build_investment_comparison(airbnb_pc4, rentals_pc4)


@dlt.table(
    name="gold_top_opportunities",
    comment="Top 20 PC4 areas by Airbnb revenue potential",
)
def gold_top_opportunities():
    from rent_airbnb.gold import top_investment_opportunities

    comparison = dlt.read("gold_investment_comparison")
    return top_investment_opportunities(comparison, n=20)
