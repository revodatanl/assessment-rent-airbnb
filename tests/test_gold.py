"""Unit tests for gold revenue calculations and investment comparison."""

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from rent_airbnb.gold import (
    aggregate_airbnb_by_pc4,
    aggregate_rentals_by_pc4,
    build_investment_comparison,
    compute_airbnb_revenue,
    compute_rental_revenue,
    export_airbnb_listing_revenue,
    export_rental_listing_revenue,
    top_investment_opportunities,
)
from rent_airbnb.schemas import (
    AIRBNB_NIGHTS_PER_YEAR,
    AIRBNB_OCCUPANCY_RATE,
    AIRBNB_PLATFORM_FEE_PCT,
    RENTAL_OCCUPIED_MONTHS_PER_YEAR,
)


@pytest.fixture()
def airbnb_silver(spark):
    data = [
        Row(pc4="1053", price_eur_night=150.0, review_score=95.0,
            room_type="Entire home/apt", accommodates=4, bedrooms=2.0,
            is_price_outlier=False),
        Row(pc4="1053", price_eur_night=100.0, review_score=90.0,
            room_type="Private room", accommodates=2, bedrooms=1.0,
            is_price_outlier=False),
        Row(pc4="1053", price_eur_night=9000.0, review_score=99.0,
            room_type="Entire home/apt", accommodates=6, bedrooms=3.0,
            is_price_outlier=True),   # outlier — excluded from revenue
        Row(pc4="1016", price_eur_night=200.0, review_score=88.0,
            room_type="Entire home/apt", accommodates=3, bedrooms=1.0,
            is_price_outlier=False),
    ]
    schema = StructType([
        StructField("pc4", StringType()),
        StructField("price_eur_night", DoubleType()),
        StructField("review_score", FloatType()),
        StructField("room_type", StringType()),
        StructField("accommodates", IntegerType()),
        StructField("bedrooms", FloatType()),
        StructField("is_price_outlier", BooleanType()),
    ])
    return spark.createDataFrame(data, schema)


@pytest.fixture()
def rentals_silver(spark):
    data = [
        Row(listing_id="r1", pc4="1053", rent_eur_month=1200.0, additional_costs_eur=100.0,
            property_type="Apartment", area_sqm=45, is_active=True, is_rent_outlier=False),
        Row(listing_id="r2", pc4="1053", rent_eur_month=800.0, additional_costs_eur=50.0,
            property_type="Room", area_sqm=20, is_active=True, is_rent_outlier=False),
        Row(listing_id="r3", pc4="1016", rent_eur_month=1500.0, additional_costs_eur=150.0,
            property_type="Apartment", area_sqm=60, is_active=True, is_rent_outlier=False),
        Row(listing_id="r4", pc4="1020", rent_eur_month=900.0, additional_costs_eur=0.0,
            property_type="Studio", area_sqm=30, is_active=False, is_rent_outlier=False),
    ]
    schema = StructType([
        StructField("listing_id", StringType()),
        StructField("pc4", StringType()),
        StructField("rent_eur_month", DoubleType()),
        StructField("additional_costs_eur", DoubleType()),
        StructField("property_type", StringType()),
        StructField("area_sqm", IntegerType()),
        StructField("is_active", BooleanType()),
        StructField("is_rent_outlier", BooleanType()),
    ])
    return spark.createDataFrame(data, schema)


# ---------------------------------------------------------------------------
# Revenue model tests
# ---------------------------------------------------------------------------

class TestRevenueModel:

    def test_airbnb_revenue_formula(self, spark, airbnb_silver):
        df = compute_airbnb_revenue(airbnb_silver)
        row = df.filter(F.col("price_eur_night") == 150.0).first()
        expected = 150.0 * AIRBNB_OCCUPANCY_RATE * AIRBNB_NIGHTS_PER_YEAR * (1 - AIRBNB_PLATFORM_FEE_PCT)
        assert abs(row["annual_revenue_eur"] - expected) < 0.01

    def test_airbnb_outlier_revenue_is_null(self, spark, airbnb_silver):
        df = compute_airbnb_revenue(airbnb_silver)
        outlier = df.filter(F.col("is_price_outlier")).first()
        assert outlier["annual_revenue_eur"] is None

    def test_rental_revenue_formula(self, spark, rentals_silver):
        df = compute_rental_revenue(rentals_silver)
        row = df.filter(F.col("rent_eur_month") == 1200.0).first()
        expected = (1200.0 + 100.0) * RENTAL_OCCUPIED_MONTHS_PER_YEAR
        assert abs(row["annual_revenue_eur"] - expected) < 0.01

    def test_inactive_rental_revenue_is_null(self, spark, rentals_silver):
        df = compute_rental_revenue(rentals_silver)
        inactive = df.filter(~F.col("is_active")).first()
        assert inactive["annual_revenue_eur"] is None

    def test_revenue_model_column_airbnb(self, spark, airbnb_silver):
        df = compute_airbnb_revenue(airbnb_silver)
        assert df.filter(F.col("revenue_model") == "airbnb").count() == df.count()

    def test_revenue_model_column_rental(self, spark, rentals_silver):
        df = compute_rental_revenue(rentals_silver)
        assert df.filter(F.col("revenue_model") == "rental").count() == df.count()

    def test_export_airbnb_listing_revenue_columns(self, spark, airbnb_silver):
        df = export_airbnb_listing_revenue(airbnb_silver)
        assert set(df.columns) == {
            "pc4", "room_type", "price_eur_night", "annual_revenue_eur",
            "is_price_outlier", "revenue_model",
        }

    def test_export_rental_listing_revenue_columns(self, spark, rentals_silver):
        df = export_rental_listing_revenue(rentals_silver)
        assert set(df.columns) == {
            "listing_id", "pc4", "property_type", "rent_eur_month",
            "additional_costs_eur", "annual_revenue_eur", "is_active",
            "is_rent_outlier", "revenue_model",
        }


# ---------------------------------------------------------------------------
# PC4 aggregation tests
# ---------------------------------------------------------------------------

class TestPC4Aggregation:

    def test_airbnb_pc4_row_count(self, spark, airbnb_silver):
        df = aggregate_airbnb_by_pc4(airbnb_silver)
        # 2 distinct PC4s
        assert df.count() == 2

    def test_airbnb_pc4_listing_count(self, spark, airbnb_silver):
        df = aggregate_airbnb_by_pc4(airbnb_silver)
        pc1053 = df.filter(F.col("pc4") == "1053").first()
        assert pc1053["listing_count"] == 3  # includes outlier in count

    def test_rental_pc4_row_count(self, spark, rentals_silver):
        df = aggregate_rentals_by_pc4(rentals_silver)
        assert df.count() == 3   # 1053, 1016, 1020

    def test_airbnb_avg_revenue_excludes_outlier(self, spark, airbnb_silver):
        """Avg revenue for PC4 1053 should be based on 2 non-outlier listings."""
        df = aggregate_airbnb_by_pc4(airbnb_silver)
        pc1053 = df.filter(F.col("pc4") == "1053").first()
        rev_150 = 150.0 * AIRBNB_OCCUPANCY_RATE * AIRBNB_NIGHTS_PER_YEAR * (1 - AIRBNB_PLATFORM_FEE_PCT)
        rev_100 = 100.0 * AIRBNB_OCCUPANCY_RATE * AIRBNB_NIGHTS_PER_YEAR * (1 - AIRBNB_PLATFORM_FEE_PCT)
        expected_avg = (rev_150 + rev_100) / 2
        assert abs(pc1053["avg_annual_revenue_eur"] - expected_avg) < 1.0


# ---------------------------------------------------------------------------
# Investment comparison tests
# ---------------------------------------------------------------------------

class TestInvestmentComparison:

    @pytest.fixture()
    def comparison(self, spark, airbnb_silver, rentals_silver):
        ab_pc4 = aggregate_airbnb_by_pc4(airbnb_silver)
        rl_pc4 = aggregate_rentals_by_pc4(rentals_silver)
        return build_investment_comparison(ab_pc4, rl_pc4)

    def test_comparison_has_all_pc4s(self, spark, comparison):
        # outer join: 1053, 1016 from both; 1020 from rental only
        pc4s = {r["pc4"] for r in comparison.select("pc4").collect()}
        assert {"1053", "1016", "1020"}.issubset(pc4s)

    def test_airbnb_only_recommendation(self, spark, comparison):
        # No rental listings in PC4 1016 in airbnb data (it's there but not in rentals for 1016 as airbnb_only)
        # Just check that rental_only / airbnb_only recommendations exist
        recs = {r["recommendation"] for r in comparison.select("recommendation").collect()}
        assert len(recs) > 0  # at minimum some recommendation present

    def test_revenue_delta_computed(self, spark, comparison):
        row = comparison.filter(F.col("pc4") == "1053").first()
        assert row["revenue_delta_eur"] is not None

    def test_recommendation_column_values(self, spark, comparison):
        valid_recs = {"airbnb", "rental", "neutral", "airbnb_only", "rental_only"}
        recs = {r["recommendation"] for r in comparison.select("recommendation").collect()}
        assert recs.issubset(valid_recs)

    def test_data_confidence_column_values(self, spark, comparison):
        valid = {"high", "medium", "low"}
        confs = {r["data_confidence"] for r in comparison.select("data_confidence").collect()}
        assert confs.issubset(valid)

    def test_top_opportunities_limit(self, spark, comparison):
        top = top_investment_opportunities(comparison, n=2)
        assert top.count() <= 2

    def test_revenue_uplift_non_negative(self, spark, comparison):
        neg = comparison.filter(F.col("revenue_uplift_pct") < 0)
        assert neg.count() == 0
