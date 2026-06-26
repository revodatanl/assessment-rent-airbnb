"""Tests for data quality expectations."""

from rent_airbnb.quality import (
    airbnb_expectation_columns,
    airbnb_silver_valid,
    expectation_summary,
    rentals_silver_valid,
)


def test_airbnb_valid_condition(spark):
    df = spark.createDataFrame([
        ("1053", 130.0, "Entire home/apt"),
        ("", 130.0, "Entire home/apt"),
        ("1053", 9000.0, "Entire home/apt"),
        ("1053", 130.0, "Invalid"),
    ], ["pc4", "price_eur_night", "room_type"])

    valid = df.filter(airbnb_silver_valid())
    assert valid.count() == 1
    assert valid.first()["pc4"] == "1053"


def test_rentals_valid_condition(spark):
    df = spark.createDataFrame([
        ("1053", 1200.0, "Apartment"),
        ("", 1200.0, "Apartment"),
        ("1053", 50.0, "Apartment"),
    ], ["pc4", "rent_eur_month", "property_type"])

    valid = df.filter(rentals_silver_valid())
    assert valid.count() == 1


def test_expectation_summary(spark):
    df = spark.createDataFrame([
        ("1053", 130.0, "Entire home/apt"),
        ("1054", 200.0, "Private room"),
    ], ["pc4", "price_eur_night", "room_type"])

    summary = expectation_summary(df, airbnb_expectation_columns())
    assert summary["valid_pc4"] == 1.0
    assert summary["valid_price"] == 1.0
