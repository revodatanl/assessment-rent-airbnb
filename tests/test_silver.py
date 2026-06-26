"""Unit tests for silver cleaning layer."""

import json

import pytest
from pyspark.sql import functions as F

from rent_airbnb.bronze import ingest_airbnb_bronze, ingest_rentals_bronze
from rent_airbnb.silver import clean_airbnb_silver, clean_rentals_silver


@pytest.fixture()
def airbnb_bronze(spark, tmp_path):
    csv = """zipcode,latitude,longitude,room_type,accommodates,bedrooms,price,review_scores_value
1053,52.373,4.868,Entire home/apt,4,2.0,130,100.0
,52.365,4.941,Private room,2,1.0,59,90.0
1016 AM,52.369,4.892,Entire home/apt,3,1.0,200,95.0
1055XP,52.370,4.860,Entire home/apt,2,1.0,180,88.0
1017,52.371,4.893,Private room,1,,25,80.0
9999,52.400,5.000,Entire home/apt,2,1.0,150,90.0
1020,52.380,4.870,Entire home/apt,4,2.0,9000,99.0
"""
    p = tmp_path / "airbnb.csv"
    p.write_text(csv)
    return ingest_airbnb_bronze(spark, str(p))


@pytest.fixture()
def rentals_bronze(spark, tmp_path):
    records = [
        {"_id": ["r1"], "city": "Amsterdam", "postalCode": "1053AB",
         "rent": "€ 1200,-", "areaSqm": "45 m2", "additionalCostsRaw": "\n€ 100\n",
         "propertyType": "Apartment", "isRoomActive": "true",
         "furnish": "Furnished", "energyLabel": "A",
         "latitude": "52.3700", "longitude": "4.8700",
         "crawledAt": ["2019-07-26T22:18:23.018+0000"],
         "firstSeenAt": ["2019-07-14T11:25:46.511+0000"],
         "lastSeenAt": ["2019-07-26T22:18:23.142+0000"]},
        {"_id": ["r2"], "city": "Amsterdam", "postalCode": "1016PK",
         "rent": "€ 800,-", "areaSqm": "22 m2", "additionalCostsRaw": "\n€ 0\n",
         "propertyType": "Room", "isRoomActive": "true",
         "furnish": "Unfurnished", "energyLabel": "B",
         "latitude": "52.3680", "longitude": "4.8800",
         "crawledAt": ["2019-07-26T22:18:23.018+0000"],
         "firstSeenAt": ["2019-07-14T11:25:46.511+0000"],
         "lastSeenAt": ["2019-07-26T22:18:23.142+0000"]},
        {"_id": ["r3"], "city": "Rotterdam", "postalCode": "3074HN",  # excluded
         "rent": "€ 500,-", "areaSqm": "14 m2", "additionalCostsRaw": "\n€ 50\n",
         "propertyType": "Room", "isRoomActive": "true",
         "furnish": "Unfurnished", "energyLabel": "Unknown",
         "latitude": "51.896", "longitude": "4.514",
         "crawledAt": ["2019-07-26T22:18:23.018+0000"],
         "firstSeenAt": ["2019-07-14T11:25:46.511+0000"],
         "lastSeenAt": ["2019-07-26T22:18:23.142+0000"]},
        {"_id": ["r4"], "city": "Amsterdam", "postalCode": "1020ZZ",
         "rent": "€ 1,-", "areaSqm": "5 m2", "additionalCostsRaw": "",  # invalid: rent too low
         "propertyType": "Room", "isRoomActive": "false",
         "furnish": "Unfurnished", "energyLabel": "G",
         "latitude": "52.3800", "longitude": "4.8600",
         "crawledAt": ["2019-07-26T22:18:23.018+0000"],
         "firstSeenAt": ["2019-07-14T11:25:46.511+0000"],
         "lastSeenAt": ["2019-07-26T22:18:23.142+0000"]},
    ]
    p = tmp_path / "rentals.json"
    p.write_text(json.dumps(records))
    return ingest_rentals_bronze(spark, str(p))


# ---------------------------------------------------------------------------
# Airbnb silver tests
# ---------------------------------------------------------------------------

class TestAirbnbSilver:

    def test_pc4_extraction_plain(self, spark, airbnb_bronze):
        df = clean_airbnb_silver(spark, airbnb_bronze)
        rows = {r["zipcode_raw"]: r["pc4"] for r in df.select("zipcode_raw", "pc4").collect()}
        assert rows.get("1053") == "1053"

    def test_pc4_extraction_with_suffix(self, spark, airbnb_bronze):
        df = clean_airbnb_silver(spark, airbnb_bronze)
        rows = {r["zipcode_raw"]: r["pc4"] for r in df.select("zipcode_raw", "pc4").collect()}
        assert rows.get("1016 AM") == "1016"
        assert rows.get("1055XP") == "1055"

    def test_non_amsterdam_pc4_excluded(self, spark, airbnb_bronze):
        df = clean_airbnb_silver(spark, airbnb_bronze)
        bad = df.filter(F.col("pc4") == "9999")
        assert bad.count() == 0

    def test_price_cast_to_double(self, spark, airbnb_bronze):
        df = clean_airbnb_silver(spark, airbnb_bronze)
        dtype = dict(df.dtypes)["price_eur_night"]
        assert dtype == "double"

    def test_extreme_price_outlier_flagged(self, spark, airbnb_bronze):
        # The 9000 listing is quarantined (above 5000 validity cap) not in silver.
        # The outlier column should exist and be boolean on whatever passes validation.
        df = clean_airbnb_silver(spark, airbnb_bronze)
        assert "is_price_outlier" in df.columns
        assert dict(df.dtypes)["is_price_outlier"] == "boolean"

    def test_missing_zipcode_records_absent(self, spark, airbnb_bronze):
        """Records with no zipcode AND no geo-enrichment should be quarantined."""
        df = clean_airbnb_silver(spark, airbnb_bronze)
        null_pc4 = df.filter(F.col("pc4").isNull() | (F.col("pc4") == ""))
        assert null_pc4.count() == 0

    def test_silver_layer_tag(self, spark, airbnb_bronze):
        df = clean_airbnb_silver(spark, airbnb_bronze)
        assert df.filter(F.col("_layer") == "silver").count() == df.count()

    def test_numeric_types(self, spark, airbnb_bronze):
        df = clean_airbnb_silver(spark, airbnb_bronze)
        dtypes = dict(df.dtypes)
        assert dtypes["latitude"] == "double"
        assert dtypes["longitude"] == "double"
        assert dtypes["accommodates"] == "int"


# ---------------------------------------------------------------------------
# Rentals silver tests
# ---------------------------------------------------------------------------

class TestRentalsSilver:

    def test_amsterdam_only(self, spark, rentals_bronze):
        df = clean_rentals_silver(spark, rentals_bronze)
        cities = {r["city"] for r in df.select("city").collect()}
        assert cities == {"Amsterdam"}

    def test_rent_parsed_correctly(self, spark, rentals_bronze):
        df = clean_rentals_silver(spark, rentals_bronze)
        r1 = df.filter(F.col("listing_id") == "r1").first()
        assert r1 is not None
        assert abs(r1["rent_eur_month"] - 1200.0) < 0.01

    def test_additional_costs_parsed(self, spark, rentals_bronze):
        df = clean_rentals_silver(spark, rentals_bronze)
        r1 = df.filter(F.col("listing_id") == "r1").first()
        assert r1["additional_costs_eur"] == 100.0

    def test_area_parsed(self, spark, rentals_bronze):
        df = clean_rentals_silver(spark, rentals_bronze)
        r1 = df.filter(F.col("listing_id") == "r1").first()
        assert r1["area_sqm"] == 45

    def test_pc4_extracted_from_postal_code(self, spark, rentals_bronze):
        df = clean_rentals_silver(spark, rentals_bronze)
        r1 = df.filter(F.col("listing_id") == "r1").first()
        assert r1["pc4"] == "1053"

    def test_invalid_rent_quarantined(self, spark, rentals_bronze):
        df = clean_rentals_silver(spark, rentals_bronze)
        # r4 has rent = €1 which is below the 200 floor
        r4 = df.filter(F.col("listing_id") == "r4")
        assert r4.count() == 0

    def test_additional_costs_null_becomes_zero(self, spark, rentals_bronze):
        """Empty additionalCostsRaw should default to 0 not null."""
        df = clean_rentals_silver(spark, rentals_bronze)
        nulls = df.filter(F.col("additional_costs_eur").isNull())
        assert nulls.count() == 0
