"""Unit tests for bronze ingestion layer."""

import json

import pytest

from rent_airbnb.bronze import ingest_airbnb_bronze, ingest_rentals_bronze


@pytest.fixture()
def sample_airbnb_csv(tmp_path):
    csv_content = """zipcode,latitude,longitude,room_type,accommodates,bedrooms,price,review_scores_value
1053,52.373,4.868,Entire home/apt,4,2.0,130,100.0
,52.365,4.941,Private room,2,1.0,59,90.0
1017 AM,52.369,4.892,Entire home/apt,3,1.0,200,95.0
"""
    p = tmp_path / "airbnb.csv"
    p.write_text(csv_content)
    return str(p)


@pytest.fixture()
def sample_rentals_json(tmp_path):
    records = [
        {
            "_id": ["abc123"],
            "city": "Amsterdam",
            "postalCode": "1053AB",
            "rent": "€ 1200,-",
            "areaSqm": "45 m2",
            "additionalCostsRaw": "\n€ 100\n",
            "propertyType": "Apartment",
            "isRoomActive": "true",
            "furnish": "Furnished",
            "energyLabel": "A",
            "latitude": "52.3700",
            "longitude": "4.8700",
            "crawledAt": ["2019-07-26T22:18:23.018+0000"],
            "firstSeenAt": ["2019-07-14T11:25:46.511+0000"],
            "lastSeenAt": ["2019-07-26T22:18:23.142+0000"],
        },
        {
            "_id": ["def456"],
            "city": "Rotterdam",     # should be excluded in silver
            "postalCode": "3074HN",
            "rent": "€ 500,-",
            "areaSqm": "14 m2",
            "additionalCostsRaw": "\n€ 50\n",
            "propertyType": "Room",
            "isRoomActive": "true",
            "furnish": "Unfurnished",
            "energyLabel": "Unknown",
            "latitude": "51.896",
            "longitude": "4.514",
            "crawledAt": ["2019-07-26T22:18:23.018+0000"],
            "firstSeenAt": ["2019-07-14T11:25:46.511+0000"],
            "lastSeenAt": ["2019-07-26T22:18:23.142+0000"],
        },
    ]
    p = tmp_path / "rentals.json"
    p.write_text(json.dumps(records))
    return str(p)


# ---------------------------------------------------------------------------
# Airbnb bronze tests
# ---------------------------------------------------------------------------

class TestAirbnbBronze:

    def test_row_count(self, spark, sample_airbnb_csv):
        df = ingest_airbnb_bronze(spark, sample_airbnb_csv)
        assert df.count() == 3

    def test_all_columns_present(self, spark, sample_airbnb_csv):
        df = ingest_airbnb_bronze(spark, sample_airbnb_csv)
        expected = {"zipcode", "latitude", "longitude", "room_type",
                    "accommodates", "bedrooms", "price", "review_scores_value",
                    "_ingested_at", "_source_file", "_layer", "_dataset"}
        assert expected.issubset(set(df.columns))

    def test_metadata_columns(self, spark, sample_airbnb_csv):
        df = ingest_airbnb_bronze(spark, sample_airbnb_csv)
        row = df.first()
        assert row["_layer"] == "bronze"
        assert row["_dataset"] == "airbnb"
        assert sample_airbnb_csv in row["_source_file"]

    def test_all_columns_are_strings(self, spark, sample_airbnb_csv):
        """Bronze schema: all source columns land as strings."""
        df = ingest_airbnb_bronze(spark, sample_airbnb_csv)
        source_cols = ["zipcode", "latitude", "longitude", "price"]
        for col_name, dtype in df.dtypes:
            if col_name in source_cols:
                assert dtype == "string", f"{col_name} should be string in bronze, got {dtype}"

    def test_missing_zipcode_preserved(self, spark, sample_airbnb_csv):
        """Empty zipcode rows must be kept at bronze — quarantine happens in silver."""
        df = ingest_airbnb_bronze(spark, sample_airbnb_csv)
        missing = df.filter(df["zipcode"].isNull() | (df["zipcode"] == ""))
        assert missing.count() == 1

    def test_write_parquet(self, spark, sample_airbnb_csv, tmp_path):
        out = str(tmp_path / "out")
        ingest_airbnb_bronze(spark, sample_airbnb_csv, output_path=out)
        read_back = spark.read.parquet(out)
        assert read_back.count() == 3


# ---------------------------------------------------------------------------
# Rentals bronze tests
# ---------------------------------------------------------------------------

class TestRentalsBronze:

    def test_row_count(self, spark, sample_rentals_json):
        df = ingest_rentals_bronze(spark, sample_rentals_json)
        assert df.count() == 2

    def test_array_fields_flattened(self, spark, sample_rentals_json):
        """_id, crawledAt etc. should be scalar strings, not arrays."""
        df = ingest_rentals_bronze(spark, sample_rentals_json)
        id_col = dict(df.dtypes)["_id"]
        assert id_col == "string", f"_id should be string, got {id_col}"
        row = df.filter(df["city"] == "Amsterdam").first()
        assert row["_id"] == "abc123"

    def test_metadata_columns(self, spark, sample_rentals_json):
        df = ingest_rentals_bronze(spark, sample_rentals_json)
        row = df.first()
        assert row["_layer"] == "bronze"
        assert row["_dataset"] == "rentals"

    def test_both_cities_present(self, spark, sample_rentals_json):
        df = ingest_rentals_bronze(spark, sample_rentals_json)
        cities = {r["city"] for r in df.select("city").collect()}
        assert "Amsterdam" in cities
        assert "Rotterdam" in cities
