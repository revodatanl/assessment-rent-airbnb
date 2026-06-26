"""Tests for streaming batch handler (L4)."""

import json

import pytest

from rent_airbnb.bronze import ingest_airbnb_bronze, normalize_rentals_bronze_df
from rent_airbnb.pipeline import PipelineConfig
from rent_airbnb.silver import clean_airbnb_silver
from rent_airbnb.streaming import process_rentals_stream_batch


@pytest.fixture()
def pipeline_config(tmp_path):
    output = tmp_path / "output"
    geo = tmp_path / "geo"
    geo.mkdir()
    (geo / "post_codes.geojson").write_text(
        '{"type":"FeatureCollection","features":[]}'
    )
    return PipelineConfig(
        airbnb_csv_path=str(tmp_path / "airbnb.csv"),
        rentals_json_path=str(tmp_path / "rentals.json"),
        post_codes_geojson_path=str(geo / "post_codes.geojson"),
        output_base=str(output),
        rentals_stream_dir=str(tmp_path / "stream"),
        streaming_checkpoint_path=str(output / "_checkpoints"),
        geo_enrich=False,
    )


@pytest.fixture()
def airbnb_ready(spark, pipeline_config, tmp_path):
    csv = tmp_path / "airbnb.csv"
    csv.write_text(
        "zipcode,latitude,longitude,room_type,accommodates,bedrooms,price,review_scores_value\n"
        "1053,52.37,4.87,Entire home/apt,2,1.0,130,90.0\n"
    )
    bronze = ingest_airbnb_bronze(spark, str(csv))
    clean_airbnb_silver(
        spark,
        bronze,
        output_path=pipeline_config.silver_airbnb_path,
    )


def test_process_stream_batch_appends_and_refreshes_gold(
    spark, pipeline_config, airbnb_ready, tmp_path
):
    record = {
        "_id": ["s1"],
        "city": "Amsterdam",
        "postalCode": "1053AB",
        "rent": "€ 1100,-",
        "areaSqm": "40 m2",
        "additionalCostsRaw": "€ 80",
        "propertyType": "Apartment",
        "isRoomActive": "true",
        "furnish": "Furnished",
        "energyLabel": "A",
        "latitude": "52.37",
        "longitude": "4.87",
        "crawledAt": ["2019-01-01T00:00:00.000+0000"],
        "firstSeenAt": ["2019-01-01T00:00:00.000+0000"],
        "lastSeenAt": ["2019-01-01T00:00:00.000+0000"],
    }
    p = tmp_path / "batch.json"
    p.write_text(json.dumps(record))
    batch = spark.read.option("multiLine", True).json(str(p))
    batch = normalize_rentals_bronze_df(
        batch, source_file=str(pipeline_config.rentals_stream_dir), dataset="rentals_stream"
    )

    process_rentals_stream_batch(batch, batch_id=1, spark=spark, config=pipeline_config)

    rentals_silver = spark.read.parquet(pipeline_config.silver_rentals_path)
    assert rentals_silver.count() >= 1

    comparison = spark.read.parquet(pipeline_config.gold_comparison_path)
    assert comparison.count() >= 1
