"""Tests for Databricks job entry points."""

from rent_airbnb.jobs import _config_from_args
from rent_airbnb.pipeline import PipelineConfig


def test_config_from_args_defaults():
    args = type("Args", (), {
        "airbnb_csv": "data/input/airbnb.csv",
        "rentals_json": "data/input/rentals.json",
        "post_codes_geojson": "data/input/geo/post_codes.geojson",
        "output_base": "data/output",
    })()
    config = _config_from_args(args)
    assert isinstance(config, PipelineConfig)
    assert config.output_base == "data/output"
    assert config.airbnb_csv_path.endswith("airbnb.csv")
