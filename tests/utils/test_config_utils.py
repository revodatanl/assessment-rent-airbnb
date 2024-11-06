import unittest
from unittest.mock import patch, mock_open
from pathlib import Path
import os

import sys
sys.path.append('/home/jiayin/assessment-rent-airbnb/src')
from utils.config_utils import load_config, get_output_dir, get_file_path

class TestConfigUtils(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data="""
    base_dir: '/home/jiayin/assessment-rent-airbnb'
    paths:
      data_dir: 'data'
      output_dirs:
        bronze: 'data/output/bronze'
        silver: 'data/output/silver'
        gold: 'data/output/gold'
    files:
      zip:
        rentals: 'rentals.zip'
        airbnb: 'airbnb.zip'
      geojson:
        post_codes: 'geo/post_codes.geojson'
        amsterdam_areas: 'geo/amsterdam_areas.geojson'
      bronze_parquet:
        rentals: 'rentals.parquet'
        airbnb: 'airbnb.parquet'
      silver_parquet:
        rentals: 'rentals_cleaned.parquet'
        airbnb: 'airbnb_cleaned.parquet'
      gold_parquet:
        output: 'average_revenue_per_postcode.parquet'
    """)
    @patch("os.path.join", side_effect=lambda *args: "/".join(args))
    @patch("utils.config_utils.Path.home", return_value=Path("/home/jiayin"))
    def test_load_config(self, mock_home, mock_join, mock_file):
        # Expected configuration based on mock YAML
        expected_config = {
            "base_dir": "/home/jiayin/assessment-rent-airbnb",
            "paths": {
                "data_dir": "data",
                "output_dirs": {
                    "bronze": "data/output/bronze",
                    "silver": "data/output/silver",
                    "gold": "data/output/gold"
                }
            },
            "files": {
                "zip": {
                    "rentals": "rentals.zip",
                    "airbnb": "airbnb.zip"
                },
                "geojson": {
                    "post_codes": "geo/post_codes.geojson",
                    "amsterdam_areas": "geo/amsterdam_areas.geojson"
                },
                "bronze_parquet": {
                    "rentals": "rentals.parquet",
                    "airbnb": "airbnb.parquet"
                },
                "silver_parquet": {
                    "rentals": "rentals_cleaned.parquet",
                    "airbnb": "airbnb_cleaned.parquet"
                },
                "gold_parquet": {
                    "output": "average_revenue_per_postcode.parquet"
                }
            }
        }
        
        # Call the function and check the result
        config = load_config()
        
        # Validate that open was called with the correct path
        mock_file.assert_called_once_with("/home/jiayin/assessment-rent-airbnb/resources/config.yml", "r")
        
        # Check if the returned configuration matches the expected data
        self.assertEqual(config, expected_config)

    @patch("utils.config_utils.load_config", return_value={
        "base_dir": "/home/jiayin/assessment-rent-airbnb",
        "paths": {
            "output_dirs": {
                "bronze": "data/output/bronze",
                "silver": "data/output/silver",
                "gold": "data/output/gold"
            }
        }
    })
    def test_get_output_dir(self, mock_load_config):
        # Test getting the output directory for 'bronze' layer
        output_dir = get_output_dir("bronze")
        expected_output_dir = "/home/jiayin/assessment-rent-airbnb/data/output/bronze"
        self.assertEqual(output_dir, expected_output_dir)

    @patch("utils.config_utils.load_config", return_value={
        "base_dir": "/home/jiayin/assessment-rent-airbnb",
        "paths": {
            "data_dir": "data",
            "output_dirs": {
                "bronze": "data/output/bronze"
            }
        },
        "files": {
            "bronze_parquet": {
                "rentals": "rentals.parquet"
            }
        }
    })
    def test_get_file_path(self, mock_load_config):
        # Test getting file path for a data file
        file_path = get_file_path("data", "bronze_parquet", "rentals")
        expected_file_path = "/home/jiayin/assessment-rent-airbnb/data/rentals.parquet"
        self.assertEqual(file_path, expected_file_path)
        
        # Test getting file path for an output file in the 'bronze' layer
        output_file_path = get_file_path("output", "bronze_parquet", "rentals", layer="bronze")
        expected_output_file_path = "/home/jiayin/assessment-rent-airbnb/data/output/bronze/rentals.parquet"
        self.assertEqual(output_file_path, expected_output_file_path)


if __name__ == '__main__':
    unittest.main()
