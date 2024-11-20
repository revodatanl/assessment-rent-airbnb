import unittest
from shapely.geometry import Point
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from silver.airbnb_data_cleaning import enrich_airbnb_zipcode, clean_airbnb_data

from unittest.mock import patch
import geopandas as gpd

class TestAirbnbDataCleaning(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("AirbnbDataCleaningTest") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @patch('silver.airbnb_data_cleaning.gpd.read_file')
    def test_enrich_airbnb_zipcode(self, mock_read_file):
        # Mocking the return value of read_file to simulate post code data
        mock_post_codes = gpd.GeoDataFrame({
            'pc4_code': ['1000AB', '2000CD'],
            'geometry': [Point(0, 0), Point(1, 1)],
            'gem_name': ['Amsterdam', 'Rotterdam'],
            'prov_name': ['North Holland', 'South Holland']
        }, crs="EPSG:28992")
        mock_read_file.return_value = mock_post_codes

        # Create a sample DataFrame
        data = [
            (1, 52.373, 4.890, '1000AB',),
            (2, 52.374, 4.891, None,)
        ]
        columns = ['id', 'latitude', 'longitude', 'zipcode']
        airbnb_df = self.spark.createDataFrame(data, columns)

        # Call the function
        enriched_df = enrich_airbnb_zipcode(airbnb_df, self.spark)

        # Check results
        self.assertTrue('zipcodeEnhanced' in enriched_df.columns)
        self.assertEqual(enriched_df.filter(enriched_df.id == 1).select('zipcodeEnhanced').first()[0], '1000AB')
        self.assertIsNotNone(enriched_df.filter(enriched_df.id == 2).select('zipcodeEnhanced').first()[0])

    def test_clean_airbnb_data(self):
        # Create a sample DataFrame
        data = [
            ("Noord-Holland", "Amsterdam", "1012AB", 52.3728, 4.8936, "Entire home/apt", 2, 1, '100.0', 4.5),
            ("Noord-Holland", "Amsterdam", "1013AB", 52.3791, 4.9003, "Private room", 1, 1, 70.0, 4.0),
            ("Noord-Holland", "Amsterdam", "1015AB", 52.3842, 4.8863, "Shared room", 4, 2, '50,-', 3.5),
        ]
        columns = ['province', 'municipality', 'zipcodeEnhanced', 'latitude', 'longitude', 'room_type', 'accommodates', 'bedrooms', 'price', 'review_scores_value']
        df = self.spark.createDataFrame(data, columns)

        # Call the function
        cleaned_df = clean_airbnb_data(df)

        # Check results
        expected_columns = ['province', 'municipality', 'zipcode', 'latitude', 'longitude', 'room_type', 'accommodates', 'bedrooms', 'price', 'review_scores_value']
        self.assertEqual(set(cleaned_df.columns), set(expected_columns))

        # Verify the price column is converted to a numeric format
        self.assertEqual(cleaned_df.schema['price'].dataType, StringType())

        # Verify cleaned data values (e.g., price values are converted properly)
        prices = cleaned_df.select('price').rdd.flatMap(lambda x: x).collect()
        expected_prices = ['100.00', '70.00', '50.00']  # Expected cleaned prices
        self.assertEqual(prices, expected_prices)

if __name__ == '__main__':
    unittest.main()
