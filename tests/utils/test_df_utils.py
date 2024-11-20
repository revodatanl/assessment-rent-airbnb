import unittest
import os
from unittest.mock import patch, MagicMock
from utils.df_utils import create_spark_session, save_to_parquet

class TestUtils(unittest.TestCase):

    @patch('utils.df_utils.SparkSession')
    def test_create_spark_session(self, mock_spark_session):
        # Setup the mock Spark session
        mock_instance = MagicMock()
        mock_spark_session.builder.appName.return_value.getOrCreate.return_value = mock_instance
        
        # Call the function to test
        spark = create_spark_session()
        
        # Check if the SparkSession was created with the correct app name
        mock_spark_session.builder.appName.assert_called_once_with("Amsterdam Properties")
        mock_spark_session.builder.appName.return_value.getOrCreate.assert_called_once()
        
        # Assert that the returned spark is the mock instance
        self.assertEqual(spark, mock_instance)

    @patch("utils.df_utils.os.makedirs")
    @patch("utils.df_utils.SparkSession")
    def test_save_to_parquet(self, mock_spark_session, mock_makedirs):
        # Create a mock DataFrame
        mock_df = MagicMock()
        
        # Patch the `write` attribute on the mock DataFrame to return a mock for `mode`
        mock_write = mock_df.write
        mock_mode = mock_write.mode.return_value
        mock_mode.parquet = MagicMock()

        # Define a fake path
        path = "/fake/path/to/parquet"

        # Call the save_to_parquet function
        save_to_parquet(mock_df, path)

        # Check if os.makedirs was called with the correct directory
        mock_makedirs.assert_called_once_with(os.path.dirname(path), exist_ok=True)

        # Check if the DataFrame write methods were called correctly
        mock_df.write.mode.assert_called_once_with('overwrite')
        mock_mode.parquet.assert_called_once_with(path)

if __name__ == '__main__':
    unittest.main()