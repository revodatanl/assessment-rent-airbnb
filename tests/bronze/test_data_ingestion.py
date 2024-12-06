import unittest
from unittest.mock import patch, MagicMock
import os
from bronze.data_ingestion import extract_file_from_zip, ingest_data_file

class TestDataIngestion(unittest.TestCase):

    @patch('zipfile.ZipFile')
    def test_extract_file_from_zip(self, mock_zipfile):
        # Setup the mock for the zip file
        mock_zipfile.return_value.__enter__.return_value.infolist.return_value = [MagicMock(filename='test_file.json')]
        
        # Specify the zip path and extraction path
        zip_path = 'test.zip'
        extraction_path = 'test_extracted'
        
        # Call the function
        result = extract_file_from_zip(zip_path, extraction_path)
        
        # Verify that the zip file was opened and the contents extracted
        mock_zipfile.assert_called_once_with(zip_path, 'r')
        mock_zipfile.return_value.__enter__.return_value.extractall.assert_called_once_with(extraction_path)
        
        # Check the returned file path is as expected
        expected_file_path = os.path.join(extraction_path, 'test_file.json')
        self.assertEqual(result, expected_file_path)

    @patch('bronze.data_ingestion.create_spark_session')
    def test_ingest_data_file_json(self, mock_create_spark_session):
        # Create a mock Spark session
        mock_spark = MagicMock()
        mock_create_spark_session.return_value = mock_spark
        
        # Mock the DataFrame returned by Spark
        mock_df = MagicMock()
        mock_spark.read.json.return_value = mock_df
        
        # Call the ingest_data_file function for a JSON file
        file_path = 'test_data.json'
        df = ingest_data_file(file_path, file_type='json', spark=mock_spark)  # Pass mock_spark as the spark argument
        
        # Verify Spark read operation
        mock_spark.read.json.assert_called_once_with(file_path)
        self.assertEqual(df, mock_df)

    @patch('bronze.data_ingestion.create_spark_session')
    def test_ingest_data_file_csv(self, mock_create_spark_session):
        # Create a mock Spark session
        mock_spark = MagicMock()
        mock_create_spark_session.return_value = mock_spark
        
        # Mock the DataFrame returned by Spark
        mock_df = MagicMock()
        mock_spark.read.csv.return_value = mock_df
        
        # Call the ingest_data_file function for a CSV file
        file_path = 'test_data.csv'
        df = ingest_data_file(file_path, file_type='csv', spark=mock_spark)  # Pass mock_spark as the spark argument
        
        # Verify Spark read operation
        mock_spark.read.csv.assert_called_once_with(file_path, header=True, inferSchema=True)
        self.assertEqual(df, mock_df)


if __name__ == '__main__':
    unittest.main()
