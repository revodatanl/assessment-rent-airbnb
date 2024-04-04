# Databricks notebook source
import os

# COMMAND ----------

# MAGIC %run /Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/src/paths

# COMMAND ----------

import os
import unittest

def check_parquet_files(destination):
    # Check if the directory exists
    if not os.path.exists(destination):
        raise FileNotFoundError(f"Destination directory not found at {destination}")

    # List all files in the directory
    files = os.listdir(destination)

    # Filter only Parquet files
    parquet_files = [file for file in files if file.endswith('.parquet')]

    # Check if there are two Parquet files
    return len(parquet_files) == 2

class TestCheckParquetFiles(unittest.TestCase):
    def test_check_parquet_files(self):

        destination_path_with_two_files = output_filepath
        self.assertTrue(check_parquet_files(destination_path_with_two_files))

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)


# COMMAND ----------


