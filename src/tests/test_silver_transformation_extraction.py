# Databricks notebook source
import unittest
import os

# COMMAND ----------

# MAGIC %run /Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/src/paths

# COMMAND ----------

def is_parquet_file_empty(file_path):

    # Read the Parquet file into a DataFrame
    df = spark.read.parquet(file_path)

    # Check if DataFrame is empty
    is_empty = df.isEmpty()

    return is_empty


def has_duplicate_records(file_path):

    # Read the Parquet file into a DataFrame
    df = spark.read.parquet(file_path)

    # Check for duplicate records
    num_records_before = df.count()

    # Drop duplicate records
    df_no_duplicates = df.dropDuplicates()

    # Count the number of records after removing duplicates
    num_records_after = df_no_duplicates.count()

    # Check if there are any duplicates
    has_duplicates = num_records_before != num_records_after

    return has_duplicates


class TestParquetFile(unittest.TestCase):

    def test_is_parquet_file_empty(self):
        # Test is_parquet_file_empty function with different file paths
        self.assertFalse(is_parquet_file_empty(airbnb_silver))
        self.assertFalse(is_parquet_file_empty(rent_silver))

    def test_has_duplicate_records(self):
        # Test has_duplicate_records function with different file paths
        self.assertFalse(has_duplicate_records(airbnb_silver))
        self.assertFalse(has_duplicate_records(rent_silver))


if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)

# COMMAND ----------


