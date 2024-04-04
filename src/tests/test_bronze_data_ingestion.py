# Databricks notebook source
import pyspark.sql.functions as F
import os
import unittest

# COMMAND ----------

# MAGIC %run /Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/src/paths

# COMMAND ----------

class TestDataIngestion(unittest.TestCase):
    def test_json_ingestion(self):
        json_df = spark.read.json(rent_bronze)
        # Assert that DataFrame is not empty
        self.assertFalse(json_df.isEmpty(), "JSON DataFrame is empty")

    def test_csv_ingestion(self):
        csv_df = spark.read.csv(airbnb_bronze, header=True, inferSchema=True)
        # Assert that DataFrame is not empty
        self.assertFalse(csv_df.isEmpty(), "CSV DataFrame is empty")

if __name__ == '__main__':
    unittest.main(argv=[''], exit=False)


# COMMAND ----------


