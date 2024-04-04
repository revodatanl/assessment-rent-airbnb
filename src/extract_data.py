# Databricks notebook source
import os
import zipfile
from io import BytesIO

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

airbnb_file_path = '/Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/data/airbnb.zip'
#airbnb_file_path = 'data/airbnb.zip'
rental_file_path = '/Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/data/rentals.zip'
output_directory = '/dbfs/FileStore/raw_data'

unzip_file(airbnb_file_path, output_directory)
unzip_file(rental_file_path, output_directory)

# COMMAND ----------

air = spark.read.csv('dbfs:/FileStore/raw_data/airbnb.csv', header=True, inferSchema=True)
rent = spark.read.json('dbfs:/FileStore/raw_data/rentals.json')



# COMMAND ----------

# file_to_delete = "dbfs:/dbfs"

# dbutils.fs.rm(file_to_delete)


# COMMAND ----------


