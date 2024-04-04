# Databricks notebook source
import os
import zipfile
from io import BytesIO

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./paths

# COMMAND ----------

airbnb_file_path = airbnb_zip_filepath
rental_file_path = rental_zip_filepath
output_directory = '/dbfs/FileStore/raw_data'

unzip_file(airbnb_file_path, output_directory)
unzip_file(rental_file_path, output_directory)

# COMMAND ----------

# air = spark.read.csv(airbnb_bronze, header=True, inferSchema=True)
# rent = spark.read.json(rent_bronze)



# COMMAND ----------


