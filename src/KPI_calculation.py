# Databricks notebook source
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./paths

# COMMAND ----------

# Import the files from the dbfs into pyspark dataframes
air_file_path = airbnb_silver
rent_path = rent_silver

air = spark.read.parquet(air_file_path)
rent = spark.read.parquet(rent_path)

# COMMAND ----------

# Apply function calculate_cumulative_percentage to calculate the average revenue, count and cummulative count percentage of houses per zipcode
air_final = calculate_cumulative_percentage(air, 'zipcode', 'price')
rent_final = calculate_cumulative_percentage(rent, 'postal_code', 'rent_price')

# COMMAND ----------

# Save final files to dbfs as final data
save_df_to_parquet(air_final, 'final_airbnb_data.parquet', '/FileStore/final_data')
save_df_to_parquet(rent_final, 'final_rent_data.parquet', '/FileStore/final_data')

# COMMAND ----------

pandas_rent = rent_final.toPandas()
pandas_air = air_final.toPandas()

# Save Pandas DataFrame as Parquet file
pandas_rent.to_parquet(rent_final_file_path)
pandas_air.to_parquet(airbnb_final_file_path)

# COMMAND ----------


