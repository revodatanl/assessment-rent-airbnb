# Databricks notebook source
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import os

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./paths

# COMMAND ----------

# Import the files from the dbfs into pyspark dataframes

air = spark.read.parquet(airbnb_silver)
rent = spark.read.parquet(rent_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Apply function calculate_cumulative_percentage to calculate the average revenue, count and cummulative count percentage of houses per zipcode

# COMMAND ----------

air_final = calculate_cumulative_percentage(air, 'zipcode', 'price')
rent_final = calculate_cumulative_percentage(rent, 'postal_code', 'rent_price')

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Produce visualization using oa subset of the records

# COMMAND ----------

# Create two subsets containg only records representing the 80% of the size
sub_air=air_final.filter(air_final['cumulative_percentage']<=80).orderBy(col("average_price").desc())

# Create two subsets containg only zipcodes that include more than 25 houses
sub_rent=rent_final.filter(rent_final['count']>=25).orderBy(col("average_price").desc())

# COMMAND ----------



# COMMAND ----------

# Assuming df is your PySpark DataFrame containing rent prices and categories of zip codes
plot_rent_prices(sub_air, "average_price", "zipcode",'Rent prices by zipcode for Airbnb')


# COMMAND ----------

plot_rent_prices(sub_rent, "average_price", "postal_code",'Rent prices by zipcode for Rental houses','lightgreen')

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
