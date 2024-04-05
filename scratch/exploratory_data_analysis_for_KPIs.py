# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/src/utils

# COMMAND ----------

# MAGIC %run /Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/src/paths

# COMMAND ----------

air = spark.read.parquet(airbnb_silver)
rent = spark.read.parquet(rent_silver)

# COMMAND ----------

air_grouped = calculate_cumulative_percentage(air, 'zipcode', 'price')
rent_grouped = calculate_cumulative_percentage(rent, 'postal_code', 'rent_price')

# COMMAND ----------

# Creating subsets for the dataframes
sub_air=air_grouped.filter(air_grouped['cumulative_percentage']<=80).orderBy(col("average_price").desc())

sub_rent=rent_grouped.filter(rent_grouped['count']>=25).orderBy(col("average_price").desc())

# COMMAND ----------

# Plot the average rent price grouped by zipcode for airbnb prices
plot_rent_prices(sub_air, "average_price", "zipcode",'Rent prices by zipcode for Airbnb')

# COMMAND ----------

# Checking the undeling data for the zipcode with the highest average priced
display(sub_air.filter(sub_air['zipcode']>='1071').orderBy(col("average_price").desc()))

# COMMAND ----------

# Plot the frequency of house grouped by zipcode for airbnb prices to check the density of houses
plot_rent_prices(sub_air.orderBy(col("count").desc()), "count", "zipcode",'Rent prices by zipcode for Airbnb','lightsalmon')

# COMMAND ----------

# Plot the average rent price grouped by zipcode for rental prices
plot_rent_prices(sub_rent, "average_price", "postal_code",'Rent prices by zipcode for Rental houses','lightcoral')

# COMMAND ----------

# Checking the undeling data for the zipcode with the highest average price
display(sub_rent.filter(sub_rent['postal_code']>='1054KS').orderBy(col("average_price").desc()))

# COMMAND ----------

# Plot the frequency of house grouped by zipcode for rental prices to check the density of houses
plot_rent_prices(sub_rent.orderBy(col("count").desc()), "count", "postal_code",'Rent prices by zipcode for Rental','lightgreen')

# COMMAND ----------


