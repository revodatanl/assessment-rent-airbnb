# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/src/utils

# COMMAND ----------

# MAGIC %run /Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/src/paths

# COMMAND ----------

air = spark.read.csv('dbfs:/FileStore/raw_data/airbnb.csv', header=True, inferSchema=True)
rent = spark.read.json('dbfs:/FileStore/raw_data/rentals.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA for airbnb data

# COMMAND ----------

display(air)

# COMMAND ----------

# Checking for datatypes
display(air.dtypes)

# COMMAND ----------

# checking for null zipcodes
display(air.filter(col("zipcode").isNull()))


# COMMAND ----------

# checking for null prices
display(air.filter(col("price").isNull()))

# COMMAND ----------

display(air.select("room_type").distinct())

# COMMAND ----------

# There are no negative values in a column that would not make sense such as room_type or bedrooms 
display(air.describe())


# COMMAND ----------

display(air.dropna(subset='zipcode').filter((air['accommodates']>1)&(air['bedrooms']<=1)))

# COMMAND ----------

# Records with complete and incomplete zipcodes, should be converted to the simplest form since there is a significant amount of data missing the complete form 
display(air.filter(air["zipcode"].substr(1, 4) == '1012'))


# COMMAND ----------

display(air.dropna(subset='zipcode').withColumn("first_four", air['zipcode'].substr(1, 4)))

# COMMAND ----------

# No records with null value in column price
display(air.filter(air['price'].isNull()))

# COMMAND ----------

display(air.filter(air['review_scores_value'].isNotNull()))


# COMMAND ----------

# Cherck for null review scores and whta is its effect
display(air.filter(air['review_scores_value'].isNull()))


# COMMAND ----------

# Checking for invalid zipcodes
invalid_zipcodes_df = air.filter(~col('zipcode').rlike('^\d{4}$'))


display(invalid_zipcodes_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA for rent data

# COMMAND ----------

display(rent)

# COMMAND ----------

# checking for null zipcodes
display(rent.filter(col("rent").isNull()))

# COMMAND ----------


df = remove_chars_from_column(rent, "areaSqm", 'm2')

# Show the DataFrame with the modified column
display(df)


# COMMAND ----------

# Checking for invalid zipcodes
invalid_zipcodes_df_rent = rent.filter(~col('postalCode').rlike('^\d{4}[A-Za-z]{2}$'))

display(invalid_zipcodes_df_rent)

# COMMAND ----------


