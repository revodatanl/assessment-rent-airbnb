# Databricks notebook source
from pyspark.sql import functions as F


# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

air_file_path = 'dbfs:/FileStore/cleaned_data/cleaned_airbnb_data.parquet'
rent_path = 'dbfs:/FileStore/cleaned_data/cleaned_rent_data.parquet'

air = spark.read.parquet(air_file_path)
rent = spark.read.parquet(rent_path)

# COMMAND ----------

display(air)

# COMMAND ----------

result_df=air.groupBy('zipcode').agg(F.mean('price').alias('average_price')).orderBy('average_price', ascending=False)

# COMMAND ----------

import matplotlib.pyplot as plt

# Convert Spark DataFrame to Pandas DataFrame
pandas_df = result_df.toPandas()

# Plotting the histogram
plt.figure(figsize=(19, 10))  
plt.bar(pandas_df['zipcode'], pandas_df['average_price'], color='skyblue')
plt.title('Average Price by Zipcode')
plt.xlabel('Zipcode')
plt.ylabel('Average Price')
plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
plt.grid(True)
plt.show()



# COMMAND ----------

display(rent)

# COMMAND ----------

# MAGIC %pip install pre-commit
# MAGIC

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

!pre-commit --version


# COMMAND ----------

import pyspark
print(pyspark.__version__)


# COMMAND ----------


