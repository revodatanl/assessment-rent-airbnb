# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

air = spark.read.csv('dbfs:/FileStore/raw_data/airbnb.csv', header=True, inferSchema=True)
rent = spark.read.json('dbfs:/FileStore/raw_data/rentals.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning airbnb data

# COMMAND ----------

air=air.dropna(subset='zipcode')
#air=air.withColumn("zipcode", air['zipcode'].substr(1, 4))
air=norm_zip_code(air,'zipcode')


# COMMAND ----------

display(air)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning rent data

# COMMAND ----------

display(rent)

# COMMAND ----------

#rent = rent.withColumn('_id', regexp_replace(col('_id').cast('string'), "[\",\\[\\]]", ""))

column_names = ["_id", "lastSeenAt",'firstSeenAt']
rent = remove_brackets(rent, column_names)

# COMMAND ----------

rent=remove_chars_from_column(rent, "additionalCostsRaw",'€')
rent=remove_chars_from_column(rent, "areaSqm",'m2')
rent=remove_chars_from_column(rent, "deposit",'€') 
rent=remove_chars_from_column(rent, "registrationCost",'€')

# COMMAND ----------

rent = remove_all_spaces_from_string_columns(rent)

# COMMAND ----------

column_list=['lastSeenAt','firstSeenAt']
rent = convert_string_columns_to_timestamps(df, column_list, 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ')


# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import substring, col
def split_string_column(df, column_name):
    """
    Split a string column into two parts, 'available_from' and 'available_until'.
    
    Args:
        df (DataFrame): The PySpark DataFrame.
        column_name (str): The name of the column to split.
        
    Returns:
        DataFrame: The PySpark DataFrame with the column split into two new columns.
    """
    # Extract the first 9 characters and put them into 'available_from' column
    df = df.withColumn("available_from", substring(col(column_name), 1, 9))
    
    # Extract the remaining characters and put them into 'available_until' column
    df = df.withColumn("available_until", substring(col(column_name), 13, 100))  # Assuming the maximum length of the column is 100 characters
    
    return df

# COMMAND ----------

rent = split_string_column(rent, "availability")


# COMMAND ----------

display(rent)

# COMMAND ----------

rent=remove_chars_from_column(rent, "available_from","'")
rent=remove_chars_from_column(rent, "available_until","'")

# COMMAND ----------

rent = rent.withColumn("available_until", when(col("available_until") == "Indefinite period", "31-12-99").otherwise(col("available_until")))


# COMMAND ----------

column_list=['available_until','available_from']
rent = convert_string_columns_to_timestamps(rent, column_list, "dd-MM-yy")

# COMMAND ----------


