# Databricks notebook source
from pyspark.sql.functions import *

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

display(air.filter(air['review_scores_value'].isNull()))


# COMMAND ----------

# Checking for invalid zipcodes
invalid_zipcodes_df = air.filter(~col('zipcode').rlike('^\d{4}$'))


display(invalid_zipcodes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### EDA for rent data

# COMMAND ----------

display(rent)

# COMMAND ----------

#rent = rent.withColumn('modified_id', regexp_replace(col('_id').cast('string'), "[\",\\[\\]]", ""))
from pyspark.sql.functions import col, regexp_replace

def remove_chars_from_column(df, column_name, chars_to_remove):

    # Construct the regex pattern to remove specified characters
    regex_pattern = "[" + "\\".join(chars_to_remove) + "]"
    
    # Apply regexp_replace to remove specified characters from the column
    df = df.withColumn(column_name, regexp_replace(col(column_name), regex_pattern, ""))
    
    return df

# Example usage:
# Assuming 'rent' is your DataFrame
# Define the list of characters to remove
chars_to_remove = [" m2"]

# Call the function to remove specified characters from the specified column
df = remove_chars_from_column(rent, "areaSqm", chars_to_remove)

# Show the DataFrame with the modified column


display(df)


# COMMAND ----------

df = remove_trailing_leading_spaces(remove_chars_from_column(rent, "deposit", "€ "))

# COMMAND ----------

display(df)

# COMMAND ----------



from pyspark.sql.functions import col, trim

def remove_trailing_leading_spaces(df):

    # Get the names of all string columns
    string_columns = [col_name for col_name, col_type in df.dtypes if col_type == 'string']
    
    # Apply trim to each string column
    for column_name in string_columns:
        df = df.withColumn(column_name, trim(col(column_name)))
    
    return df

df = remove_trailing_leading_spaces(rent)

# Show the DataFrame with the modified columns
display(df)



# COMMAND ----------

# Checking for invalid zipcodes
invalid_zipcodes_df_rent = rent.filter(~col('postal_code').rlike('^\d{4}[A-Za-z]{2}$'))

display(invalid_zipcodes_df_rent)
