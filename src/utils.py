# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

# COMMAND ----------

def remove_all_spaces_from_string_columns(df):

    # Get the names of all string columns
    string_columns = [col_name for col_name, col_type in df.dtypes if col_type == 'string']
    
    # Apply regexp_replace to each string column
    for column_name in string_columns:
        df = df.withColumn(column_name, regexp_replace(col(column_name), "^\s+|\s+$", ""))
    
    return df

# COMMAND ----------

def remove_chars_from_column(df, column_name, chars_to_remove):

    # Apply regexp_replace to remove specified characters from the column
    df = df.withColumn(column_name, regexp_replace(col(column_name), chars_to_remove, ""))
    
    return df

# COMMAND ----------

def convert_column_datatype(df, column_name, target_datatype):

    # Apply cast to convert column data type
    df = df.withColumn(column_name, col(column_name).cast(target_datatype))
    
    return df

# COMMAND ----------

def remove_brackets(df, column_names):

    # Define the characters to remove
    chars_to_remove = "[\",\\[\\]]"

    # Escape special characters in chars_to_remove and construct the regex pattern
    escaped_chars_to_remove = "[" + "".join(["\\" + c if c in "\\[]" else c for c in chars_to_remove]) + "]"
    
    # Apply regexp_replace to each column in the list
    for column_name in column_names:
        df = df.withColumn(column_name, regexp_replace(col(column_name).cast("string"), escaped_chars_to_remove, ""))
    
    return df

# COMMAND ----------

def convert_string_columns_to_timestamps(df, column_names, timestamp_format):

    # Iterate over each column name in the list and convert the column to a timestamp column
    for column_name in column_names:
        df = df.withColumn(column_name, to_timestamp(col(column_name), timestamp_format))
    
    return df


# COMMAND ----------

def norm_zip_code(df,column):
    
    return df.withColumn(column, df[column].substr(1, 4))

# COMMAND ----------


def split_string_column(df, column_name):

    # Extract the first 9 characters and put them into 'available_from' column
    df = df.withColumn("available_from", substring(col(column_name), 1, 9))
    
    # Extract the remaining characters and put them into 'available_until' column
    df = df.withColumn("available_until", substring(col(column_name), 13, 100))  # Assuming the maximum length of the column is 100 characters
    
    return df


# COMMAND ----------

def split_column(df, column_name):

    # Split the column into two parts based on the '-' character
    split_col = split(df[column_name], '-')
    
    # Create new columns 'rent' and 'Utilities incl' from the split parts
    df = df.withColumn('rent_num', split_col.getItem(0))
    df = df.withColumn('utilities_included', split_col.getItem(1))
    
    return df

# COMMAND ----------

def convert_strings_to_numeric(df, column_names):

    # Apply the conversion logic to each column
    converted_df = df
    for column_name in column_names:
        converted_df = converted_df.withColumn(column_name,
                                               when(col(column_name) == '-', 0)
                                               .otherwise(col(column_name).cast("float")))
    
    return converted_df

# COMMAND ----------

def rename_columns(df: DataFrame, old_names: list, new_names: list) -> DataFrame:

    if len(old_names) != len(new_names):
        raise ValueError("Length of old_names and new_names must be the same.")
    
    for old, new in zip(old_names, new_names):
        df = df.withColumnRenamed(old, new)
    
    return df
