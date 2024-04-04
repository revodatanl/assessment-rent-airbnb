# Databricks notebook source
from pyspark.sql.functions import *

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
    """
    Convert multiple string columns in a PySpark DataFrame to timestamp columns using the specified timestamp format.
    
    Args:
        df (DataFrame): The PySpark DataFrame.
        column_names (list): A list of string column names to convert.
        timestamp_format (str): The format of the timestamp strings in the columns.
        
    Returns:
        DataFrame: The PySpark DataFrame with the specified string columns converted to timestamp columns.
    """
    # Iterate over each column name in the list and convert the column to a timestamp column
    for column_name in column_names:
        df = df.withColumn(column_name, to_timestamp(col(column_name), timestamp_format))
    
    return df


# COMMAND ----------

def norm_zip_code(df,column):
    
    return df.withColumn(column, df[column].substr(1, 4))

# COMMAND ----------

def split_date_range_column(df, date_range_column):
    """
    Split a date range column into 'available_from' and 'available_until' columns in a PySpark DataFrame.
    
    Args:
        df (DataFrame): The PySpark DataFrame.
        date_range_column (str): The name of the date range column.
        
    Returns:
        DataFrame: The PySpark DataFrame with the date range column split into two new columns.
    """
    # Find the position of the third '-' character in the date range column
    third_dash_pos = instr(col(date_range_column), "-")
    third_dash_pos = instr(col(date_range_column)[third_dash_pos + 1:], "-") + third_dash_pos
    
    # Split the date range column into two parts based on the third '-'
    df = df.withColumn("available_from", col(date_range_column).substr(1, third_dash_pos - 1))
    df = df.withColumn("available_until", col(date_range_column).substr(third_dash_pos + 1))
    
    # Convert date strings to actual date objects
    df = df.withColumn("available_from", to_date(col("available_from"), 'dd-MM-yy'))
    df = df.withColumn("available_until", when(col("available_until") == "Indefinite period", "9999-12-31").otherwise(to_date(col("available_until"), 'dd-MM-yy')))
    
    return df

# COMMAND ----------



