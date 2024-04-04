# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------


def unzip_file(zip_file_path, output_dir):
    """
    Unzips the specified file and stores the extracted files in the given output directory.
    """
    # Create the output directory if it does not exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Open the zip file
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        # Extract each file in the zip file
        for file_name in zip_ref.namelist():
            # Read content of the file
            file_content = zip_ref.read(file_name)
            
            # Determine the output file path
            output_file_path = os.path.join(output_dir, file_name)
            
            # Write the extracted file content to the output file
            with open(output_file_path, 'wb') as output_file:
                output_file.write(file_content)
                
            print(f"Extracted file: {file_name} to {output_file_path}")

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

    split_col = split(df[column_name], '-')
    
    df = df.withColumn('rent_num', split_col.getItem(0))
    df = df.withColumn('utilities_included', split_col.getItem(1))
    
    return df

# COMMAND ----------

def convert_strings_to_numeric(df, column_names):

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

# COMMAND ----------

def save_df_to_parquet(df, file_name, file_path):

    # Construct the full file path in DBFS
    full_file_path = f'dbfs:{file_path}/{file_name}'

    # Save DataFrame to Parquet format in DBFS
    df.write.parquet(full_file_path, mode='overwrite')


# COMMAND ----------

def calculate_cumulative_percentage(df, postal_code_col, rent_price_col):
    # Group by postal code, calculate average rent price and count
    result_df = df.groupBy(postal_code_col).agg(
        F.mean(rent_price_col).alias('average_price'),
        F.count(rent_price_col).alias('count')
    )
    
    # Order by count in descending order
    windowSpec = Window.orderBy(F.desc('count'))
    result_df = result_df.orderBy(F.desc('count'))
    
    # Add a new column with the cumulative sum of count
    result_df = result_df.withColumn('cumulative_sum', F.sum('count').over(windowSpec))
    
    # Calculate cumulative percentage based on cumulative sum of count
    result_df = result_df.withColumn('cumulative_percentage', F.col('cumulative_sum') * 100 / F.sum('count').over(Window.partitionBy()))
    
    return result_df

# COMMAND ----------


