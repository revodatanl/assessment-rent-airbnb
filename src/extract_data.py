# Databricks notebook source
import os
import zipfile
from io import BytesIO

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

airbnb_file_path = '/Workspace/Repos/assessment/assessment-rent-airbnb/data/airbnb.zip'
rental_file_path = '/Workspace/Repos/assessment/assessment-rent-airbnb/data/rentals.zip'
output_directory = '/dbfs/FileStore/raw_data'

unzip_file(airbnb_file_path, output_directory)
unzip_file(rental_file_path, output_directory)

# COMMAND ----------

air = spark.read.csv('dbfs:/FileStore/raw_data/airbnb.csv', header=True, inferSchema=True)
rent = spark.read.json('dbfs:/FileStore/raw_data/rentals.json')



# COMMAND ----------

# file_to_delete = "dbfs:/FileStore/data"

# dbutils.fs.rm(file_to_delete)


# COMMAND ----------


