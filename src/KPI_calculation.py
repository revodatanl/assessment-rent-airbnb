# Databricks notebook source
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# Import the files from the dbfs into pyspark dataframes
air_file_path = 'dbfs:/FileStore/cleaned_data/cleaned_airbnb_data.parquet'
rent_path = 'dbfs:/FileStore/cleaned_data/cleaned_rent_data.parquet'

air = spark.read.parquet(air_file_path)
rent = spark.read.parquet(rent_path)

# COMMAND ----------

# Apply function calculate_cumulative_percentage to calculate the average revenue, count and cummulative count percentage of houses per zipcode
air_final = calculate_cumulative_percentage(air, 'zipcode', 'price')
rent_final = calculate_cumulative_percentage(rent, 'postal_code', 'rent_price')

# COMMAND ----------

save_df_to_parquet(air_final, 'final_airbnb_data.parquet', '/FileStore/final_data')
save_df_to_parquet(rent_final, 'final_rent_data.parquet', '/FileStore/final_data')

# COMMAND ----------



# # Convert Spark DataFrame to Pandas DataFrame
# pandas_df = result_air.toPandas()

# # Plotting the histogram
# plt.figure(figsize=(19, 10))  
# plt.bar(pandas_df['zipcode'], pandas_df['average_price'], color='skyblue')
# plt.title('Average Price by Zipcode')
# plt.xlabel('Zipcode')
# plt.ylabel('Average Price')
# plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
# plt.grid(True)
# plt.show()



# COMMAND ----------

# #esult_air=air.groupBy('zipcode').agg(F.mean('price').alias('average_price')).orderBy('average_price', ascending=False)
# result_air=air.groupBy('zipcode').agg(F.mean('price').alias('average_price'),
#     F.count('price').alias('count')).orderBy(['count','average_price'], ascending=False)
#rent=norm_zip_code(rent,'postal_code')
# result_rent=rent.groupBy('postal_code').agg(F.mean('rent_price').alias('average_price'),
#     F.count('rent_price').alias('count')).orderBy(['count','average_price'], ascending=False)
#result_air.write.format("delta").save("/delta-tables/result_air")
# Read data from Delta Lake table into a DataFrame
#test = spark.read.format("delta").load("/delta-tables/result_air")

# COMMAND ----------

def save_df_as_parquet_local(df, file_path):
    """
    Save DataFrame as Parquet file in local file system.

    Parameters:
        df (DataFrame): The DataFrame to be saved.
        file_path (str): The file path where the Parquet file will be saved.
    """
    df.write.mode('overwrite').parquet("file://" + file_path)

# Example usage


# COMMAND ----------

rent_final.write.parquet('/Workspace/Repos/kapposev@outlook.com/assessment-rent-airbnb/data/output/rent_final.parquet')

# COMMAND ----------


