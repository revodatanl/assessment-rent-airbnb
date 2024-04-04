# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./paths

# COMMAND ----------

air = spark.read.csv(airbnb_bronze, header=True, inferSchema=True)
rent = spark.read.json(rent_bronze)

# COMMAND ----------

air=air.dropDuplicates()
rent=rent.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning airbnb data

# COMMAND ----------

air=air.dropna(subset='zipcode')
air=norm_zip_code(air,'zipcode')
air=air.filter(col('zipcode').rlike('^\d{4}$'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning rent data

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
rent = convert_string_columns_to_timestamps(rent, column_list, 'yyyy-MM-dd\'T\'HH:mm:ss.SSSZ')
rent=remove_chars_from_column(rent, "postedAgo","'")
rent = split_string_column(rent, "availability")

# COMMAND ----------

rent=remove_chars_from_column(rent, "available_from","'")
rent=remove_chars_from_column(rent, "available_until","'")
rent = rent.withColumn("available_until", when(col("available_until") == "Indefinite period", "31-12-99").otherwise(col("available_until")))


# COMMAND ----------

column_list=['available_until','available_from']
rent = convert_string_columns_to_timestamps(rent, column_list, "dd-MM-yy")

# COMMAND ----------

rent = split_column(rent, 'rent')
rent=remove_chars_from_column(rent, "rent_num",",")
rent=remove_chars_from_column(rent, "rent_num","€")

# COMMAND ----------

rent = rent.withColumn("utilities_included", when(col("utilities_included").like("%Utilities incl%"), 1).otherwise(0))
rent = remove_all_spaces_from_string_columns(rent)

# COMMAND ----------

columns_to_drop = ['availability', 'crawlStatus', 'crawledAt', 'detailsCrawledAt', 'source', 'rent']
rent = rent.drop(*columns_to_drop)


# COMMAND ----------


old_column_names = ["_id", "additionalCostsRaw",'areaSqm','descriptionTranslated','energyLabel','firstSeenAt','isRoomActive','lastSeenAt','propertyType','postedAgo','postalCode','registrationCost','smokingInside','pageDescription','rent_num','matchCapacity']
new_column_names = ["id", "additional_raw_costs",'area_sqm','translated_description','energy_label','first_seen_at','is_room_active','last_seen_at','property_type','posted_ago','postal_code','registration_cost','smoking_inside','page_description','rent_price','match_capacity']

rent = rename_columns(rent, old_column_names, new_column_names)


# COMMAND ----------

rent = convert_strings_to_numeric(rent, ["additional_raw_costs", "deposit",'area_sqm','registration_cost'])

# COMMAND ----------

save_df_to_parquet(air, 'cleaned_airbnb_data.parquet', '/FileStore/cleaned_data')
save_df_to_parquet(rent, 'cleaned_rent_data.parquet', '/FileStore/cleaned_data')
