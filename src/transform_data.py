# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

air = spark.read.csv('dbfs:/FileStore/raw_data/airbnb.csv', header=True, inferSchema=True)
rent = spark.read.json('dbfs:/FileStore/raw_data/rentals.json')

# COMMAND ----------

air=air.dropDuplicates()
rent=rent.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning airbnb data

# COMMAND ----------

air=air.dropna(subset='zipcode')
air=norm_zip_code(air,'zipcode')


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

from pyspark.sql.functions import when, length, substring, trim, current_date, expr

df = rent.withColumn("postedAgo_hours", 
                     when(trim(length(column_name)) < 4, 
                           when(substring(column_name, -1, 1) == 'w',substring('postedAgo', 1, 1)*24)
                          .when(substring(column_name, -1, 1) == 'd', substring('postedAgo', 1, 2)*24)
                          .when(substring(column_name, -1, 1) == 'h',substring('postedAgo', 1, 2) )
                          
                          .otherwise("big date")
                     )
                )



# COMMAND ----------



# # Extract the numerical part of the string
# numeric_value = regexp_extract(column_name, "\\d+", 0)

# df = rent.withColumn("postedAgo_hours", 
#                      when(length(column_name) < 4, 
#                           when(substring(column_name, -1, 1) == 'w', numeric_value.cast("int") * 24*7)
#                           .when(substring(column_name, -1, 1) == 'd', numeric_value.cast("int") * 24)
#                           .when(substring(column_name, -1, 1) == 'h', numeric_value.cast("int"))
#                           .otherwise("big date")
#                      )
#                 )


# COMMAND ----------




# df = df.withColumn("postedAgo_dates",
#                              when(length(column_name) > 6,
#                                   to_date(column_name, "yy MMM dd"))
#                              .otherwise(to_date(column_name, "dd MMM")))



# from pyspark.sql.functions import concat, year, month, current_date

# current_year = year(current_date())
# current_month = month(current_date())

# df = df.withColumn("postedAgo_dates",
#                    when(length(column_name) > 6,
#                         to_date(column_name, "yy MMM dd"))
#                    .otherwise(
#                        when(month(to_date(concat(column_name, current_year), "dd MMMyyyy")) > current_month,
#                             to_date(concat(column_name, current_year - 1), "dd MMMyyyy"))
#                        .otherwise(to_date(concat(column_name, current_year), "dd MMMyyyy"))
#                    ))




# COMMAND ----------

display(df)

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

# Assuming 'df' is your DataFrame
old_column_names = ["_id", "additionalCostsRaw",'areaSqm','descriptionTranslated','energyLabel','firstSeenAt','isRoomActive','lastSeenAt','propertyType','postedAgo','postalCode','registrationCost','smokingInside','pageDescription','rent_num','matchCapacity']
new_column_names = ["id", "additional_raw_costs",'area_sqm','translated_description','energy_label','first_seen_at','is_room_active','last_seen_at','property_type','posted_ago','postal_code','registration_cost','smoking_inside','page_description','rent_price','match_capacity']

rent = rename_columns(rent, old_column_names, new_column_names)


# COMMAND ----------

rent = convert_strings_to_numeric(rent, ["additional_raw_costs", "deposit",'area_sqm','registration_cost'])

# COMMAND ----------


air = spark.read.csv('dbfs:/FileStore/cleaned_data/cleaned_airbnb.csv', header=True, inferSchema=True)
rent = spark.read.json('dbfs:/FileStore/cleaned_data/cleaned_rentals.json')
