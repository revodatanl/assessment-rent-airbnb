# Databricks notebook source
PATH = "/Volumes/development/personal_mmityu"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rentals

# COMMAND ----------

rentals_df = spark.read.json(f"{PATH}/lz_rentals/")
display(rentals_df)
rentals_df.createOrReplaceTempView("rentals")

# COMMAND ----------

# Basic
display(df.describe())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Seems good enough as generic ID
# MAGIC select distinct array_size(_id)
# MAGIC from rentals

# COMMAND ----------


# COMMAND ----------

display(
    spark.sql(
        """
    select _id[0] as rental_id
        , ifnull(nullif(regexp_replace(additionalCostsRaw, '[^0-9.]', ''),''),0) as additional_cost_amt_eur
        , cast(replace(areaSqm, ' m2', '') as integer) as area_m2
        , upper(trim(city)) as city_nm
        , ifnull(nullif(regexp_replace(deposit, '[^0-9.]', ''),''),0) as deposit_amt_eur
        , upper(energyLabel) as energy_label_desc
        , to_date(replace(split(availability, ' - ')[0],char(39),'20'), 'dd-MM-yyyy') as availability_start_dt
        , case
            when split(availability, ' - ')[1] = 'Indefinite period' then date('9999-12-31')
            else to_date(replace(split(availability, ' - ')[1],char(39),'20'), 'dd-MM-yyyy')
        end as availability_end_dt
        , upper(nullif(trim(furnish),'')) as furnishing_type_desc
        , case when trim(internet) = 'Yes' then 'Y' else 'N' end as internet_access_ind
        , case when isRoomActive = 'true' then 'Y' else 'N' end as room_active_ind
        , ifnull(living, 'Unknown') as living_type_desc
        , cast(latitude as decimal(10,8)) as latitude_deg
        , cast(longitude as decimal(10,8)) as longitude_deg
        , case
            when matchCapacity = '1 person' then '1'
            when matchCapacity = '2 persons' then '2'
            when matchCapacity = '3 persons' then '3'
            when matchCapacity = '4 persons' then '4'
            when matchCapacity = '5 persons' then '5'
            when matchCapacity = '>5 persons' then '5+'
            else 'Unknown'
        end as capacity_desc
        , left(trim(postalCode), 4) as postal_cd
        , nullif(regexp_extract(upper(postalCode), '([A-Z]{2})$'), '') as postal_suffix_cd
        , trim(propertyType) as property_type_desc
        , ifnull(nullif(regexp_replace(registrationCost, '[^0-9.]', ''),''),0) as registration_cost_amt_eur
        , nullif(regexp_replace(split(rent,',- ')[0], '[^0-9.]', ''),'') as rent_amt_eur
        , if(trim(try_element_at(split(rent,',- '),2)) = 'Utilities incl.','Y','N') as utilities_included_ind   
        , 'kamernet' as source_type_desc
    from rentals
    """
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct to_date(replace(split(availability, ' - ')[0],'\'','20'), 'dd-MM-yyyy') as availability_start_dtacity
# MAGIC from rentals

# COMMAND ----------

# MAGIC %md
# MAGIC ## Airbnb

# COMMAND ----------


airbnb_df = spark.read.options(header=True).csv(f"{PATH}/lz_airbnb/")
display(airbnb_df)
airbnb_df.createOrReplaceTempView("airbnb")

# COMMAND ----------

# Basic
display(airbnb_df.describe())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Poke around for distinct values or quick check of the cleasning logic
# MAGIC select distinct zipcode
# MAGIC , nullif(regexp_extract(upper(zipcode), '([A-Z]{2})$'), '') as postal_cd
# MAGIC from airbnb
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for dupes
# MAGIC select *
# MAGIC from airbnb
# MAGIC qualify count(1) over (partition by latitude, longitude) > 1
