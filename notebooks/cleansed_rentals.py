# Databricks notebook source
# MAGIC
# MAGIC %pip install koheesio==0.10.6
# MAGIC %restart_python
# MAGIC

# COMMAND ----------

from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.readers.delta import DeltaTableReader
from koheesio.spark.transformations.transform import Transform
from koheesio.spark.transformations.uuid5 import HashUUID5
from koheesio.spark.writers import StreamingOutputMode
from koheesio.spark.writers.delta import DeltaTableStreamWriter
from koheesio.spark.writers.stream import Trigger

from src.shared import (
    BASE_VOLUMES_PATH,
    CATALOG,
    CONFIG,
    SCHEMA,
    common_transformations,
)

# COMMAND ----------

dataset = "rentals"
location = f"{BASE_VOLUMES_PATH}/{CONFIG['lz'][dataset]}"
checkpoint_location = f"{BASE_VOLUMES_PATH}/checkpoint/{dataset}_cleansed"

# COMMAND ----------

spark.sql(
    f"""
    create table if not exists {CATALOG}.{SCHEMA}.{dataset}_cleansed
    (
        rental_uuid string
        , rental_id string
        , area_m2 integer
        , city_nm string
        , energy_label_desc string
        , furnishing_type_desc string
        , internet_access_ind string
        , room_active_ind string
        , living_type_desc string
        , capacity_desc string
        , property_type_desc string
        , utilities_included_ind string
        , availability_start_dt date
        , availability_end_dt date
        , deposit_amt_eur decimal(18,3)
        , additional_cost_amt_eur decimal(18,3)
        , registration_cost_amt_eur decimal(18,3)
        , rent_amt_eur decimal(18,3)
        , postal_cd string
        , postal_suffix_cd string
        , latitude_deg decimal(10,8)
        , longitude_deg decimal(10,8)
        , source_type_desc string
        , meta_record_checksum_txt string
        /* add metadata columns, e.g. timestamp */
    )
    using delta
    cluster by auto
    tblproperties (
        'delta.autoOptimize.autoCompact' = 'true',
        'delta.columnMapping.mode' = 'name',
        'delta.enableChangeDataFeed' = 'true'
    )
    """
)

# COMMAND ----------

reader = DeltaTableReader(
    table=DeltaTableStep(
        table=f"{dataset}_raw",
        database=SCHEMA,
        catalog=CATALOG,
    ).table_name,
    streaming=False,
    read_change_feed=False,
    ignore_deletes=False,
)
df = reader.read()
df.createOrReplaceTempView(dataset)

# COMMAND ----------

reader = DeltaTableReader(
    table=DeltaTableStep(
        table=f"{dataset}_raw",
        database=SCHEMA,
        catalog=CATALOG,
    ).table_name,
    streaming=True,
    read_change_feed=False,
    ignore_deletes=False,
)

df = reader.read()
df.createOrReplaceTempView(dataset)

df = spark.sql(
    """
    select from_json(_id, "array<string>")[0] as rental_id
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

# COMMAND ----------


def batch_function(df, batch_id):
    spark = df.sparkSession
    df = df.transform(
        Transform(
            func=common_transformations,
            hash_key_columns=["rental_id"],
            hash_output_column="rental_uuid",
        )
    ).drop_duplicates()

    df.createOrReplaceTempView("s")

    spark.sql(
        f"""
        merge into {CATALOG}.{SCHEMA}.{dataset}_cleansed t
        using s
        on t.rental_uuid = s.rental_uuid
        when matched and t.meta_record_checksum_txt != s.meta_record_checksum_txt then update set
            t.rental_id = s.rental_id
            , t.additional_cost_amt_eur = s.additional_cost_amt_eur
            , t.area_m2 = s.area_m2
            , t.city_nm = s.city_nm
            , t.deposit_amt_eur = s.deposit_amt_eur
            , t.energy_label_desc = s.energy_label_desc
            , t.availability_start_dt = s.availability_start_dt
            , t.availability_end_dt = s.availability_end_dt
            , t.furnishing_type_desc = s.furnishing_type_desc
            , t.internet_access_ind = s.internet_access_ind
            , t.room_active_ind = s.room_active_ind
            , t.living_type_desc = s.living_type_desc
            , t.latitude_deg = s.latitude_deg
            , t.longitude_deg = s.longitude_deg
            , t.capacity_desc = s.capacity_desc
            , t.postal_cd = s.postal_cd
            , t.postal_suffix_cd = s.postal_suffix_cd
            , t.property_type_desc = s.property_type_desc
            , t.registration_cost_amt_eur = s.registration_cost_amt_eur
            , t.rent_amt_eur = s.rent_amt_eur
            , t.utilities_included_ind = s.utilities_included_ind
            , t.source_type_desc = s.source_type_desc
            , t.meta_record_checksum_txt = s.meta_record_checksum_txt
        when not matched then
            insert (
                rental_uuid
                , rental_id
                , additional_cost_amt_eur
                , area_m2
                , city_nm
                , deposit_amt_eur
                , energy_label_desc
                , availability_start_dt
                , availability_end_dt
                , furnishing_type_desc
                , internet_access_ind
                , room_active_ind
                , living_type_desc
                , latitude_deg
                , longitude_deg
                , capacity_desc
                , property_type_desc
                , postal_cd
                , postal_suffix_cd
                , registration_cost_amt_eur
                , rent_amt_eur
                , utilities_included_ind
                , source_type_desc
                , meta_record_checksum_txt
            )
            values (
                s.rental_uuid
                , s.rental_id
                , s.additional_cost_amt_eur
                , s.area_m2
                , s.city_nm
                , s.deposit_amt_eur
                , s.energy_label_desc
                , s.availability_start_dt
                , s.availability_end_dt
                , s.furnishing_type_desc
                , s.internet_access_ind
                , s.room_active_ind
                , s.living_type_desc
                , s.latitude_deg
                , s.longitude_deg
                , s.capacity_desc
                , s.property_type_desc
                , s.postal_cd
                , s.postal_suffix_cd
                , s.registration_cost_amt_eur
                , s.rent_amt_eur
                , s.utilities_included_ind
                , s.source_type_desc
                , s.meta_record_checksum_txt
            )
        """
    )


DeltaTableStreamWriter(
    df=df,
    table=f"{CATALOG}.{SCHEMA}.{dataset}_cleansed",
    batch_function=batch_function,
    output_mode=StreamingOutputMode.APPEND,
    trigger=Trigger(available_now=True),
    checkpoint_location=checkpoint_location,
).write()
