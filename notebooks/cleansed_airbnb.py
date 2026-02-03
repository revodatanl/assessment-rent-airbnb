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

dataset = "airbnb"
location = f"{BASE_VOLUMES_PATH}/{CONFIG['lz'][dataset]}"
checkpoint_location = f"{BASE_VOLUMES_PATH}/checkpoint/{dataset}_cleansed"

# COMMAND ----------

# Create target table
spark.sql(
    f"""
    create table if not exists {CATALOG}.{SCHEMA}.{dataset}_cleansed
    (
        airbnb_uuid string
        , postal_cd string
        , postal_suffix_cd string
        , latitude_deg decimal(10,8)
        , longitude_deg decimal(10,8)
        , accomodation_desc string
        , capacity_desc string
        , bedroom_cnt integer
        , price_per_night_eur decimal(18,3)
        , reviewer_score_cnt decimal(18,3)
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
    streaming=True,
    read_change_feed=False,
    ignore_deletes=False,
)

df = reader.read()
df.createOrReplaceTempView(dataset)

df = spark.sql(
    """
    select left(trim(zipcode), 4) as postal_cd
        , nullif(regexp_extract(upper(zipcode), '([A-Z]{2})$'), '') as postal_suffix_cd
        , cast(latitude as decimal(10,8)) as latitude_deg
        , cast(longitude as decimal(10,8)) as longitude_deg
        , upper(trim(room_type)) as accomodation_desc
        , case
            when cast(accommodates as int) between 1 and 5 then cast(accommodates as int)
            when cast(accommodates as int) > 5 then '5+'
            else 'Unknown'
        end as capacity_desc
        , cast(bedrooms as int) as bedroom_cnt
        , cast(price as decimal(18,3)) as price_per_night_eur
        , cast(review_scores_value as decimal(18,3)) as reviewer_score_cnt
    from airbnb
    """
)

# COMMAND ----------


def batch_function(df, batch_id):
    spark = df.sparkSession
    df = df.transform(
        Transform(
            func=common_transformations,
            hash_key_columns=["latitude_deg", "longitude_deg"],
            hash_output_column="airbnb_uuid",
        )
    ).drop_duplicates()

    df.createOrReplaceTempView("s")

    spark.sql(
        f"""
        merge into {CATALOG}.{SCHEMA}.{dataset}_cleansed t
        using s
        on t.airbnb_uuid = s.airbnb_uuid
        when matched and t.meta_record_checksum_txt != s.meta_record_checksum_txt then update set
            t.postal_cd = s.postal_cd
            , t.postal_suffix_cd = s.postal_suffix_cd
            , t.latitude_deg = s.latitude_deg
            , t.longitude_deg = s.longitude_deg
            , t.accomodation_desc = s.accomodation_desc
            , t.capacity_desc = s.capacity_desc
            , t.bedroom_cnt = s.bedroom_cnt
            , t.price_per_night_eur = s.price_per_night_eur
            , t.reviewer_score_cnt = s.reviewer_score_cnt
            , t.meta_record_checksum_txt = s.meta_record_checksum_txt
        when not matched then
            insert (
                airbnb_uuid
                , postal_cd
                , postal_suffix_cd
                , latitude_deg
                , longitude_deg
                , accomodation_desc
                , capacity_desc
                , bedroom_cnt
                , price_per_night_eur
                , reviewer_score_cnt
                , meta_record_checksum_txt
            )
            values (
                s.airbnb_uuid
                , s.postal_cd
                , s.postal_suffix_cd
                , s.latitude_deg
                , s.longitude_deg
                , s.accomodation_desc
                , s.capacity_desc
                , s.bedroom_cnt
                , s.price_per_night_eur
                , s.reviewer_score_cnt
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
