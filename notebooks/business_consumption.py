# Databricks notebook source
# MAGIC %pip install koheesio==0.10.6
# MAGIC %restart_python  

# COMMAND ----------

from koheesio.spark.writers.file_writer import CsvFileWriter
from koheesio.spark.writers import BatchOutputMode
from koheesio.spark.readers.spark_sql_reader import SparkSqlReader

from src.shared import CONFIG, BASE_VOLUMES_PATH, CATALOG, SCHEMA

# COMMAND ----------

spark.sql(f"""
    create or replace view {CATALOG}.{SCHEMA}.investment_opportunity
    as 
    select *
    , dense_rank(airbnb_income_amt) over (order by airbnb_income_amt desc) as airbnb_income_rank
    , dense_rank(kamernet_income_amt) over (order by kamernet_income_amt desc) as kamernet_income_rank
    from (
    select rc.postal_cd
        , rc.postal_suffix_cd
        , rc.internet_access_ind
        , rc.utilities_included_ind
        , rc.deposit_amt_eur
        , rc.additional_cost_amt_eur
        , rc.registration_cost_amt_eur
        , rc.rent_amt_eur
        , ac.price_per_night_eur
        , 12 * 15 * ac.price_per_night_eur as airbnb_income_amt
        , (12 * rc.rent_amt_eur) - rc.additional_cost_amt_eur - rc.registration_cost_amt_eur as kamernet_income_amt
    from {CATALOG}.{SCHEMA}.rentals_cleansed rc
        left join {CATALOG}.{SCHEMA}.airbnb_cleansed ac
        on rc.postal_cd = ac.postal_cd
        and rc.postal_suffix_cd = ac.postal_suffix_cd
    where 1=1
        and ac.airbnb_uuid is not null
        and utilities_included_ind = 'N'
    )
""")

# COMMAND ----------

spark.sql(f"""
    create volume if not exists {CATALOG}.{SCHEMA}.output;
""")

# COMMAND ----------

output_path = f"{BASE_VOLUMES_PATH}/output/"

# COMMAND ----------

reader = SparkSqlReader(
    sql=f"select * from {CATALOG}.{SCHEMA}.investment_opportunity"
)
writer = CsvFileWriter(
    df=reader.read().coalesce(1),
    path=output_path,
    output_mode=BatchOutputMode.OVERWRITE,
    header=True,
)
writer.write()
