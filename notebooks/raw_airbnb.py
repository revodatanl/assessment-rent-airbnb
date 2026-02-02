# Databricks notebook source
# MAGIC %pip install koheesio==0.10.6
# MAGIC %restart_python

# COMMAND ----------

from koheesio.context import Context
from koheesio.spark.readers.databricks.autoloader import AutoLoader, AutoLoaderFormat
from koheesio.spark.delta import DeltaTableStep
from koheesio.spark.writers.delta import DeltaTableStreamWriter
from koheesio.spark.writers import StreamingOutputMode
from koheesio.spark.writers.stream import Trigger

from src.shared import CONFIG, BASE_VOLUMES_PATH, CATALOG, SCHEMA

# COMMAND ----------

dataset = "airbnb"
location = f"{BASE_VOLUMES_PATH}/{CONFIG['lz'][dataset]}"
schema_location = f"{BASE_VOLUMES_PATH}/autoloader/{dataset}"
checkpoint_location = f"{BASE_VOLUMES_PATH}/checkpoint/{dataset}_raw"

# COMMAND ----------

df = AutoLoader(
    format=AutoLoaderFormat.CSV,
    location=location,
    schema_location=schema_location,
    # schema_=schema,
    options={"header": True}
).read()

delta_table = DeltaTableStep(
    catalog=CATALOG,
    database=SCHEMA,
    table=f"{dataset}_raw",
    create_if_not_exists=True,
)

DeltaTableStreamWriter(
    df=df,
    table=delta_table,
    output_mode=StreamingOutputMode.APPEND,
    trigger=Trigger(available_now=True),
    checkpoint_location=checkpoint_location,
).write()
