# Databricks notebook source
!pip install pre-commit black flake8

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

!pre-commit install


# COMMAND ----------


