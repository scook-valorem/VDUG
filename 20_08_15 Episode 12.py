# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

dbutils.fs.rm(untappd_base_query_path+'/checkpoints', True)

# COMMAND ----------

