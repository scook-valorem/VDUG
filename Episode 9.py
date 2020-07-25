# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clear checkpoints

# COMMAND ----------

dbutils.fs.rm(untappd_base_query_path+'/checkpoints', True)

# COMMAND ----------

