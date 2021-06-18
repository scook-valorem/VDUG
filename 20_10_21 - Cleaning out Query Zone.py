# Databricks notebook source
# MAGIC %md
# MAGIC ### add resiliency for API calls and raw data 
# MAGIC #### add enforced schema

# COMMAND ----------

# MAGIC %md 
# MAGIC ### imports

# COMMAND ----------

import requests
import json
import datetime
from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ### import configuration

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

untappd_raw_path = base_path+'raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
untappd_raw_delta_path = base_path+'raw/untappd/delta'
untappd_query_path =base_path+'query/untappd'
untappd_raw_schema_path = base_path + 'raw/untappd/schema'

# COMMAND ----------

dbutils.fs.ls(untappd_base_query_path)

# COMMAND ----------

dbutils.fs.rm(untappd_base_query_path,True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM beer

# COMMAND ----------

def dedupe_delta_table(name):
  df = spark.read.format('delta').option('ignoreChanges', True).load('{}{}'.format(untappd_base_query_path,name)).distinct()
  df.write.format('delta').option('path',  '{}{}'.format(untappd_base_query_path,name)).option('checkpointLocation', untappd_base_query_path+'{}/checkpoints'.format(name)).outputMode("overwrite")

# COMMAND ----------

name = 'beer'
df = spark.read.format('delta').option('ignoreChanges', True).load('{}{}'.format(untappd_base_query_path,name)).distinct()

# COMMAND ----------

df.count()

# COMMAND ----------

df_duped = spark.read.format('delta').option('ignoreChanges', True).load('{}{}'.format(untappd_base_query_path,name))

# COMMAND ----------

df_duped.count()

# COMMAND ----------

'{}{}'.format(untappd_base_query_path,name)

# COMMAND ----------

df.write.format('delta').option('path',  '{}{}'.format(untappd_base_query_path,name)).option('checkpointLocation', untappd_base_query_path+'{}/checkpoints'.format(name)).mode("overwrite")

# COMMAND ----------

df.count()

# COMMAND ----------

dbutils.fs.ls(untappd_base_query_path+'beer')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM beer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM venues

# COMMAND ----------

