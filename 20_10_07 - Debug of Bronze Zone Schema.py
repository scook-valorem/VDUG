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

df_delta = spark.read.format('delta').load(untappd_raw_delta_path)
schema = df_delta.schema

# COMMAND ----------

dbutils.fs.put(untappd_raw_schema_path, schema.json(), True)

# COMMAND ----------

head = dbutils.fs.head(untappd_raw_schema_path, 10000)
schema = StructType.fromJson(json.loads(head))

# COMMAND ----------

df = spark.read.schema(schema).json(untappd_raw_path)

# COMMAND ----------

