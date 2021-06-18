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

display(spark.table('beer').distinct())

# COMMAND ----------

display(spark.table('beer'))

# COMMAND ----------

df_beer = spark.table('beer')
df_facts = spark.table('facts')

# COMMAND ----------

df = df_beer.join(df_facts, 'beer_bid')

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df_dedupe = df.dropDuplicates(['checkin_id','created_at'])

# COMMAND ----------

df_dedupe.count()

# COMMAND ----------

