# Databricks notebook source
# MAGIC %md
# MAGIC # Bring in all existing data from the Raw/Bronze Zone

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

import requests
import json
import datetime
from pyspark.sql.types import StructType
untappd_raw_path = base_path+'raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
untappd_raw_delta_path = base_path+'raw/untappd/delta'
untappd_query_path =base_path+'query/untappd'
untappd_raw_schema_path = base_path + 'raw/untappd/schema'
head = dbutils.fs.head(untappd_raw_schema_path, 10000)
schema = StructType.fromJson(json.loads(head))
months = dbutils.fs.ls('/mnt/default/raw/untappd/2020')
for month in months:
  days = dbutils.fs.ls(month.path)
  for day in days:
    files = dbutils.fs.ls(day.path)
    for file in files:
      df = spark.read.schema(schema).json(file.path)
      df.write.format('delta').mode("append").save(untappd_raw_delta_path)

# COMMAND ----------

for day in days:
  print(dbutils.fs.ls(day.path))

# COMMAND ----------

dbutils.fs.rm('/mnt/default/query/checkpoints', True)

# COMMAND ----------

