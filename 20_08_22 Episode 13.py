# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

tables = spark.sql('SHOW TABLES').select('tableName').take(19)

# COMMAND ----------

for table in tables:
  print("removing {}".format(table.tableName))
  spark.sql('DROP TABLE {}'.format(table.tableName))

# COMMAND ----------

dbutils.fs.ls('mnt/default/query')

# COMMAND ----------

dbutils.fs.rm('mnt/default/query', True)

# COMMAND ----------

import requests
import json
import datetime

# COMMAND ----------

untappd_raw_path = base_path+'raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
untappd_raw_delta_path = base_path+'raw/untappd/delta'

# COMMAND ----------

df = spark.read.json(untappd_raw_path)

# COMMAND ----------

display(df)

# COMMAND ----------

count = 0
for record in dbutils.fs.ls('mnt/default/raw/untappd'):
  for record in dbutils.fs.ls(record.path):
    for record in dbutils.fs.ls(record.path):
      df = spark.read.json(record.path)
      if df.count():
        count = count + df.count()
        print("appending from {}".format(record.path))
        df.write.format('delta').mode("append").save(untappd_raw_delta_path)
print(count)

# COMMAND ----------

print(count)

# COMMAND ----------

df_raw = spark.read.format('delta').load(untappd_raw_delta_path)

# COMMAND ----------

display(df_raw)

# COMMAND ----------

dbutils.fs.rm('mnt/default/query/checkpoints', True)

# COMMAND ----------

dbutils.fs.rm('mnt/default/query/untappd', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT *
# MAGIC FROM untappd

# COMMAND ----------

df = spark.table('untappd')

# COMMAND ----------

df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.dropDuplicates(['checkin_id','created_at']))

# COMMAND ----------

df.dropDuplicates(['checkin_id','created_at']).count()

# COMMAND ----------

