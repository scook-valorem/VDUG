# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

"default/raw/untappd/2020/9/29"

# COMMAND ----------

dbutils.fs.ls('/mnt/default')

# COMMAND ----------

# MAGIC %md
# MAGIC ## How to mount

# COMMAND ----------

file_system = 'default'
storage_account = 'mixerdemolake'
base_path = "abfss://{0}@{1}.dfs.core.windows.net/".format(file_system,storage_account)

# COMMAND ----------

base_path

# COMMAND ----------

dbutils.fs.mount(source = base_path, mount_point = '/mnt/default', extra_configs = {"fs.azure.account.auth.type":"CustomAccessToken", "fs.azure.account.custom.token.provider.class" : spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")})

# COMMAND ----------

dbutils.fs.ls('/mnt/default/raw/untappd/2020/9/29')

# COMMAND ----------

df = spark.read.json('dbfs:/mnt/default/raw/untappd/2020/9/29/untappd.json')

# COMMAND ----------

df_beer = df.select(df.beer.beer_abv,df.beer.beer_active, df.beer.beer_label, df.beer.beer_name, df.beer.beer_slug, df.beer.beer_style, df.beer.bid, df.beer.has_had)

# COMMAND ----------

df_beer.write.save(path='dbfs:/mnt/default/demos/raw_delta', format='delta')

# COMMAND ----------

spark.sql(
  '''
  CREATE TABLE IF NOT EXISTS demo_beer_dim
  USING DELTA
  LOCATION 'dbfs:/mnt/default/demos/raw_delta'
  '''
  )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM demo_beer_dim

# COMMAND ----------

