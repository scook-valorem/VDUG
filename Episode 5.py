# Databricks notebook source
# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

dbutils.fs.ls('/mnt')

# COMMAND ----------

dbutils.fs.ls('/mnt/default')

# COMMAND ----------

dbutils.fs.ls('/mnt/default/raw/2020/06/27')

# COMMAND ----------

# MAGIC %md
# MAGIC ### dropping untappd query zone table, only need to read in the raw json

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE untappd

# COMMAND ----------

dbutils.fs.rm('/mnt/default/query/', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ### dropping all tables to change mount path

# COMMAND ----------

tables = ["media","source","untappd_badges"]
for table in tables:
  spark.sql("""
  DROP TABLE {}
  """.format(table))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE beer

# COMMAND ----------

dbutils.fs.unmount("/mnt/default")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename/Fix Untappd_Badges

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE
# MAGIC FROM badges

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE badges 

# COMMAND ----------



# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

from pyspark.sql.functions import col, explode, when

# COMMAND ----------

untappd_raw_path = base_path+'raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
untappd_base_query_path =base_path+'query/'
df = spark.read.json(untappd_raw_path)
df_badges = df.select(col('badges.Count').alias('badge_count'), explode(col('badges.items')).alias('items'), col('badges.retro_status').alias('retro_status'))
df_badges_flat = flatten_df(df_badges)
df_badges_flat_clean = clean_flat_column_names(df_badges_flat,'items')

# COMMAND ----------

display(df_badges_flat_clean)

# COMMAND ----------

create_register_delta_table(df_badges_flat_clean,'badges', untappd_base_query_path+'badges', True)

# COMMAND ----------

dbutils.fs.rm(untappd_base_query_path+'badges', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### streaming reads

# COMMAND ----------

print(untappd_base_query_path)

# COMMAND ----------

df_badges = spark.readStream.format("delta").load(untappd_base_query_path+'badges')

# COMMAND ----------

display(df_badges)

# COMMAND ----------

