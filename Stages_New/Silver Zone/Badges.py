# Databricks notebook source
# MAGIC %md
# MAGIC ### Badges

# COMMAND ----------

# MAGIC %md
# MAGIC #### Initialize Notebook

# COMMAND ----------

from pyspark.sql.functions import col, explode, when, split, to_timestamp, size

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read in Untappd Data

# COMMAND ----------

df = spark.read.format('delta').load('{}{}'.format(untappd_base_query_path,'untappd'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extract and Transform Badges Data

# COMMAND ----------

from pyspark.sql.functions import explode
df_badges = df.select(df.badges.count.alias('badge_count'),df.badges.retro_status.alias('retro'),explode(df.badges.items).alias('items'))
df_badges_flat = flatten_df(df_badges)
df_badges_flat_clean = clean_flat_column_names(df_badges_flat, 'items')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Badges Data to Data Lake

# COMMAND ----------

# Write the Badges dataframe to the datalake
write_delta_table(df_badges_flat_clean,'badges', primary_key='badge_id')

# COMMAND ----------

