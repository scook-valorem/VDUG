# Databricks notebook source
# MAGIC %md
# MAGIC ### Toasts

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
# MAGIC #### Extract and Transform Toasts Data

# COMMAND ----------

df_toasts = df.select(df.toasts.count.alias('toasts_count'), explode(df.toasts.items).alias('items'), df.toasts.auth_toast.alias('auth_toast'), df.toasts.total_count.alias('total_count'))
# Flatten the Toasts dataframe
df_toasts_flat = flatten_df(df_toasts)
# Clean up the object names created by the flattening process
df_toasts_flat_clean = clean_flat_column_names(df_toasts_flat,'items')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Toasts Data to Data Lake

# COMMAND ----------

# Write the Toasts dataframe to the datalake
write_delta_table(df_toasts_flat_clean,'toasts', primary_key='like_id')