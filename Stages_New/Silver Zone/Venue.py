# Databricks notebook source
# MAGIC %md
# MAGIC ### Venue

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
# MAGIC #### Extract and Transform Venues Data

# COMMAND ----------

df_venue = df.select(df.venue)
df_venue_flat = flatten_df(df_venue)
df_venue_flat_clean = clean_flat_column_names(df_venue_flat,'venue')
df_venue_flat_exploded = df_venue_flat_clean.withColumn('categories_items', explode(df_venue_flat_clean.categories_items))
df_venue_flat_exploded_exploded = df_venue_flat_exploded.withColumn('category_id', df_venue_flat_exploded.categories_items.category_id).withColumn('category_key', df_venue_flat_exploded.categories_items.category_key).withColumn('category_name', df_venue_flat_exploded.categories_items.category_name).withColumn('is_primary', df_venue_flat_exploded.categories_items.is_primary).drop(df_venue_flat_exploded.categories_items).withColumnRenamed('id', 'venue_id')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Venue Data to Data Lake

# COMMAND ----------

# Write the Toasts dataframe to the datalake
write_delta_table(df_venue_flat_exploded_exploded,'venue', primary_key='venue_id')