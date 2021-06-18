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
# MAGIC #### Extract and Transform Beer Data

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_beer = df.select(df.beer)
df_beer_flattened = flatten_df(df_beer)
df_beer_flattened_clean = clean_flat_column_names(df_beer_flattened, 'beer')
#.withColumnRenamed('beer_beer_abv','abv').withColumnRenamed('beer_beer_active','active').withColumnRenamed('beer_beer_name','name').withColumnRenamed('beer_beer_slug','slug').withColumnRenamed('beer_beer_style','style').withColumnRenamed('beer_beer_bid','id').withColumnRenamed('beer_has_had','has_had').withColumnRenamed('beer_beer_label','label')
df_beer_flattened_clean_transformed = df_beer_flattened_clean.withColumn('active',when(col('active') > 0 , True).otherwise(False)).withColumnRenamed('bid','beer_bid')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Badges Data to Data Lake

# COMMAND ----------

# Write the Badges dataframe to the datalake
write_delta_table(df_beer_flattened_clean_transformed,'beer', primary_key='beer_bid')

# COMMAND ----------

