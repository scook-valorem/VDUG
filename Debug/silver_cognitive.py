# Databricks notebook source
# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.legacy.timeParserPolicy = LEGACY

# COMMAND ----------

from pyspark.sql.functions import col, explode, when, split, to_timestamp

# COMMAND ----------

print(untappd_raw_delta_path)
print(untappd_base_query_path)

# COMMAND ----------

sentiment_raw_path = base_path+'raw/sentiment/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
sentiment_raw_delta_path = base_path+'raw/sentiment/delta'
sentiment_query_path =base_path+'query/sentiment'

# COMMAND ----------

df_raw = spark.readStream.format('delta').option('ignoreChanges', True).load(sentiment_raw_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cognitive Services
# MAGIC Ingest sentiment table from raw and integrate into delta layer

# COMMAND ----------

df_sentiment_flat = flatten_df(df_raw.withColumn('sentiment',explode(col('sentiment'))))

# COMMAND ----------

write_delta_table(df_sentiment_flat,'comment_sentiment')

# COMMAND ----------

register_delta_table('comment_sentiment')

# COMMAND ----------

dbutils.notebook.exit("Success")