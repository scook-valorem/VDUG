# Databricks notebook source
df = spark.table('_00_10_json')

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Component Statuses

# COMMAND ----------

display(df.where(col('Body_dataItemType') == 'componentStatus'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### component status value counts

# COMMAND ----------

display(df.where(col('Body_dataItemType') == 'componentStatus').groupBy('Body_Value_Status').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### component status value counts
# MAGIC #### Gray

# COMMAND ----------

display(df.where((col('Body_dataItemType') == 'componentStatus') & (col('Body_Value_Status')=='Gray')).groupBy('Body_Value_Status',"Body_Value_telemetryValue","Body_Value_description").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### component status value counts
# MAGIC #### Yellow

# COMMAND ----------

display(df.where((col('Body_dataItemType') == 'componentStatus') & (col('Body_Value_Status')=='Yellow')).groupBy('Body_Value_Status',"Body_Value_telemetryValue","Body_Value_description").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### component status value counts
# MAGIC #### Green

# COMMAND ----------

display(df.where((col('Body_dataItemType') == 'componentStatus') & (col('Body_Value_Status')=='Green')).groupBy('Body_Value_Status',"Body_Value_telemetryValue","Body_Value_description").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### component status value counts
# MAGIC #### Red

# COMMAND ----------

display(df.where((col('Body_dataItemType') == 'componentStatus') & (col('Body_Value_Status')=='Red')).groupBy('Body_Value_Status',"Body_Value_telemetryValue","Body_Value_description").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### component status value counts
# MAGIC #### Gray

# COMMAND ----------

display(df.where((col('Body_dataItemType') == 'componentStatus') & (col('Body_Value_Status')=='Gray')).groupBy('Body_Value_Status',"Body_Value_telemetryValue","Body_Value_description").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Asset Statuses
# MAGIC These all have a null value for telemetryValue and description
# MAGIC Probably relies on enrichment from other sources via Id values

# COMMAND ----------

display(df.where(col('Body_dataItemType') == 'assetStatus'))

# COMMAND ----------

