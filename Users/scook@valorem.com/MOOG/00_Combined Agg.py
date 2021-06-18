# Databricks notebook source
# MAGIC %md
# MAGIC # comes from assets table

# COMMAND ----------

df = spark.table('MES')

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## change types

# COMMAND ----------

df = df.withColumn('EnqueuedTimeUtc', col('EnqueuedTimeUtc').cast(TimestampType())).withColumn('SystemProperties_enqueuedTime', col('SystemProperties_enqueuedTime').cast(TimestampType())).withColumn('EnqueuedTimeUtc', col('EnqueuedTimeUtc').cast(TimestampType()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### exploration

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.where(col('Body_Value_eventId') == '591368b9-0be9-4f2d-8eac-cf197b3ab8a7'))

# COMMAND ----------

display(df.where(col('Body_Value_eventId') == 'a1915d0c-591e-40bf-9c50-2e9e4fb28c2a'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Component Statuses

# COMMAND ----------

display(df.where(col('Body_dataItemType') == 'componentStatus'))

# COMMAND ----------

display(df.where(col('Body_dataItemType') == 'componentStatus').groupBy('Body_AssetId','Body_Value_ComponentName','Body_Value_Status').count())

# COMMAND ----------

display(df.where(col('Body_AssetId')=='06a70edd-ce90-4494-aecb-8488ecac0ed8'))

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
# MAGIC ## Asset Statuses
# MAGIC These all have a null value for telemetryValue and description
# MAGIC Probably relies on enrichment from other sources via Id values

# COMMAND ----------

display(df.where(col('Body_dataItemType') == 'assetStatus'))

# COMMAND ----------

