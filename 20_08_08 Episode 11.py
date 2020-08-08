# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM facts

# COMMAND ----------

df = spark.table('facts')

# COMMAND ----------

from pyspark.sql.functions import to_date
df_date = df.withColumn('created_at_formatted', to_date(df.created_at, 'E, d M y k:m:s x'))

# COMMAND ----------

display(df_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT created_at, date_format(created_at,'d')
# MAGIC FROM facts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(date 'Fri, 01 Dec 2017 02:12:21 +0000','d')

# COMMAND ----------

from pyspark.sql.functions import split
df_date = df.withColumn('created_at_split', split(df.created_at,',')[1])

# COMMAND ----------

display(df_date)

# COMMAND ----------

df_date_tmp = df_date.withColumn('created_at_split', split(df_date.created_at_split,' \\+')[0])

# COMMAND ----------

display(df_date_tmp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.legacy.timeParserPolicy = LEGACY

# COMMAND ----------

# MAGIC %md
# MAGIC # CORRECT

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
display(df_date_tmp.withColumn('created_at_date', to_timestamp(df_date_tmp.created_at_split, 'dd MMM yyyy HH:mm:ss')).select('created_at_split','created_at_date'))

# COMMAND ----------

from pyspark.sql.functions import date_format
display(df_date.withColumn('created_at_date', date_format(df_date.created_at_split,'dd')).select('created_at_split','created_at_date'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format('28 Jul 2020 01:29:45')

# COMMAND ----------

df_beer = spark.table('beer')

# COMMAND ----------

checkin_id:long
beer_bid:long
abv:double
label:string
name:string
slug:string
style:string
has_had:boolean
active:boolean