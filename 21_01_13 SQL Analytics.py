# Databricks notebook source
# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

untappd_raw_path = base_path+'raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
untappd_raw_delta_path = base_path+'raw/untappd/delta'
untappd_query_path =base_path+'query/untappd'
untappd_raw_schema_path = base_path + 'raw/untappd/schema'

# COMMAND ----------

# from pyspark.sql.functions import cast
from pyspark.sql.types import DateType
df = spark.table('facts')
df_date = df.withColumn('date', df.created_at.cast(DateType()))

# COMMAND ----------

display(df_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     count(f.beer_id) as `Beer Count`,
# MAGIC     CAST( created_at as Date)
# MAGIC FROM default.facts f
# MAGIC INNER JOIN default.beer b on f.beer_id == b.beer_id
# MAGIC GROUP BY CAST( created_at as Date)
# MAGIC ORDER BY `Beer Count` DESC

# COMMAND ----------

display(df_date.where(df_date.date == '2020-03-01'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC CAST( created_at as Date) as `Date`,
# MAGIC checkin_id,
# MAGIC name
# MAGIC FROM facts f
# MAGIC INNER JOIN beer b on f.beer_id == b.beer_id

# COMMAND ----------

