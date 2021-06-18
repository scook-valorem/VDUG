# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

tables = spark.sql('SHOW TABLES').select('tableName').take(20)

# COMMAND ----------

for table in tables:
  print("removing {}".format(table.tableName))
  spark.sql('DROP TABLE {}'.format(table.tableName))

# COMMAND ----------

df_fact = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'facts')
display(df_fact)

# COMMAND ----------

