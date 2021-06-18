# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename/Fix Untappd_Badges

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE
# MAGIC FROM badges

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE badges 

# COMMAND ----------

untappd_base_query_path =base_path+'query/'
dbutils.fs.rm(untappd_base_query_path+'/badges', True)

# COMMAND ----------

