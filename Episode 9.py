# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop User Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE user

# COMMAND ----------

dbutils.fs.rm('/mnt/default/query/user', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clear Checkpoints

# COMMAND ----------

dbutils.fs.rm(untappd_base_query_path+'/checkpoints', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Comments Table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE comments

# COMMAND ----------

dbutils.fs.rm('/mnt/default/query/comments', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE facts

# COMMAND ----------

dbutils.fs.rm('/mnt/default/query/facts', True)

# COMMAND ----------

