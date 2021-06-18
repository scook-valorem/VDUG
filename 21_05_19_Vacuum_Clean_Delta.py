# Databricks notebook source
# MAGIC %sql
# MAGIC Optimize untappd

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM untappd

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE untappd
# MAGIC SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM untappd

# COMMAND ----------

