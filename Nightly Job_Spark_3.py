# Databricks notebook source
# %md
# # Run Bronze layer

# COMMAND ----------

# dbutils.notebook.run('Stages/bronze',600, {"username":"slcc2c"})

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Silver layer

# COMMAND ----------

dbutils.notebook.run('Stages/silver',600)

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Gold layer

# COMMAND ----------

dbutils.notebook.run('Stages/gold',600)