# Databricks notebook source
# MAGIC %md
# MAGIC # Run Bronze layer

# COMMAND ----------

# dbutils.notebook.run('/Stages/bronze',600, {"username":"slcc2c"})

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Silve layer

# COMMAND ----------

dbutils.notebook.run('/Debug/silver_debug',600)

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Gold layer

# COMMAND ----------

dbutils.notebook.run('Stages/gold',600)