# Databricks notebook source
# MAGIC %md
# MAGIC # Run Bronze layer

# COMMAND ----------

dbutils.notebook.run('Stages/bronze_cognitive',600, {})

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Silver layer

# COMMAND ----------

dbutils.notebook.run('Stages/silver_cognitive',600)

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Gold layer

# COMMAND ----------

dbutils.notebook.run('Stages/gold',600)