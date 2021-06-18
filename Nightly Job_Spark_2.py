# Databricks notebook source
# MAGIC %md
# MAGIC # Run Bronze layer

# COMMAND ----------

dbutils.notebook.run('Stages/bronze',600, {"username":"slcc2c"})

# COMMAND ----------

# MAGIC %md
# MAGIC # Run Bronze Cognitive layer

# COMMAND ----------

dbutils.notebook.run('Stages/bronze_cognitive',600, {})

# COMMAND ----------

# %md
# Run Silver layer

# COMMAND ----------

# dbutils.notebook.run('Stages/silver',600)

# COMMAND ----------

# %md
# Run Gold layer

# COMMAND ----------

# dbutils.notebook.run('Stages/gold',600)