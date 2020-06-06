# Databricks notebook source
# MAGIC %run /Utilities/parameters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Secrets

# COMMAND ----------

dbutils.help()

# COMMAND ----------

secrets = dbutils.secrets()

# COMMAND ----------

secrets.listScopes()

# COMMAND ----------

secrets.get('default','test')

# COMMAND ----------

secrets.list('default')

# COMMAND ----------

secrets.list(scope)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Lake Connection (Token)

# COMMAND ----------

spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
# use if you don't already have a container
# dbutils.fs.ls("abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

# COMMAND ----------

spark.conf.set(
  base_path, storage_key
  )

# COMMAND ----------

dbutils.fs.ls(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Credential Passthru ex.

# COMMAND ----------

# spark.conf.unset(base_path)

# COMMAND ----------

dbutils.fs.ls(base_path)

# COMMAND ----------

