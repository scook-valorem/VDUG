# Databricks notebook source
import datetime

# COMMAND ----------

scope = 'default'
storage_account = 'mixerdemolake'
file_system = 'default'
base_path = "abfss://{0}@{1}.dfs.core.windows.net/".format(file_system,storage_account)
date = datetime.datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize Secrets

# COMMAND ----------

storage_key = dbutils.secrets.get(scope=scope,key='key')
storage_connection_string = dbutils.secrets.get(scope=scope,key='connection_string')

# COMMAND ----------

# MAGIC %md
# MAGIC #### untappd

# COMMAND ----------

untappd_token = dbutils.secrets.get(scope='untappd', key='accesstoken')

# COMMAND ----------

