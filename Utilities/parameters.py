# Databricks notebook source
import datetime

# COMMAND ----------

scope = 'default'
cognitive_scope = 'Untappd-Cognitive'
storage_account = 'mixerdemolake'
file_system = 'default'
file_system_base_path = "abfss://{0}@{1}.dfs.core.windows.net/".format(file_system,storage_account)
base_path = "dbfs:/mnt/default/"
date = datetime.datetime.now()
untappd_raw_delta_path = base_path+'raw/untappd/delta'
untappd_base_query_path =base_path+'query/'
untappd_base_sanctioned_path = base_path + 'sanctioned/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize Secrets

# COMMAND ----------

storage_key = dbutils.secrets.get(scope=scope,key='key')
storage_connection_string = dbutils.secrets.get(scope=scope,key='connection_string')
cognitive_key = dbutils.secrets.get(scope=cognitive_scope,key='key')
cognitive_endpoint = dbutils.secrets.get(scope=cognitive_scope,key='endpoint')
cognitive_location = dbutils.secrets.get(scope=cognitive_scope,key='location')

# COMMAND ----------

# MAGIC %md
# MAGIC #### untappd

# COMMAND ----------

untappd_token = dbutils.secrets.get(scope='untappd', key='accesstoken')

# COMMAND ----------

