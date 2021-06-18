# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "bda4ba66-06f7-4f2a-b02f-36da4d2906c4",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="default",key="service_principle_client_secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5c8085d9-1e88-4bb6-b5bd-e6e6d5b5babd/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://default@mixerdemolake.dfs.core.windows.net/",
  mount_point = "/mnt/default",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.unmount('/mnt/default')|

# COMMAND ----------

dbutils.fs.ls(base_path)

# COMMAND ----------



# COMMAND ----------

