# Databricks notebook source
# MAGIC %run /Utilities/parameters

# COMMAND ----------

dbutils.fs.unmount("/mnt/default")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "ba686cbc-b9bf-46be-8403-573c9501f298",
           "fs.azure.account.oauth2.client.secret":dbutils.secrets.get(scope='default',key='service_principle_client_secret'),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5c8085d9-1e88-4bb6-b5bd-e6e6d5b5babd/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://default@mixerdemolake.dfs.core.windows.net/",
  mount_point = "/mnt/default",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls(base_path)

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

