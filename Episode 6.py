# Databricks notebook source
dbutils.fs.unmount("/mnt/default")


# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "",
           "fs.azure.account.oauth2.client.secret":"",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/5c8085d9-1e88-4bb6-b5bd-e6e6d5b5babd/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://default@mixerdemolake.dfs.core.windows.net/",
  mount_point = "/mnt/default",
  extra_configs = configs)