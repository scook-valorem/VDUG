# Databricks notebook source
# MAGIC %md 
# MAGIC ### imports

# COMMAND ----------

import requests
import json
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### import configuration

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

dbutils.widgets.text("username","", label="username for untappd API")
username = dbutils.widgets.get("username")
untappd_raw_path = base_path+'raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
untappd_query_path =base_path+'query/untappd'

# COMMAND ----------

print(untappd_raw_path)

# COMMAND ----------

user = requests.get("https://api.untappd.com/v4/user/info/"+username+"?access_token="+untappd_token)
user_data = user.json()
total_checkins = user_data["response"]["user"]["stats"]["total_checkins"]
print("Total Checkins = " + str(total_checkins))


# COMMAND ----------

checkins = requests.get("https://api.untappd.com/v4/user/checkins/"+username+"?access_token="+untappd_token)
checkins_data = checkins.json()
full_data = checkins_data["response"]["checkins"]["items"]
full_data_json = []
i = 0


while (len(full_data) < total_checkins):
    max_id   = checkins_data["response"]["pagination"]["max_id"]
    next_url = "https://api.untappd.com/v4/user/checkins/"+username+"?max_id=" + str(max_id) + "&access_token="+untappd_token
    print("Downloading " + next_url)
    checkins = requests.get(next_url)
    checkins_data = checkins.json()

    full_data.extend(checkins_data["response"]["checkins"]["items"])
for data in full_data:
    full_data_json.append({})
    for key in data.keys():
        if(type(data[key]) is dict):
            full_data_json[i][key] = json.dumps(data[key])
        else:
            full_data_json[i][key] = data[key]
    i = i+1

# COMMAND ----------

print(checkins_data)

# COMMAND ----------

dbutils.fs.ls(base_path)

# COMMAND ----------

spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
# use if you don't already have a container
# dbutils.fs.ls("abfss://<file-system-name>@<storage-account-name>.dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")
spark.conf.set(
  base_path, storage_key
  )

# COMMAND ----------

with open('/mnt/default/raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day), 'w+') as file:
  json.dump(full_data_json, file)

# COMMAND ----------

dbutils.fs.mount(source = base_path, mount_point = '/mnt/default', extra_configs = {"fs.azure.account.auth.type":"CustomAccessToken", "fs.azure.account.custom.token.provider.class" : spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")})

# COMMAND ----------

dbutils.fs.ls('/mnt')

# COMMAND ----------

f = open('dbfs:/mnt/default/test.txt', 'w+')

# COMMAND ----------



# COMMAND ----------

help(dbutils.fs.put)

# COMMAND ----------

dbutils.fs.put(base_path+'test.txt',"Hello")

# COMMAND ----------

json.dumps(full_data)

# COMMAND ----------

dbutils.fs.put(untappd_raw_path, json.dumps(full_data))

# COMMAND ----------

dbutils.fs.ls(base_path)

# COMMAND ----------

df = spark.read.json(untappd_raw_path)

# COMMAND ----------

df.show()

# COMMAND ----------

df.write.format("delta").save(untappd_query_path)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE untappd

# COMMAND ----------

spark.sql(
'''
CREATE TABLE untappd
USING DELTA
LOCATION '{}'
'''.format(untappd_query_path)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM untappd

# COMMAND ----------

# MAGIC %md
# MAGIC I. dbutils.fs.put
# MAGIC II. Azure Blob SDK in python
# MAGIC III. REST API