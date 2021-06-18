# Databricks notebook source
# MAGIC %md
# MAGIC ### add resiliency for API calls and raw data 
# MAGIC ### add a schema inference catch in case the schema file is lost
# MAGIC ### consider using an upsert to only add new data 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### imports

# COMMAND ----------

import requests
import json
import datetime
from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ### import configuration

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

dbutils.widgets.text("username","", label="username for untappd API")
username = dbutils.widgets.get("username")
untappd_raw_path = base_path+'raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
untappd_raw_delta_path = base_path+'raw/untappd/delta'
untappd_query_path =base_path+'query/untappd'
untappd_raw_schema_path = base_path + 'raw/untappd/schema'

# COMMAND ----------

print(untappd_raw_path)
print(username)
print(untappd_token)

# COMMAND ----------

user = requests.get("https://api.untappd.com/v4/user/info/"+username+"?access_token="+untappd_token)
user_data = user.json()
total_checkins = user_data["response"]["user"]["stats"]["total_checkins"]
print("Total Checkins = " + str(total_checkins))

# COMMAND ----------

print(user_data)

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
    try:
      full_data.extend(checkins_data["response"]["checkins"]["items"])
    except TypeError:
      break
# for data in full_data:
#     full_data_json.append({})
#     for key in data.keys():
#         if(type(data[key]) is dict):
#             full_data_json[i][key] = json.dumps(data[key])
#         else:
#             full_data_json[i][key] = data[key]
#     i = i+1

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.put(untappd_raw_path, json.dumps(full_data), True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest our schema file from the Data Lake

# COMMAND ----------

head = dbutils.fs.head(untappd_raw_schema_path, 10000)
schema = StructType.fromJson(json.loads(head))

# COMMAND ----------

df = spark.read.schema(schema).json(untappd_raw_path)

# COMMAND ----------

# df.show()

# COMMAND ----------

df.write.format('delta').mode("append").save(untappd_raw_delta_path)

# COMMAND ----------

dbutils.notebook.exit("Success")