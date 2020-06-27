# Databricks notebook source
# MAGIC %md
# MAGIC ### add resiliency for API calls and raw data 
# MAGIC #### add enforced schema

# COMMAND ----------

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

dbutils.fs.put(untappd_raw_path, json.dumps(full_data))

# COMMAND ----------

