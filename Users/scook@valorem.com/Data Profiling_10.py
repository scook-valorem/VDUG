# Databricks notebook source
# MAGIC %md # Data Profiling
# MAGIC ###### Author: Eddie Edgeworth
# MAGIC 
# MAGIC This notebook requires the cluster to have the pandas_profiling PyPi library loaded

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run ./Utilities

# COMMAND ----------

import numpy as np
import pandas as pd
import pandas_profiling
import json
from datetime import datetime, timedelta
import hashlib
import uuid
import matplotlib

dbutils.widgets.text(name="stepLogGuid", defaultValue="00000000-0000-0000-0000-000000000000", label="stepLogGuid")
dbutils.widgets.text(name="stepKey", defaultValue="-1", label="stepKey")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table")
dbutils.widgets.text(name="deltaHistoryMinutes", defaultValue="-1", label="History Minutes")
dbutils.widgets.text(name="samplePercent", defaultValue="-1", label="Sample Percentage")

stepLogGuid = dbutils.widgets.get("stepLogGuid")
stepKey = int(dbutils.widgets.get("stepKey"))
tableName = dbutils.widgets.get("tableName")
deltaHistoryMinutes = int(dbutils.widgets.get("deltaHistoryMinutes"))
samplePercent = float(dbutils.widgets.get("samplePercent"))

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()

p = {
  "stepLogGuid": stepLogGuid,
  "stepKey": stepKey,
  "tableName": tableName,
  "deltaHistoryMinutes": deltaHistoryMinutes,
  "samplePercent": samplePercent
}
parameters = json.dumps(p)

notebookLogGuid = str(uuid.uuid4())
# log_notebook_start(notebookLogGuid, stepLogGuid, stepKey, parameters, context, server, database, login, pwd)

print("Notebook Log Guid: {0}".format(notebookLogGuid))
print("Step Log Guid: {0}".format(stepLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh Table

# COMMAND ----------

refreshed = refreshTable(tableName)
if refreshed == False:
  log_notebook_end(notebookLogGuid, server, database, login, pwd)
  dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(tableName))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Obtain Change Delta

# COMMAND ----------

if deltaHistoryMinutes != -1:
  try:
    print("Obtaining time travel delta")
    dfList.append(getTableChangeDelta(dfList[-1], tableName, deltaHistoryMinutes))
    if dfList[-1].count == 0:
      log_notebook_end(notebookLogGuid, server, database, login, pwd)
      dbutils.notebook.exit("No new or modified rows to process.")
  except Exception as e:
    err = {
      "sourceName" : "Data Profiling: Obtain Change Delta",
      "errorCode" : "100",
      "errorDescription" : e.__class__.__name__    
    }
    error = json.dumps(err)
    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sample Percentage

# COMMAND ----------

if samplePercent != -1:
  try:
    print("Sampling {0} Percent of Dataframe".format(samplePercent * 100))
    dfList.append(dfList[-1].sample(samplePercent))
  except Exception as e:
    err = {
      "sourceName": "Data Profiling: Sample Percentage",
      "errorCode": "200",
      "errorDescription": e.__class__.__name__
    }
    error = json.dumps(err)
    log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
    raise(e)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Convert to Pandas

# COMMAND ----------

try:
  pdf = dfList[-1].toPandas()
except Exception as e:
  err = {
    "sourceName": "Data Profiling: Convert to Pandas",
    "errorCode": "300",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pandas Profiling

# COMMAND ----------

try:
  pr = pandas_profiling.ProfileReport(pdf) 
except Exception as e:
  err = {
    "sourceName": "Data Profiling: Pandas Profiling",
    "errorCode": "400",
    "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
  log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)  

# COMMAND ----------

pr.html

# COMMAND ----------

pr.to_html()

# COMMAND ----------

dbutils.fs.ls('/FileStore')

# COMMAND ----------

dbutils.fs.put(pr.to_html(),'/FileStore/tables')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display Profiling Report

# COMMAND ----------

try:
  displayHTML(pr.html)
except Exception as e:
  err = {
      "sourceName": "Data Profiling: Display Profiling Report",
      "errorCode": "500",
      "errorDescription": e.__class__.__name__
  }
  error = json.dumps(err)
#   log_notebook_error(notebookLogGuid, error, server, database, login, pwd)
  raise(e)

# COMMAND ----------

#can also get the raw json and peruse it, but not sure this is valuable enough to implement
#prJson = json.loads(pr.json)
#prJson.keys()
#correlations = prJson['correlations']
#correlations.keys()
#pearson = correlations['pearson']

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

log_notebook_end(notebookLogGuid, server, database, login, pwd)
dbutils.notebook.exit("Succeeded")