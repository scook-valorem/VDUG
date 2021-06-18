# Databricks notebook source
# MAGIC %md # Data Catalog Table
# MAGIC ###### Author: Eddie Edgeworth

# COMMAND ----------

# MAGIC %md #### Initialize

# COMMAND ----------

# MAGIC %run "/Framework/Orchestration/Notebook Functions"

# COMMAND ----------

# MAGIC %run /Framework/Development/Utilities

# COMMAND ----------

import json
import uuid
from pyspark.sql.functions import col, coalesce, length, lit, when, lower, substring, min, max, avg, stddev, count, countDistinct, isnan, isnull, desc
from pyspark.sql.types import FloatType, StringType, LongType
from functools import reduce
from pyspark.sql import DataFrame
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

dbutils.widgets.text(name="catalogName", defaultValue="", label="DB Catalog Name")
dbutils.widgets.text(name="tableName", defaultValue="", label="Table Name")

catalogName = dbutils.widgets.get("catalogName")
tableName = dbutils.widgets.get("tableName")
fullyQualifiedTableName = "{0}.{1}".format(catalogName, tableName)

context = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
p = {
  "catalogName": catalogName,
  "tableName": tableName
}
parameters = json.dumps(p)

notebookLogGuid = str(uuid.uuid4())

print("Notebook Log Guid: {0}".format(notebookLogGuid))
print("Context: {0}".format(context))
print("Parameters: {0}".format(parameters))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Refresh, Sample and Cache Table

# COMMAND ----------

refreshed = refreshTable(fullyQualifiedTableName)
if refreshed == False:
  log_notebook_end(notebookLogGuid, server, database, login, pwd)
  dbutils.notebook.exit("Table does not exist")
dfList = []
dfList.append(spark.table(fullyQualifiedTableName))
while dfList[-1].count() > 100000:
  dfList.append(dfList[-1].sample(False,.01))
dfList[-1].cache()

# COMMAND ----------

# MAGIC %md #### Catalog Columns

# COMMAND ----------

vcAll = []
sumAll = []

for c in dfList[-1].dtypes:
  vc, sum = dataCatalogColumn(catalogName, tableName, dfList[-1], c[0], c[1])
  vcAll.append(vc)
  sumAll.append(sum)
  
vcUnioned = reduce(DataFrame.unionAll, vcAll)
sumUnioned = reduce(DataFrame.unionAll, sumAll)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Stage data in Bronze Zone

# COMMAND ----------

vcUnionedFiltered = vcUnioned.filter("value is not null").filter("value <> ''")
vcPk = pkCol(vcUnionedFiltered, "Catalog,Table,Column,DataType,value")
sumPk = pkCol(sumUnioned, "Catalog,Table,Column")

vcUpsertTableName = "bronze.dataCatalog_{0}_{1}_vc".format(catalogName, tableName)
sumUpsertTableName = "bronze.dataCatalog_{0}_{1}_sum".format(catalogName, tableName)

spark.sql("DROP TABLE IF EXISTS " + vcUpsertTableName)
spark.sql("DROP TABLE IF EXISTS " + sumUpsertTableName)

vcPk.write.saveAsTable(vcUpsertTableName)
sumPk.write.saveAsTable(sumUpsertTableName)

bronzeDataPath = "{0}/{1}/{2}/{3}/".format(bronzeBasePath, "datacatalog", catalogName, tableName)

vcSql = "CREATE TABLE IF NOT EXISTS {0} USING delta LOCATION '{1}/{2}/{3}_vc'".format(vcUpsertTableName, bronzeDataPath, catalogName, tableName)
sumSql = "CREATE TABLE IF NOT EXISTS {0} USING delta LOCATION '{1}/{2}/{3}_sum'".format(sumUpsertTableName, bronzeDataPath, catalogName, tableName)

spark.sql(vcSql)
spark.sql(sumSql)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Delta Table

# COMMAND ----------

def mergeHelper(destinationTableName, stagingTableName, catalogName, tableName):
  sql = """
  MERGE INTO silverprotected.{0} AS tgt
  USING {1} AS src
  ON tgt.pk = src.pk AND tgt.Catalog = src.Catalog AND tgt.Table = src.Table AND tgt.Column = src.Column AND tgt.Catalog = '{2}' AND tgt.Table = '{3}'
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  """.format(destinationTableName, stagingTableName, catalogName, tableName)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

def deleteHelper(destinationTableName, stagingTableName, catalogName, tableName):
  sql = "DELETE FROM silverprotected.{0} WHERE Catalog = '{2}' AND Table = '{3}' AND pk NOT IN (SELECT pk FROM {1})".format(destinationTableName, stagingTableName, catalogName, tableName)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

mergeHelper("dataCatalogValueCounts", vcUpsertTableName, catalogName, tableName)
deleteHelper("dataCatalogValueCounts", vcUpsertTableName, catalogName, tableName)
mergeHelper("dataCatalogSummary", sumUpsertTableName, catalogName, tableName)
deleteHelper("dataCatalogSummary", sumUpsertTableName, catalogName, tableName)