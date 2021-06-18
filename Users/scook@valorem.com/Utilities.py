# Databricks notebook source
# MAGIC %md # Utilities
# MAGIC ###### Author: Eddie Edgeworth 2/7/20

# COMMAND ----------

# import pyodbc
import uuid
import datetime

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.{AnalysisException}

# COMMAND ----------

# MAGIC %scala
# MAGIC Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

# COMMAND ----------

def pkCol(df, pkcols):
  df.createOrReplaceTempView("df")
  cols = pkcols.split(",")
  pk = "CONCAT("
  for c in cols:
    col = "CAST(COALESCE(`" + c.strip() + "`,'') AS STRING),"
    pk += col
  pk = pk[0:-1] + ")"
  sql = "SELECT *, {0} AS pk from df".format(pk)
  pkdf = spark.sql(sql)
  return pkdf

# COMMAND ----------

def sparkTableExists(table):
  try:
    exists = (spark.table(table) is not None)
  except:
    exists = False
  return exists

# COMMAND ----------

def checkDeltaFormat(table):
  format = spark.sql("DESCRIBE DETAIL {0}".format(table)).collect()[0][0]
  return format

# COMMAND ----------

def getSchema(dataPath, externalSystem, schema, table, stepLogGuid, basepath, samplingRatio=.5, timeout=6000, zone="silver", delimiter="", header=True, multiLine="False"):
  if zone == "silver":
    path = "{0}/query/schemas/{1}/{2}/{3}/schema.json".format(basepath, externalSystem, schema, table)
    args = {
      "stepLogGuid": stepLogGuid,
      "dataPath": dataPath,
      "externalSystem": externalSystem,
      "schemaName": schema, 
      "tableName": table,
      "samplingRatio": samplingRatio,
      "schemaPath": path
    }
  elif zone == "bronze":
    path = "{0}/raw/schemas/{1}/{2}/schema.json".format(basepath, externalSystem, table)
    args = {
      "stepLogGuid": stepLogGuid,
      "dataPath": dataPath,
      "externalSystem": externalSystem,
      "tableName": tableName,
      "samplingRatio": samplingRatio,
      "delimiter": delimiter,
      "hasHeader": header,
      "schemaPath": path,
      "multiLine": multiLine
    }
  
  try:
    head = dbutils.fs.head(path, 256000)
  except Exception as e:
    dbutils.notebook.run("/Framework/Data Engineering/Silver Zone/Get Schema", timeout, args)
    head = dbutils.fs.head(path, 256000)
    
  import json
  from pyspark.sql.types import StructType
  return StructType.fromJson(json.loads(head))

# COMMAND ----------

def mergeDelta(upsertTable, table, cols):
  def insert(cols):
    c = ["`{0}`".format(c) for c in cols]
    return ",".join(c)
  def values(cols):
    c = ["src.`{0}`".format(c) for c in cols]
    return ",".join(c)    
  def update(cols):
    c = ["tgt.`{0}`=src.`{0}`".format(c) for c in cols if c != "pk"]
    return ",".join(c)
  
  insert = insert(cols)
  values = values(cols)
  update = update(cols)
  
  sql = """
  MERGE INTO {0} AS tgt 
  USING {1} AS src ON src.pk = tgt.pk
  WHEN MATCHED THEN UPDATE SET {2}
  WHEN NOT MATCHED THEN INSERT ({3}) 
  VALUES({4})
  """.format(table, upsertTable, update, insert, values)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

def optimize(table, optimizeWhere="", optimizeZOrderBy=""):
  if optimizeWhere != "":
    where = "WHERE {0}".format(optimizeWhere)
  else:
    where = ""
    
  if optimizeZOrderBy != "":
    zorder = "ZORDER BY ({0})".format(optimizeZOrderBy)
  else:
    zorder = ""
    
  sql = "OPTIMIZE {0} {1} {2}".format(table, where, zorder)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

def vacuum(table, hours):
  sql = "VACUUM {0} RETAIN {1} HOURS".format(table, hours)
  print(sql)
  spark.sql(sql)

# COMMAND ----------

def cleansePath (path, keep):
  files = dbutils.fs.ls(path)
  remove = [f.path for f in files if f.path[-3:] != keep]
  for file in remove:
    dbutils.fs.rm(file)

# COMMAND ----------

def renameFile (path, current, new):
  c = "{0}/{1}".format(path, current)
  n = "{0}/{1}".format(path, new)
  dbutils.fs.mv(c, n)

# COMMAND ----------

def cleanseColumns(df, regex='[^\w]'):
  import re
  cols = []
  for c in df.columns:
    col = re.sub(regex, '', c)
    cols.append(col)
  new = df.toDF(*cols)
  return new

# COMMAND ----------

def flatten_df(nested_df):
    stack = [((), nested_df)]
    columns = []

    while len(stack) > 0:
        parents, df = stack.pop()

        flat_cols = [
            col(".".join(parents + (c[0],))).alias("_".join(parents + (c[0],)))
            for c in df.dtypes
            if c[1][:6] != "struct"
        ]

        nested_cols = [
            c[0]
            for c in df.dtypes
            if c[1][:6] == "struct"
        ]

        columns.extend(flat_cols)

        for nested_col in nested_cols:
            projected_df = df.select(nested_col + ".*")
            stack.append((parents + (nested_col,), projected_df))

    return nested_df.select(columns)

# COMMAND ----------

def flatten_df2(nested_df, layers):
    flat_cols = []
    nested_cols = []
    flat_df = []

    flat_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] != 'struct'])
    nested_cols.append([c[0] for c in nested_df.dtypes if c[1][:6] == 'struct'])

    flat_df.append(nested_df.select(flat_cols[0] +
                               [col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols[0]
                                for c in nested_df.select(nc+'.*').columns])
                  )
    for i in range(1, layers):
        print (flat_cols[i-1])
        flat_cols.append([c[0] for c in flat_df[i-1].dtypes if c[1][:6] != 'struct'])
        nested_cols.append([c[0] for c in flat_df[i-1].dtypes if c[1][:6] == 'struct'])

        flat_df.append(flat_df[i-1].select(flat_cols[i] +
                                [col(nc+'.'+c).alias(nc+'_'+c)
                                    for nc in nested_cols[i]
                                    for c in flat_df[i-1].select(nc+'.*').columns])
        )

    return flat_df[-1]

# COMMAND ----------

def parseJSONCols(df, *cols, sanitize=True):
    res = df
    for i in cols:
        if sanitize:
            res = (
                res.withColumn(
                    i,
                    psf.concat(psf.lit('{"data": '), i, psf.lit('}'))
                )
            )
        schema = spark.read.json(res.rdd.map(lambda x: x[i])).schema
        res = res.withColumn(i, psf.from_json(psf.col(i), schema))
        if sanitize:
            res = res.withColumn(i, psf.col(i).data)
    return res

# COMMAND ----------

# MAGIC %scala
# MAGIC //def pathHasData(path:String, extension:String) = {
# MAGIC //  try
# MAGIC //  {
# MAGIC //    val files = dbutils.fs.ls(path)
# MAGIC //    val data = files.filter { file => file.path.toString.split("\\.").last == extension && file.size > 3 }
# MAGIC //    var ret: = ""
# MAGIC //    if(data) {
# MAGIC //      ret = path
# MAGIC //    } else {
# MAGIC //      ret = ""
# MAGIC //    }
# MAGIC //    (ret)
# MAGIC //  } catch {
# MAGIC //    case x: AnalysisException =>
# MAGIC //    {
# MAGIC //      println(x)
# MAGIC //      (ret)
# MAGIC //    }
# MAGIC //  }
# MAGIC //}

# COMMAND ----------

def pathHasData(path, extension):
  try:
    files = dbutils.fs.ls(path)
    if extension != "":
      data = [f for f in files if f.name[-len(extension):] == extension and f.size > 3]
    else:
      data = [f for f in files if f.size > 3]
    if len(data) > 0:
        return path
    else:
      print("No {0} files to process".format(extension))
      return ""
  except Exception as e:
    print(e)
    return ""

# COMMAND ----------

def refreshTable(tableName):
  try:
    spark.sql("REFRESH TABLE {0}".format(tableName))
    df = spark.table(tableName)
    return True
  except Exception as e:
    return False