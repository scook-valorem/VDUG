# Databricks notebook source
# MAGIC %run ./Utilities

# COMMAND ----------

dbutils.widgets.text(name="tableName",defaultValue="table",label="Table Name")
tableName = dbutils.widgets.get("tableName")

# COMMAND ----------

#rename this when you clone/rename the notebook
notebookName = "EDA"

# COMMAND ----------

# MAGIC %md #### Code

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, length, lit, when, col, lower, substring, min, max, avg, stddev, count, countDistinct, isnan, isnull, desc
from pyspark.sql.types import FloatType, StringType

# COMMAND ----------

def get_summary_statistics_for_dataframe_column(df, columnName, dataType):
  if dataType == "boolean":
    df_summary = (df.select(col(columnName).alias("ColumnName").cast(StringType())))
  else:
    df_summary = (df.select(col(columnName).alias("ColumnName")))
    
  df_summary = (df_summary \
  .agg(
     min("ColumnName").alias("MinimumValue") \
    ,max("ColumnName").alias("MaximumValue") \
    ,avg("ColumnName").alias("AvgValue") \
    ,stddev("ColumnName").alias("StdDevValue") \
    ,countDistinct("ColumnName").alias("DistinctCountValue") \
    ,count(when(isnan("ColumnName") | col("ColumnName").isNull(), "ColumnName")).alias("NumberOfNulls") \
    ,count(when(col("ColumnName")=="", "ColumnName")).alias("NumberOfBlanks") \
    ,count(when(col("ColumnName")==0, "ColumnName")).alias("NumberOfZeros") \
    ,count("*").alias("RecordCount")
      )
             )

  df_summary = (df_summary \
              .selectExpr( \
                          "cast(MinimumValue as string) MinimumValue",
                          "cast(MaximumValue as string) MaximumValue",
                          "cast(AvgValue as float) AvgValue",
                          "cast(StdDevValue as float) StdDevValue",
                          "cast(DistinctCountValue as int) DistinctCountValue",
                          "cast(NumberOfNulls + NumberOfBlanks as int) NumberOfNulls",
                          "cast(NumberOfZeros as int) NumberOfZeros",
                          "cast(RecordCount as int) RecordCount",
                          "cast(NumberOfNulls + NumberOfBlanks / cast(RecordCount as float) as float) PercentNulls ",
                          "cast(NumberOfZeros / cast(RecordCount as float) as float) PercentZeros ",
                          "cast(DistinctCountValue / cast(RecordCount as float) as float) Selectivity "
                         )
             )

  
  return df_summary

# COMMAND ----------

def get_value_counts_for_dataframe_column_asJSONString(df, columnName):
  df_value_counts = (df.select(col(columnName).alias("value")) \
                   .groupBy("value") \
                   .agg(count("*").alias("total")) \
                   .orderBy(desc("total")) \
                   .limit(20)
  )
  vc = df_value_counts.toJSON().collect()
  vc_column = ''.join(vc)
  return vc_column

# COMMAND ----------

def get_value_counts_for_dataframe_column(df, columnName):
  df_value_counts = (df.select(col(columnName).alias("value")) \
                   .groupBy("value") \
                   .agg(count("*").alias("total")) \
                   .orderBy(desc("total")) \
                   .limit(20)
  )
  return df_value_counts

# COMMAND ----------

# MAGIC %md #### Exploratory Data Analysis

# COMMAND ----------

query = "REFRESH TABLE " + tableName
spark.sql(query)
df = spark.table(tableName)
df.cache()
df_columns = df.columns

# COMMAND ----------

datatypes_dict = dict(df.dtypes)

# COMMAND ----------

#delete the output path files
#dbutils.fs.rm(summaryPath,True)

##make this into a Delta Table, append the tables
spark.sql("""
DROP TABLE IF EXISTS EDA_{0}
""".format(tableName))

##make this into a Delta Table, append the tables
spark.sql("""
DROP TABLE IF EXISTS EDA_{0}_TopValueCounts
""".format(tableName))

# COMMAND ----------

# MAGIC %md ###### Total number of columns

# COMMAND ----------

intCols, timestampCols, numericCols, stringCols, boolCols, allCols = schema_to_lists_by_type(df.schema)
print("Integer Columns: {0}".format(len(intCols)))
print("timestamp Columns: {0}".format(len(timestampCols)))
print("numeric Columns: {0}".format(len(numericCols)))
print("string Columns: {0}".format(len(stringCols)))
print("bool Columns: {0}".format(len(boolCols)))
print("all Columns: {0}".format(len(allCols)))

# COMMAND ----------

# MAGIC %md ##### Summary Statistics of each column

# COMMAND ----------

from pyspark.sql.types import StringType, LongType

# COMMAND ----------

for columnName in df.columns:
  print(columnName)
  columnDataType = datatypes_dict.get(columnName)
  vc = get_value_counts_for_dataframe_column(df, columnName)
  df_summary = get_summary_statistics_for_dataframe_column(df, columnName, columnDataType)

  df_summary = (df_summary \
    .withColumn("TableName", lit(tableName)) \
    .withColumn("ColumnName", lit(columnName)) \
    .withColumn("ColumnDataType", lit(columnDataType))
               )
  
  vc_summary = (vc \
    .withColumn("TableName", lit(tableName)) \
    .withColumn("ColumnName", lit(columnName)) \
    .withColumn("ColumnDataType", lit(columnDataType)) \
    .withColumn("value", col("value").cast(StringType())).alias("value") \
    .withColumn("total", col("total").cast(LongType())).alias("total")
               )

  df_summary \
    .write \
    .mode("append") \
    .format("delta") \
    .save(summaryPath)
  
  vc_summary \
    .write \
    .mode("append") \
    .format("delta") \
    .save(vcSummaryPath)

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS EDA_{0}
""".format(tableName))

spark.sql("""
CREATE TABLE EDA_{0}
USING DELTA
LOCATION '{1}'
""".format(tableName, summaryPath))

spark.sql("""
OPTIMIZE EDA_{0} ZORDER BY TableName, ColumnName
""".format(tableName))

spark.sql("""
DROP TABLE IF EXISTS EDA_{0}_TopValueCounts
""".format(tableName))

spark.sql("""
CREATE TABLE EDA_{0}_TopValueCounts
USING DELTA
LOCATION '{1}'
""".format(tableName, vcSummaryPath))

spark.sql("""
OPTIMIZE EDA_{0}_TopValueCounts ZORDER BY TableName, ColumnName
""".format(tableName))

# COMMAND ----------

#display(spark.sql("""
#SELECT ColumnName, ColumnDataType, TopValueCounts, Selectivity, PercentNulls, PercentZeros, MinimumValue, MaximumValue, AvgValue, StdDevValue, DistinctCountValue, NumberOfNulls, NumberOfZeros, RecordCount
#FROM eda_{0}
#""".format(tableName))
#       )

# COMMAND ----------

# MAGIC %md #### Log Completion

# COMMAND ----------

# MAGIC %scala
# MAGIC //Log Completed
# MAGIC val logMessage = "Completed"
# MAGIC val notebookContext = ""
# MAGIC log_to_framework_db_scala (notebookPath:String, logMessage:String, notebookContext:String) 

# COMMAND ----------

dbutils.notebook.exit("Succeeded")