# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/00_10.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

from pyspark.sql.functions import col
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

from pyspark.sql.functions import unbase64, lit,decode, from_json, col
from pyspark.sql.types import StringType, MapType, StructType, StructField
def clean_MES(df):
  df_decoded = df.withColumn('Body',decode(unbase64(df.Body),'utf-8'))
  return flatten_df(flatten_df(df_decoded.withColumn('Body', from_json(col('Body'),StructType([StructField("dataItemType", StringType(), True),StructField("assetId", StringType(), True),StructField("value", StringType(), True)],)))).drop(col('SystemProperties')).withColumn('SystemProperties_connectionAuthMethod', from_json(col('SystemProperties_connectionAuthMethod'), StructType([StructField("scope", StringType(), True),StructField("type", StringType(), True),StructField("issuer", StringType(), True),StructField("acceptingIpFilterRule", StringType(), True)],))).withColumn('Body_Value', from_json(col('Body_Value'), StructType([StructField("eventId", StringType(), True),StructField("assetId", StringType(), True),StructField("telemetryValue", StringType(), True),StructField("description", StringType(), True),StructField("dateTime", StringType(), True),StructField("componentName", StringType(), True),StructField("status", StringType(), True)],))))

# COMMAND ----------

df_flat = clean_MES(df)

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/00_04.json"
file_type = "json"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df_4 = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# COMMAND ----------

# Create a view or table

temp_table_name = "MES"

df_flat.unionAll(clean_MES(df_4)).createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE MES

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "MES"

flatten_df(df_decoded_flat).write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

