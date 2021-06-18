# Databricks notebook source
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

# TODO add type hint for dataframe
from delta.tables import *
def write_delta_table(df, name : str, primary_key : str, zone = 'query' ):
  if zone == 'query':
    print('writing {} to query zone'.format(name))
    # old logic
    # df.writeStream.format('delta').option('path',  '{}{}'.format(untappd_base_query_path,name)).option('checkpointLocation', untappd_base_query_path+'{}/checkpoints'.format(name)).outputMode("append").trigger(once=True).start()
    try:
      deltaTable = DeltaTable.forPath(spark, '{}{}'.format(untappd_base_query_path,name))
      deltaTable.alias('source').merge(
      df.alias('updates'),
      'source.{0} = updates.{0}'.format(primary_key)) \
      .whenNotMatched().insertAll() \
      .whenMatched().updateAll() \
      .execute()
    except:
      df.write.format('delta').mode("append").save('{}{}'.format(untappd_base_query_path,name))
  elif zone == 'sanctioned':
    # NOTE: All sanctioned zone processing is done in batch
    print('writing batch to sanctioned zone')
    # old logic
    # df.write.format('delta').option('checkpointLocation', untappd_base_sanctioned_path+'{}/checkpoints'.format(name)).option('mergeSchema', True).mode("overwrite").save('{}{}'.format(untappd_base_sanctioned_path,name))
    try:
      deltaTable = DeltaTable.forPath(spark, '{}{}'.format(untappd_base_sanctioned_path,name))
      deltaTable.alias('source').merge(
      df.alias('updates'),
      'source.{0} = updates.{0}'.format(primary_key)) \
      .whenNotMatched().insertAll() \
      .whenMatched().updateAll() \
      .execute()
    except:
      df.write.format('delta').mode("append").save('{}{}'.format(untappd_base_sanctioned_path,name))
  else:
    return "invalid zone option"
  
def register_delta_table(name: str, zone = 'query'):
  if zone == 'query':
    spark.sql(
    '''
    CREATE TABLE IF NOT EXISTS {}
    USING DELTA
    LOCATION '{}'
    '''.format(name, '{}{}'.format(untappd_base_query_path,name))
    )
  elif zone == 'sanctioned':
    spark.sql(
    '''
    CREATE TABLE IF NOT EXISTS {}
    USING DELTA
    LOCATION '{}'
    '''.format(name, '{}{}'.format(untappd_base_sanctioned_path,name))
    )

# COMMAND ----------

def dedupe_delta_table(name):
  df = spark.read.format('delta').option('ignoreChanges', True).load('{}{}'.format(untappd_base_query_path,name)).distinct()
  df.write.format('delta').option('path',  '{}{}'.format(untappd_base_query_path,name)).option('checkpointLocation', untappd_base_query_path+'{}/checkpoints'.format(name)).outputMode("overwrite")

# COMMAND ----------

def clean_flat_column_names(df, col_name):
  df_clean = df
  for col in df.columns:
    splits = col.split('{}_'.format(col_name))
    name = splits[len(splits) - 1]
    df_clean = df_clean.withColumnRenamed(col,name)
  return df_clean

# COMMAND ----------

def infer_schema(file_path, data_type):
  df = spark.read.format(data_type).options('inferSchema', True).load(file_path)
  return df.schema