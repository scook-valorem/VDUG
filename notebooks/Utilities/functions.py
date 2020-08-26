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

# add type hint for dataframe
def write_delta_table(df, name: str ):
  df.writeStream.format('delta').option('path',  '{}{}'.format(untappd_base_query_path,name)).option('checkpointLocation', untappd_base_query_path+'{}/checkpoints'.format(name)).outputMode("append").trigger(once=True).start()
def register_delta_table(name: str):
  spark.sql(
  '''
  CREATE TABLE IF NOT EXISTS {}
  USING DELTA
  LOCATION '{}'
  '''.format(name, '{}{}'.format(untappd_base_query_path,name))
  )

# COMMAND ----------

def clean_flat_column_names(df, col_name):
  df_clean = df
  for col in df.columns:
    splits = col.split('{}_'.format(col_name))
    name = splits[len(splits) - 1]
    df_clean = df_clean.withColumnRenamed(col,name)
  return df_clean

# COMMAND ----------

