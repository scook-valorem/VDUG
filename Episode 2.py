# Databricks notebook source
# MAGIC %md
# MAGIC ### import configuration

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

untappd_badges_query_path =base_path+'query/untappd_badges'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC from untappd

# COMMAND ----------

df = spark.table('untappd')

# COMMAND ----------

from pyspark.sql.functions import explode
df_badges = df.select(df.checkin_id, df.badges.count.alias('badge_count'),df.badges.retro_status.alias('retro'),explode(df.badges.items).alias('items'))

# COMMAND ----------

df_badges_flat = df_badges.select(df_badges.checkin_id, df_badges.badge_count,df_badges.retro,df_badges.items.badge_description.alias('description'),df_badges.items.badge_id.alias('id'), df_badges.items.badge_image.lg.alias('lg_image'), df_badges.items.badge_image.md.alias('md_image'), df_badges.items.badge_image.sm.alias('sm_image'), df_badges.items.badge_name.alias('name'), df_badges.items.created_at.alias('time'), df_badges.items.user_badge_id.alias('user_badge_id'))
display(df_badges_flat)

# COMMAND ----------

df_badges_flat.write.format('delta').save(untappd_badges_query_path)

# COMMAND ----------

spark.sql(
'''
CREATE TABLE untappd_badges
USING DELTA
LOCATION '{}'
'''.format(untappd_badges_query_path)
)

# COMMAND ----------

df = spark.table('untappd')

# COMMAND ----------

nested_df = df_badges

# COMMAND ----------


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

# COMMAND ----------

# MAGIC %md
# MAGIC #checkin_id:long
# MAGIC #badge_count:long
# MAGIC #retro:boolean
# MAGIC #description:string
# MAGIC #id:long
# MAGIC #lg_image:string
# MAGIC #md_image:string
# MAGIC #sm_image:string
# MAGIC #name:string
# MAGIC #time:string
# MAGIC #user_badge_id:long

# COMMAND ----------

tmp = nested_df.select(columns)
tmp = tmp.withColumnRenamed('items_badge_description','description').withColumnRenamed('items_badge_id','id').withColumnRenamed('items_badge_name','name').withColumnRenamed('items_created_at','time').withColumnRenamed('items_user_badge_id','user_badge_id').withColumnRenamed('items_badge_image_lg','lg_image').withColumnRenamed('items_badge_image_md','md_image').withColumnRenamed('items_badge_image_sm','sm_image')
tmp = tmp.select('checkin_id', 'badge_count', 'retro', 'description', 'id', 'lg_image','md_image','sm_image','name','time','user_badge_id')

# COMMAND ----------

df_badges_flat.sort('time').subtract(tmp.sort('time')).show()

# COMMAND ----------

tmp.subtract(df_badges_flat).show()

# COMMAND ----------

