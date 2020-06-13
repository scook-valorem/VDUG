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
# display(df_badges_flat)

# COMMAND ----------

df_badges_flat.write.format('delta').mode('overwrite').save(untappd_badges_query_path)

# COMMAND ----------

spark.sql(
'''
CREATE TABLE IF NOT EXISTS untappd_badges
USING DELTA
LOCATION '{}'
'''.format(untappd_badges_query_path)
)