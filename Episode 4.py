# Databricks notebook source
# MAGIC %md
# MAGIC ### import configuration

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

untappd_base_query_path =base_path+'query/'

# COMMAND ----------

from pyspark.sql.functions import col, explode, when

# COMMAND ----------

# MAGIC %md
# MAGIC ### Show Available Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

df = spark.table('untappd')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### nested Id's
# MAGIC ##### badges, comments, media, toasts

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Comments Fact Table

# COMMAND ----------

df_comments_facts = df.select(col('checkin_id'), explode(col('comments.items')).alias('items'))
df_comments_facts_clean = df_comments_facts.select(col('checkin_id'), col('items.comment_id').alias('comment_id'))

# COMMAND ----------

create_register_delta_table(df_comments_facts_clean,'fact_comments', untappd_base_query_path+'fact_comments', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Media Fact Table

# COMMAND ----------

df_media_facts = df.select(col('checkin_id'), explode(col('media.items')).alias('items'))
df_media_facts_clean = df_media_facts.select(col('checkin_id'), col('items.photo_id').alias('photo_id'))


# COMMAND ----------

create_register_delta_table(df_media_facts_clean,'fact_media', untappd_base_query_path+'fact_media', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Toasts Fact Table

# COMMAND ----------

df_toasts_facts = df.select(col('checkin_id'), explode(col('toasts.items')).alias('items'))
df_toasts_facts_clean = df_toasts_facts.select(col('checkin_id'), col('items.like_id').alias('like_id'))

# COMMAND ----------

create_register_delta_table(df_toasts_facts_clean,'fact_toasts', untappd_base_query_path+'fact_toasts', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Badges Fact Table

# COMMAND ----------

df_badges_facts = df.select(col('checkin_id'), explode(col('badges.items')).alias('items'))
df_badges_facts_flat = df_badges_facts.select(col('checkin_id'),col('items.badge_id').alias('badge_id'), col('items.user_badge_id').alias('user_badge_id'))

# COMMAND ----------

create_register_delta_table(df_badges_facts_flat,'fact_badges', untappd_base_query_path+'fact_badges', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Primary Fact Table

# COMMAND ----------

df_facts = df.select(col('checkin_comment'),col('checkin_id'),col('created_at'),col('rating_score'),col('venue'), col('beer.bid').alias('beer_bid'), col('brewery.brewery_id'), col('user.uid'))

# COMMAND ----------

create_register_delta_table(df_facts,'facts', untappd_base_query_path+'facts', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### User

# COMMAND ----------

df_user = df.select(df.user)
df_user_flat = flatten_df(df_user)
df_user_flat_clean = clean_flat_column_names(df_user_flat,'user')

# COMMAND ----------

create_register_delta_table(df_user_flat_clean,'user', untappd_base_query_path+'user', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename/Fix Untappd_Badges

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE
# MAGIC FROM untappd_badges

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE untappd_badges

# COMMAND ----------

df_badges = df.select(col('badges.count').alias('badge_count'), explode(col('badges.items')).alias('items'), col('badges.retro_status').alias('retro_status'))
df_badges_flat = flatten_df(df_badges)
df_badges_flat_clean = clean_flat_column_names(df_badges_flat,'items')

# COMMAND ----------

create_register_delta_table(df_badges_flat_clean,'badges', untappd_base_query_path+'badges', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Toasts

# COMMAND ----------

df_toasts = df.select(col('toasts.count').alias('toasts_count'), explode(col('toasts.items')).alias('items'), col('toasts.auth_toast').alias('auth_toast'), col('toasts.total_count').alias('total_count'))
df_toasts_flat = flatten_df(df_toasts)
df_toasts_flat_clean = clean_flat_column_names(df_toasts_flat,'items')

# COMMAND ----------

create_register_delta_table(df_toasts_flat_clean,'toasts', untappd_base_query_path+'toasts', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Viz Example

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM beer
# MAGIC ORDER BY abv DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM brewery

# COMMAND ----------

