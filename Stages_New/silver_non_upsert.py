# Databricks notebook source
# MAGIC %md
# MAGIC ### import configuration

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

from pyspark.sql.functions import col, explode, when

# COMMAND ----------



# COMMAND ----------

print(untappd_raw_delta_path)
print(untappd_base_query_path)

# COMMAND ----------

df = spark.readStream.format('delta').load(untappd_raw_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Badges

# COMMAND ----------

from pyspark.sql.functions import explode
df_badges = df.select(df.checkin_id, df.badges.count.alias('badge_count'),df.badges.retro_status.alias('retro'),explode(df.badges.items).alias('items'))

# COMMAND ----------

df_badges_flat = df_badges.select(df_badges.checkin_id, df_badges.badge_count,df_badges.retro,df_badges.items.badge_description.alias('description'),df_badges.items.badge_id.alias('id'), df_badges.items.badge_image.lg.alias('lg_image'), df_badges.items.badge_image.md.alias('md_image'), df_badges.items.badge_image.sm.alias('sm_image'), df_badges.items.badge_name.alias('name'), df_badges.items.created_at.alias('time'), df_badges.items.user_badge_id.alias('user_badge_id'))
# display(df_badges_flat)

# COMMAND ----------

df_badges_upsert = df_badges_flat.join(spark.table('badges'), 'checkin_id', 'left_anti')

# COMMAND ----------

df_badges_upsert.writeStream.format('delta').option('path', untappd_base_query_path+'/badges').option('checkpointLocation', untappd_base_query_path+'/checkpoints').trigger(once=True).start()

# COMMAND ----------

spark.sql(
'''
CREATE TABLE IF NOT EXISTS badges
USING DELTA
LOCATION '{}'
'''.format(untappd_base_query_path+'/badges')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Beer

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_beer_flattened = flatten_df(df.select(df.beer)).withColumnRenamed('beer_beer_abv','abv').withColumnRenamed('beer_beer_active','active').withColumnRenamed('beer_beer_name','name').withColumnRenamed('beer_beer_slug','slug').withColumnRenamed('beer_beer_style','style').withColumnRenamed('beer_beer_bid','id').withColumnRenamed('beer_has_had','has_had').withColumnRenamed('beer_beer_label','label')
df_beer_flattened_cleaned = df_beer_flattened.withColumn('active_bool',when(df_beer_flattened.active > 0 , True).otherwise(False)).drop('active').withColumnRenamed('active_bool','active')

# COMMAND ----------

df_beer_flattened_checkin = df_beer_flattened_cleaned.join(spark.table('facts').select('checkin_id', 'beer_bid'), 'beer_bid')
df_beer_checkin = spark.table('facts').select('checkin_id', 'beer_bid').join(spark.table('beer'),'beer_bid')

# COMMAND ----------

df_badges_upsert = df_beer_flattened_checkin.join(df_beer_checkin, 'checkin_id', 'left_anti')
create_register_delta_table(df = df_beer_flattened_cleaned, name = 'beer', path = untappd_base_query_path+'beer')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brewery

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_brewery_flattened = flatten_df(df.select(df.brewery))

# COMMAND ----------

for col in df_brewery_flattened.columns:
  splits = col.split('brewery_')
  name = splits[len(splits) - 1]
  df_brewery_flattened = df_brewery_flattened.withColumnRenamed(col,name)


# COMMAND ----------

create_register_delta_table(df_brewery_flattened, 'brewery', untappd_base_query_path+'brewery', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comments

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_comments = df.select(df.comments.count.alias('comments_count'), explode(df.comments.items), df.comments.total_count.alias('total_count'), df.checkin_id)


# COMMAND ----------

for col in df_comments.columns:
  splits = col.split('col_')
  name = splits[len(splits) - 1]
  df_comments = df_comments.withColumnRenamed(col,name)

# COMMAND ----------

create_register_delta_table(df_comments, 'comments', untappd_base_query_path+'comments', True)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### Media

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_media = df.select(df.media.count.alias('photo_count'), explode(df.media.items).alias('photos'))
df_media_flattened = df_media.select(df_media.photo_count, df_media.photos.photo.photo_img_lg.alias('img_lg'),df_media.photos.photo.photo_img_md.alias('img_md'),df_media.photos.photo.photo_img_og.alias('img_og'),df_media.photos.photo.photo_img_sm.alias('img_sm'),df_media.photos.photo_id.alias('id'))

# COMMAND ----------

create_register_delta_table(df_media_flattened, 'media', untappd_base_query_path+'media', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_source = df.select(df.source)
df_source_flattened = df_source.select(df_source.source.app_name.alias('app_name'), df_source.source.app_website.alias('app_website'))

# COMMAND ----------

create_register_delta_table(df_source_flattened, 'source', untappd_base_query_path+'source', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Comments Fact Table

# COMMAND ----------

display(df)

# COMMAND ----------

display(df.select(df.comments.items))

# COMMAND ----------

df_comments_facts = df.select(df.checkin_id, explode(df.comments.items).alias('items'))
df_comments_facts_clean = df_comments_facts.select(df_comments_facts.checkin_id, df_comments_facts.items.comment_id).alias('comment_id')

# COMMAND ----------

display(df_comments_facts_clean)

# COMMAND ----------

create_register_delta_table(df_comments_facts_clean,'fact_comments', untappd_base_query_path+'fact_comments', True)

# COMMAND ----------

df_media_facts = df.select(df.checkin_id, explode(df.media.items).alias('items'))
df_media_facts_clean = df_media_facts.select(df_media_facts.checkin_id, df_media_facts.items.photo_id.alias('photo_id'))


# COMMAND ----------

create_register_delta_table(df_media_facts_clean,'fact_media', untappd_base_query_path+'fact_media', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Toasts Fact Table

# COMMAND ----------

df_toasts_facts = df.select(df.checkin_id, explode(df.toasts.items).alias('items'))
df_toasts_facts_clean = df_toasts_facts.select(df_toasts_facts.checkin_id, df_toasts_facts.items.like_id.alias('like_id'))

# COMMAND ----------

create_register_delta_table(df_toasts_facts_clean,'fact_toasts', untappd_base_query_path+'fact_toasts', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Badges Fact Table

# COMMAND ----------

df_badges_facts = df.select(df.checkin_id, explode(df.badges.items).alias('items'))
df_badges_facts_flat = df_badges_facts.select(df_badges_facts.checkin_id,df_badges_facts.items.badge_id.alias('badge_id'), df_badges_facts.items.user_badge_id.alias('user_badge_id'))

# COMMAND ----------

create_register_delta_table(df_badges_facts_flat,'fact_badges', untappd_base_query_path+'fact_badges', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Primary Fact Table

# COMMAND ----------

df_facts = df.select(df.checkin_comment,df.checkin_id,df.created_at,df.rating_score,df.venue, df.beer.bid.alias('beer_bid'), df.brewery.brewery_id, df.user.uid)

# COMMAND ----------

create_register_delta_table(df_facts,'facts', untappd_base_query_path+'facts', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### User

# COMMAND ----------

df_user = df.select(df.user)
df_user_flat = df_user.select(df_user.user.bio, df_user.user.contact.facebook, df_user.user.contact.foursquare, df_user.user.contact.twitter, df_user.user.first_name, df_user.user.is_private, df_user.user.is_supporter, df_user.user.last_name, df_user.user.location, df_user.user.relationship, df_user.user.uid, df_user.user.url, df_user.user.user_avatar, df_user.user.user_name)

# COMMAND ----------

create_register_delta_table(df_user_flat,'user', untappd_base_query_path+'user', True)

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

