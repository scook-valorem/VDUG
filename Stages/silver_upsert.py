# Databricks notebook source
# MAGIC %md
# MAGIC # TODO 
# MAGIC #### fix dupes
# MAGIC #### adress 'ignoreChanges'
# MAGIC #### string object not callable for toast/comments
# MAGIC #### add a primary key to source
# MAGIC #### fact table for venues

# COMMAND ----------

# MAGIC %md
# MAGIC ### import configuration

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

from pyspark.sql.functions import col, explode, when

# COMMAND ----------

print(untappd_raw_delta_path)
print(untappd_base_query_path)

# COMMAND ----------

df = spark.readStream.format('delta').load(untappd_raw_delta_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extract venues

# COMMAND ----------

from pyspark.sql.functions import col, json_tuple, from_json, schema_of_json
schema = schema_of_json('''{"venue_id":9917985,"venue_name":"Untappd at Home","venue_slug":"untappd-at-home","primary_category_key":"Residence","primary_category":"Residence","parent_category_id":"4e67e38e036454776db1fb3a","categories":{"count":1,"items":[{"category_key":"home_private","category_name":"Home (private)","category_id":"4bf58dd8d48988d103941735","is_primary":true}]},"location":{"venue_address":"","venue_city":"","venue_state":"Everywhere","venue_country":"United States","lat":34.2347,"lng":-77.9482},"contact":{"twitter":"","venue_url":""},"foursquare":{"foursquare_id":"5e7b4d99c91df60008e8b168","foursquare_url":"https://4sq.com/3bDWYuq"},"venue_icon":{"sm":"https://untappd.akamaized.net/venuelogos/venue_9917985_b3a5d245_bg_64.png","md":"https://untappd.akamaized.net/venuelogos/venue_9917985_b3a5d245_bg_88.png","lg":"https://untappd.akamaized.net/venuelogos/venue_9917985_b3a5d245_bg_176.png?v=1"},"is_verified":true}''')
df = df.withColumn("venue", from_json(df.venue, schema))

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

df_badges_upsert.writeStream.format('delta').option('path', untappd_base_query_path+'/badges').option('checkpointLocation', untappd_base_query_path+'/checkpoints').option('ignoreChanges', True).outputMode("append").trigger(once=True).start()

# COMMAND ----------

spark.sql(
'''
CREATE TABLE IF NOT EXISTS badges
USING DELTA
LOCATION '{}'
'''.format(untappd_base_query_path+'/badges')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL badges

# COMMAND ----------

# MAGIC %md
# MAGIC ### Beer

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_beer_flattened = flatten_df(df.select(df.beer)).withColumnRenamed('beer_beer_abv','abv').withColumnRenamed('beer_beer_active','active').withColumnRenamed('beer_beer_name','name').withColumnRenamed('beer_beer_slug','slug').withColumnRenamed('beer_beer_style','style').withColumnRenamed('beer_beer_bid','id').withColumnRenamed('beer_has_had','has_had').withColumnRenamed('beer_beer_label','label')
df_beer_flattened_cleaned = df_beer_flattened.withColumn('active_bool',when(df_beer_flattened.active > 0 , True).otherwise(False)).drop('active').withColumnRenamed('active_bool','active')

# COMMAND ----------

# df_beer_flattened_checkin = df_beer_flattened_cleaned.join(spark.table('facts').select('checkin_id', 'beer_bid'), 'beer_bid')
# df_beer_checkin = spark.table('facts').select('checkin_id', 'beer_bid').join(spark.table('beer'),'beer_bid')

# COMMAND ----------

df_beer_upsert = df_beer_flattened_cleaned.join(spark.table('beer'), 'beer_bid', 'left_anti')
create_register_delta_table(df = df_beer_upsert, name = 'beer', path = untappd_base_query_path+'beer')

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

df_brewery_flattened_upsert = df_brewery_flattened.join(spark.table('brewery'), 'id', 'left_anti')
create_register_delta_table(df_brewery_flattened_upsert, 'brewery', untappd_base_query_path+'brewery', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comments

# COMMAND ----------

# from pyspark.sql.functions import explode, when
# df_comments = df.select(df.comments.count.alias('comments_count'), explode(df.comments.items), df.comments.total_count.alias('total_count'))
# df_comments_flattened = flatten_df(df_comments)

# COMMAND ----------

# for col in df_comments_flattened.columns:
#   splits = col.split('col_')
#   name = splits[len(splits) - 1]
#   df_comments_flattened = df_comments_flattened.withColumnRenamed(col,name)
# df_comments_flattened = df_comments_flattened.withColumn('user_brewery_details_tmp', explode('user_brewery_details'))
# df_comments_flattened = df_comments_flattened.withColumn('user_venue_details_tmp', explode('user_venue_details'))
# df_comments_flattened = df_comments_flattened.drop('user_venue_details', 'user_brewery_details')
# df_comments_flattened = df_comments_flattened.withColumnRenamed('user_venue_details_tmp','user_venue_details').withColumnRenamed('user_brewery_details_tmp','user_brewery_details')


# COMMAND ----------

# df_comments_flattened.writeStream.format('delta').option('path',  untappd_base_query_path+'comments').option('checkpointLocation', untappd_base_query_path+'/checkpoints').trigger(once=True).start()

# COMMAND ----------

# %sql
#   CREATE TABLE IF NOT EXISTS comments
#   USING DELTA
#   LOCATION 'dbfs:/mnt/default/query/comments'

# COMMAND ----------

# df_comments_upsert = df_comments_flattened.join(spark.table('comments'), 'comment_id', 'left_anti')
# create_register_delta_table(df_comments_upsert, 'comments', untappd_base_query_path+'comments', True)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### Media

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_media = df.select(df.media.count.alias('photo_count'), explode(df.media.items).alias('photos'))
df_media_flattened = df_media.select(df_media.photo_count, df_media.photos.photo.photo_img_lg.alias('img_lg'),df_media.photos.photo.photo_img_md.alias('img_md'),df_media.photos.photo.photo_img_og.alias('img_og'),df_media.photos.photo.photo_img_sm.alias('img_sm'),df_media.photos.photo_id.alias('id'))

# COMMAND ----------

df_media_flattened_upsert = df_media_flattened.join(spark.table('media'), 'id', 'left_anti')
create_register_delta_table(df_media_flattened_upsert, 'media', untappd_base_query_path+'media', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_source = df.select(df.source)
df_source_flattened = df_source.select(df_source.source.app_name.alias('app_name'), df_source.source.app_website.alias('app_website'))

# COMMAND ----------

df_source_flattened_upsert = df_source_flattened.join(spark.table('source'),'app_name','left_anti')
create_register_delta_table(df_source_flattened_upsert, 'source', untappd_base_query_path+'source', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Beer Fact Table

# COMMAND ----------

df_beer_facts = df.select(df.beer.bid, df.checkin_id).withColumnRenamed('beer.bid','beer_bid')
df_beer_facts_clean_upsert = df_beer_facts.join(spark.table('fact_beer'), 'checkin_id', 'left_anti')
create_register_delta_table(df_beer_facts_clean_upsert,'fact_beer', untappd_base_query_path+'fact_beer', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Brewery Fact Table

# COMMAND ----------

df_brewery_facts = df.select(df.brewery.brewery_id, df.checkin_id).withColumnRenamed('brewery.brewery_id','brewery_bid')
df_brewery_facts_clean_upsert = df_brewery_facts.join(spark.table('fact_brewery'), 'checkin_id', 'left_anti')
create_register_delta_table(df_brewery_facts_clean_upsert,'fact_brewery', untappd_base_query_path+'fact_brewery', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Comments Fact Table

# COMMAND ----------

df_comments_facts = df.select(df.checkin_id, explode(df.comments.items).alias('items'))
df_comments_facts_clean = df_comments_facts.select(df_comments_facts.checkin_id, df_comments_facts.items.comment_id).alias('comment_id')

# COMMAND ----------

df_comments_facts_clean_upsert = df_comments_facts_clean.join(spark.table('fact_comments'), 'checkin_id', 'left_anti')
create_register_delta_table(df_comments_facts_clean_upsert,'fact_comments', untappd_base_query_path+'fact_comments', True)

# COMMAND ----------

df_media_facts = df.select(df.checkin_id, explode(df.media.items).alias('items'))
df_media_facts_clean = df_media_facts.select(df_media_facts.checkin_id, df_media_facts.items.photo_id.alias('photo_id'))


# COMMAND ----------

df_media_facts_clean_upsert = df_media_facts_clean.join(spark.table('fact_media'), 'checkin_id', 'left_anti')
create_register_delta_table(df_media_facts_clean_upsert,'fact_media', untappd_base_query_path+'fact_media', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Toasts Fact Table

# COMMAND ----------

df_toasts_facts = df.select(df.checkin_id, explode(df.toasts.items).alias('items'))
df_toasts_facts_clean = df_toasts_facts.select(df_toasts_facts.checkin_id, df_toasts_facts.items.like_id.alias('like_id'))

# COMMAND ----------

df_toasts_facts_clean_upsert = df_toasts_facts_clean.join(spark.table('fact_comments'), 'checkin_id', 'left_anti')
create_register_delta_table(df_toasts_facts_clean_upsert,'fact_toasts', untappd_base_query_path+'fact_toasts', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Badges Fact Table

# COMMAND ----------

df_badges_facts = df.select(df.checkin_id, explode(df.badges.items).alias('items'))
df_badges_facts_flat = df_badges_facts.select(df_badges_facts.checkin_id,df_badges_facts.items.badge_id.alias('badge_id'), df_badges_facts.items.user_badge_id.alias('user_badge_id'))

# COMMAND ----------

df_badges_facts_flat_upsert = df_badges_facts_flat.join(spark.table('fact_badges'), 'checkin_id', 'left_anti')
create_register_delta_table(df_badges_facts_flat_upsert,'fact_badges', untappd_base_query_path+'fact_badges', True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Primary Fact Table

# COMMAND ----------

df_facts = df.select(df.checkin_comment,df.checkin_id,df.created_at,df.rating_score, df.beer.bid.alias('beer_bid'), df.brewery.brewery_id, df.user.uid.alias('uid'), df.venue.venue_id.alias('venue_id'))

# COMMAND ----------

# df_facts_upsert = df_facts.join(spark.table('facts'), 'checkin_id', 'left_anti')
create_register_delta_table(df_facts,'facts', untappd_base_query_path+'facts', register=True)

# COMMAND ----------

df_facts_upsert = df_facts.join(spark.table('facts'), 'checkin_id', 'left_anti')
create_register_delta_table(df_facts_upsert,'facts', untappd_base_query_path+'facts', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### User
# MAGIC ##### disabled until we have other users

# COMMAND ----------

# df_user = df.select(df.user)
# df_user_flat = df_user.select(df_user.user.bio, df_user.user.contact.facebook, df_user.user.contact.foursquare, df_user.user.contact.twitter, df_user.user.first_name, df_user.user.is_private, df_user.user.is_supporter, df_user.user.last_name, df_user.user.location, df_user.user.relationship, df_user.user.uid, df_user.user.url, df_user.user.user_avatar, df_user.user.user_name)
# for col in df_user_flat.columns:
#   splits = col.split('.')
#   name = splits[len(splits) - 1]
#   df_user_flat = df_user_flat.withColumnRenamed(col,name)
# df_user_flat_dedupe = df_user_flat.drop_duplicates()

# COMMAND ----------

# df_user_flat_dedupe_upsert = df_user_flat_dedupe.join(spark.table('user'), 'uid', 'left_anti')
# create_register_delta_table(df_user_flat,'user', untappd_base_query_path+'user', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Toasts

# COMMAND ----------

# df_toasts = df.select(df.toasts.count.alias('toasts_count'), explode(df.toasts.items).alias('items'), df.toasts.auth_toast.alias('auth_toast'), df.toasts.total_count.alias('total_count'))


# COMMAND ----------

# df_toasts = df.select(df.toasts.count.alias('toasts_count'), explode(df.toasts.items).alias('items'), df.toasts.auth_toast.alias('auth_toast'), df.toasts.total_count.alias('total_count'))
# df_toasts_flat = flatten_df(df_toasts)
# df_toasts_flat_clean = clean_flat_column_names(df_toasts_flat,'items')

# COMMAND ----------

# df_toasts_flat_clean_upsert = df_toasts_flat_clean.join(spark.table('toasts'), 'like_id', 'left_anti')
# create_register_delta_table(df_toasts_flat_clean_upsert,'toasts', untappd_base_query_path+'toasts', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Venue

# COMMAND ----------

df_venue_flat = flatten_df(df.select(df.venue))
for col in df_venue_flat.columns:
  splits = col.split('venue_')
  name = splits[len(splits) - 1]
  df_venue_flat = df_venue_flat.withColumnRenamed(col,name)

# COMMAND ----------

df_venue_flat_exploded = df_venue_flat.withColumn('categories_items', explode(df_venue_flat.categories_items))
df_venue_flat_exploded = df_venue_flat_exploded.withColumn('category_id', df_venue_flat_exploded.categories_items.category_id).withColumn('category_key', df_venue_flat_exploded.categories_items.category_key).withColumn('category_name', df_venue_flat_exploded.categories_items.category_name).withColumn('is_primary', df_venue_flat_exploded.categories_items.is_primary).drop(df_venue_flat_exploded.categories_items).withColumnRenamed('id', 'venue_id')

# COMMAND ----------

df_venues_flat_clean_upsert = df_venue_flat_exploded.join(spark.table('venues'), 'venue_id', 'left_anti')
create_register_delta_table(df_venues_flat_clean_upsert,'venues', untappd_base_query_path+'venues', register=True)

# COMMAND ----------

