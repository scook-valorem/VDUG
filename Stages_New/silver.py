# Databricks notebook source
# MAGIC %md
# MAGIC # TODO
# MAGIC #### clean up flattening logic
# MAGIC #### Remove Venue from fact table
# MAGIC #### Create a cleanup job

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.legacy.timeParserPolicy = LEGACY

# COMMAND ----------

from pyspark.sql.functions import col, explode, when, split, to_timestamp, size

# COMMAND ----------

print(untappd_raw_delta_path)
print(untappd_base_query_path)

# COMMAND ----------

df_raw = spark.read.format('delta').option('ignoreChanges', True).load(untappd_raw_delta_path)
df_date_fix = df_raw.withColumn('created_at', split(df_raw.created_at,',')[1])
df_date_fix = df_date_fix.withColumn('created_at', split(df_date_fix.created_at,' \\+')[0])
df_date_fix = df_date_fix.withColumn('created_at', to_timestamp(df_date_fix.created_at, 'dd MMM yyyy HH:mm:ss'))
df = df_date_fix.dropDuplicates(['checkin_id','created_at']).withWatermark('created_at', '24 hours')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### extract venues

# COMMAND ----------

from pyspark.sql.functions import col, json_tuple, from_json, schema_of_json
schema = schema_of_json('''{"venue_id":9917985,"venue_name":"Untappd at Home","venue_slug":"untappd-at-home","primary_category_key":"Residence","primary_category":"Residence","parent_category_id":"4e67e38e036454776db1fb3a","categories":{"count":1,"items":[{"category_key":"home_private","category_name":"Home (private)","category_id":"4bf58dd8d48988d103941735","is_primary":true}]},"location":{"venue_address":"","venue_city":"","venue_state":"Everywhere","venue_country":"United States","lat":34.2347,"lng":-77.9482},"contact":{"twitter":"","venue_url":""},"foursquare":{"foursquare_id":"5e7b4d99c91df60008e8b168","foursquare_url":"https://4sq.com/3bDWYuq"},"venue_icon":{"sm":"https://untappd.akamaized.net/venuelogos/venue_9917985_b3a5d245_bg_64.png","md":"https://untappd.akamaized.net/venuelogos/venue_9917985_b3a5d245_bg_88.png","lg":"https://untappd.akamaized.net/venuelogos/venue_9917985_b3a5d245_bg_176.png?v=1"},"is_verified":true}''')
df = df.withColumn("venue", from_json(df.venue, schema))

# COMMAND ----------

write_delta_table(df = df, name = 'untappd', primary_key = 'checkin_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brewery

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_brewery_flattened = flatten_df(df.select(df.brewery))

# COMMAND ----------

for col_name in df_brewery_flattened.columns:
  splits = col_name.split('brewery_')
  name = splits[len(splits) - 1]
  df_brewery_flattened = df_brewery_flattened.withColumnRenamed(col_name,name)


# COMMAND ----------

write_delta_table(df_brewery_flattened, 'brewery')

# COMMAND ----------

# register_delta_table('brewery')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comments

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_comments = df.select(df.comments.count.alias('comments_count'), explode(df.comments.items), df.comments.total_count.alias('total_count'))
df_comments_flattened = flatten_df(df_comments)

# COMMAND ----------

for col in df_comments_flattened.columns:
  splits = col.split('col_')
  name = splits[len(splits) - 1]
  df_comments_flattened = df_comments_flattened.withColumnRenamed(col,name)
df_comments_flattened = df_comments_flattened.withColumn('user_brewery_details_tmp', explode('user_brewery_details'))
df_comments_flattened = df_comments_flattened.withColumn('user_venue_details_tmp', explode('user_venue_details'))
df_comments_flattened = df_comments_flattened.drop('user_venue_details', 'user_brewery_details')
df_comments_flattened = df_comments_flattened.withColumnRenamed('user_venue_details_tmp','user_venue_details').withColumnRenamed('user_brewery_details_tmp','user_brewery_details')


# COMMAND ----------

df_comments_flattened.writeStream.format('delta').option('path',  untappd_base_query_path+'comments').option('checkpointLocation', untappd_base_query_path+'/checkpoints').trigger(once=True).start()

# COMMAND ----------

# %sql
#   CREATE TABLE IF NOT EXISTS comments
#   USING DELTA
#   LOCATION 'dbfs:/mnt/default/query/comments'

# COMMAND ----------

# df_comments_upsert = df_comments_flattened.join(spark.table('comments'), 'comment_id', 'left_anti')
# write_delta_table(df_comments_upsert, 'comments', untappd_base_query_path+'comments', True)

# COMMAND ----------

# MAGIC 
# MAGIC %md
# MAGIC ### Media

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_media = df.select(df.media.count.alias('photo_count'), explode(df.media.items).alias('photos'))
df_media_flattened = df_media.select(df_media.photo_count, df_media.photos.photo.photo_img_lg.alias('img_lg'),df_media.photos.photo.photo_img_md.alias('img_md'),df_media.photos.photo.photo_img_og.alias('img_og'),df_media.photos.photo.photo_img_sm.alias('img_sm'),df_media.photos.photo_id.alias('id'))

# COMMAND ----------

write_delta_table(df_media_flattened, 'media')

# COMMAND ----------

# register_delta_table( 'media')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_source = df.select(df.source, df.checkin_id)
df_source_flattened = df_source.select(df_source.source.app_name.alias('app_name'), df_source.source.app_website.alias('app_website'), df.checkin_id)

# COMMAND ----------

write_delta_table(df_source_flattened, 'source')

# COMMAND ----------

# register_delta_table( 'source')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Fact Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Beer Fact Table

# COMMAND ----------

df_beer_facts = df.select(df.beer.bid, df.checkin_id).withColumnRenamed('beer.bid','beer_bid')
write_delta_table(df_beer_facts,'fact_beer')

# COMMAND ----------

# register_delta_table('fact_beer')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Brewery Fact Table

# COMMAND ----------

df_brewery_facts = df.select(df.brewery.brewery_id, df.checkin_id).withColumnRenamed('brewery.brewery_id','brewery_bid')
write_delta_table(df_brewery_facts,'fact_brewery')

# COMMAND ----------

# register_delta_table('fact_brewery')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Comments Fact Table

# COMMAND ----------

df_comments_facts = df.select(df.checkin_id, explode(df.comments.items).alias('items'))
df_comments_facts_clean = df_comments_facts.select(df_comments_facts.checkin_id, df_comments_facts.items.comment_id).alias('comment_id')

# COMMAND ----------

write_delta_table(df_comments_facts_clean,'fact_comments')

# COMMAND ----------

# register_delta_table('fact_comments')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Media Fact Table

# COMMAND ----------

df_media_facts = df.select(df.checkin_id, explode(df.media.items).alias('items'))
df_media_facts_clean = df_media_facts.select(df_media_facts.checkin_id, df_media_facts.items.photo_id.alias('photo_id'))


# COMMAND ----------

write_delta_table(df_media_facts_clean,'fact_media')

# COMMAND ----------

# register_delta_table('fact_media')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Toasts Fact Table

# COMMAND ----------

df_toasts_facts = df.select(df.checkin_id, explode(df.toasts.items).alias('items'))
df_toasts_facts_clean = df_toasts_facts.select(df_toasts_facts.checkin_id, df_toasts_facts.items.like_id.alias('like_id'))

# COMMAND ----------

write_delta_table(df_toasts_facts_clean,'fact_toasts')

# COMMAND ----------

# register_delta_table('fact_toasts')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Badges Fact Table

# COMMAND ----------

df_badges_facts = df.select(df.checkin_id, explode(df.badges.items).alias('items'))
df_badges_facts_flat = df_badges_facts.select(df_badges_facts.checkin_id,df_badges_facts.items.badge_id.alias('badge_id'), df_badges_facts.items.user_badge_id.alias('user_badge_id'))

# COMMAND ----------

write_delta_table(df_badges_facts_flat,'fact_badges')

# COMMAND ----------

# register_delta_table('fact_badges')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Primary Fact Table

# COMMAND ----------

df_facts = df.select(df.checkin_comment,df.checkin_id,df.created_at,df.rating_score, df.beer.bid.alias('beer_bid'), df.brewery.brewery_id, df.user.uid.alias('uid'), df.venue.venue_id.alias('venue_id'))

# COMMAND ----------

# df_facts_upsert = df_facts.join(spark.table('facts'), 'checkin_id', 'left_anti')
write_delta_table(df_facts,'facts')

# COMMAND ----------

# register_delta_table('facts')

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
# write_delta_table(df_user_flat,'user', untappd_base_query_path+'user', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Venue Fact

# COMMAND ----------

df_venue_facts = df.select(df.checkin_id, df.venue.venue_id)

# COMMAND ----------

write_delta_table(df_venue_facts,'fact_venue')

# COMMAND ----------

# register_delta_table('fact_venue')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cognitive Services
# MAGIC Ingest sentiment table from raw and integrate into delta layer

# COMMAND ----------

sentiment_raw_path = base_path+'raw/sentiment/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
sentiment_raw_delta_path = base_path+'raw/sentiment/delta'
sentiment_query_path =base_path+'query/sentiment'

# COMMAND ----------

df_sentiment_raw = spark.readStream.format('delta').option('ignoreChanges', True).load(sentiment_raw_delta_path)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_sentiment_raw_flat = df_sentiment_raw.withColumn('sentiment',col('sentiment')[0]).withColumn('statistics', col('sentiment').statistics).withColumn('documentScores', col('sentiment').documentScores).withColumn('documentScores', col('sentiment').documentScores).withColumn('warnings', col('sentiment').warnings).withColumn('sentiment', col('sentiment').sentiment)

# COMMAND ----------

write_delta_table(df_sentiment_raw_flat,'sentiment')

# COMMAND ----------

# register_delta_table('Sentiment')

# COMMAND ----------

df_sentiment_raw_sentences_flat = df_sentiment_raw.withColumn('sentences', col('sentiment').sentences).select(col('sentences'), col('checkin_id')).withColumn('sizes', size(col('sentences'))).where(col('sizes') > 0).select(explode(col('sentences')).alias('sentences_exploded'), col('checkin_id')).select(explode(col('sentences_exploded')).alias('sentences_exploded'), col('checkin_id')).withColumn('text', col('sentences_exploded').text).withColumn('sentiment', col('sentences_exploded').sentiment).withColumn('confidenceScores', col('sentences_exploded').confidenceScores).withColumn('offset', col('sentences_exploded').offset).withColumn('length', col('sentences_exploded').length).withColumn('confidence_positive', col('confidenceScores').positive).withColumn('confidence_neutral', col('confidenceScores').neutral).withColumn('confidence_negative', col('confidenceScores').negative).drop(col('sentences_exploded')).drop(col('confidenceScores'))

# COMMAND ----------

write_delta_table(df_sentiment_raw_sentences_flat,'sentiment_sentences')

# COMMAND ----------

# register_delta_table('sentiment_sentences')

# COMMAND ----------

dbutils.notebook.exit("Success")