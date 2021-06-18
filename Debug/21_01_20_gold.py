# Databricks notebook source
# MAGIC %md
# MAGIC # TODO 
# MAGIC #### Fix Has Had

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

from pyspark.sql.functions import concat, avg, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ### Badge Views

# COMMAND ----------

df_badges = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'badges')
df_badges_unique = df_badges.withColumnRenamed('id','badge_id').withColumnRenamed('name', 'badge_name').drop('time').distinct()
df_badges_unique_sk = df_badges_unique.withColumn('sk_badges', concat(col('checkin_id'), col('badge_id')))

# COMMAND ----------

display(df_badges_unique_sk)

# COMMAND ----------

write_delta_table(df_badges_unique_sk, 'badges', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('badges', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Beer Views

# COMMAND ----------

df_beer = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'beer')
df_beer_unique = df_beer.withColumnRenamed('beer_bid','beer_id').distinct()

# COMMAND ----------

write_delta_table(df_beer_unique, 'beer', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('beer', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brewery Views

# COMMAND ----------

df_brewery = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'brewery')
df_brewery_unique = df_brewery.withColumnRenamed('id','brewery_id').distinct()
df_brewery_unique_agg = df_brewery_unique.groupby("brewery_id", "name", "active", "label", "page_url", "slug", "type", "country_name", "city", "state", "contact_facebook", "contact_instagram", "contact_twitter", "contact_url").agg(avg("location_lat"), avg("location_lng")).withColumnRenamed('avg(location_lat)','location_lat').withColumnRenamed('avg(location_lng)','location_lng')

# COMMAND ----------

write_delta_table(df_brewery_unique_agg, 'brewery', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('brewery', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comments Views

# COMMAND ----------

df_comments = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'comments')
df_comments_unique = df_comments.distinct()

# COMMAND ----------

write_delta_table(df_comments_unique, 'comments', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('comments', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### media Views

# COMMAND ----------

df_media = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'media')
df_media_unique = df_media.withColumnRenamed('id','media_id').distinct()

# COMMAND ----------

write_delta_table(df_media_unique, 'media_test', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('media_test', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### source Views

# COMMAND ----------

df_source = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'source')
df_source_unique = df_source.withColumnRenamed('id','source_id').distinct()

# COMMAND ----------

write_delta_table(df_source_unique, 'source', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('source', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### toasts Views

# COMMAND ----------

df_toasts = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'toasts')
df_toasts_unique = df_toasts.withColumnRenamed('like_id','toast_id').distinct()

# COMMAND ----------

write_delta_table(df_toasts_unique, 'toasts', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('toasts', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### venues Views

# COMMAND ----------

df_venues = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'venues')
df_venues_unique = df_venues.distinct()

# COMMAND ----------

write_delta_table(df_venues_unique, 'venues', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('venues', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Views
# MAGIC ### Fact

# COMMAND ----------

df_facts = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'facts')
df_facts_unique = df_facts.withColumnRenamed('brewery.brewery_id','brewery_id').withColumnRenamed('beer_bid','beer_id').distinct()
df_facts_upsert = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'manual_import_fact')\
          .select(col('comment').alias('checkin_comment'), col('checkin_id'), col('created_at'), col('rating_score'),col('bid').alias('beer_id'), col('brewery_id'), lit('2746346').alias('uid'))\
          .withColumn('sk_beers', concat(col('checkin_id'), col('beer_id'))).withColumn('sk_brewery', concat(col('checkin_id'), col('brewery_id')))
df_facts_unique_upsert = df_facts_upsert.join(df_facts_unique, df_facts_upsert.checkin_id == df_facts_unique.checkin_id, 'right_outer')\
          .select(df_facts_upsert.checkin_comment, df_facts_upsert.checkin_id, df_facts_upsert.created_at, df_facts_upsert.rating_score,df_facts_upsert.beer_id, df_facts_upsert.brewery_id, df_facts_unique.venue_id, df_facts_upsert.uid).distinct()
df_facts_unique_sk = df_facts_unique_upsert.withColumn('sk_beers', concat(col('checkin_id'), col('beer_id'))).withColumn('sk_brewery', concat(col('checkin_id'), col('brewery_id')))

# COMMAND ----------

write_delta_table(df_facts_unique_sk, 'facts', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('facts', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Beer Facts

# COMMAND ----------

df_facts_beer = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'fact_beer')
df_facts_beer_unique = df_facts_beer.withColumnRenamed('beer_bid','beer_id').distinct()
df_facts_beer_unique_sk = df_facts_beer_unique.withColumn('sk_beers', concat(col('checkin_id'), col('beer_id')))

# COMMAND ----------

write_delta_table(df_facts_beer_unique_sk, 'fact_beer', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('fact_beer', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brewery Fact

# COMMAND ----------

df_facts_brewery = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'fact_brewery')
df_facts_brewery_unique = df_facts_brewery.withColumnRenamed('brewery_bid','brewery_id').distinct()
df_facts_brewery_unique_sk = df_facts_brewery_unique.withColumn('sk_brewery', concat(col('checkin_id'), col('brewery_id')))

# COMMAND ----------

write_delta_table(df_facts_brewery_unique_sk, 'fact_brewery', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('fact_brewery', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Badges Fact

# COMMAND ----------

df_facts_badges = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'fact_badges')
df_facts_badges_unique = df_facts_badges.distinct()
df_facts_badges_unique_sk = df_facts_badges_unique.withColumn('sk_badges', concat(col('checkin_id'), col('badge_id')))

# COMMAND ----------

write_delta_table(df_facts_badges_unique_sk, 'fact_badges', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('fact_badges', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comments Fact

# COMMAND ----------

df_fact_comments = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'fact_comments')
df_fact_comments_unique = df_fact_comments.withColumnRenamed('items.comment_id','comment_id').distinct()
df_fact_comments_unique_sk = df_fact_comments_unique.withColumn('sk_comments', concat(col('checkin_id'), col('comment_id')))

# COMMAND ----------

display(df_fact_comments_unique_sk)

# COMMAND ----------

write_delta_table(df_fact_comments_unique_sk, 'fact_comments', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('fact_comments', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Media Fact

# COMMAND ----------

df_fact_media = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'fact_media')
df_fact_media_unique = df_fact_media.withColumnRenamed('photo_id','media_id').distinct()

# COMMAND ----------

write_delta_table(df_fact_media_unique, 'fact_media', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('fact_media', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Toasts Fact

# COMMAND ----------

df_facts_toasts = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'fact_toasts')
df_facts_toasts_unique = df_facts_toasts.withColumnRenamed('like_id','toast_id').withColumn('sk_toasts', concat(col('checkin_id'), col('toast_id'))).distinct()

# COMMAND ----------

write_delta_table(df_facts_toasts_unique, 'fact_toasts', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('fact_toasts', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Venues Fact

# COMMAND ----------

df_fact_venue = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'fact_venue')
df_fact_venue_unique = df_fact_venue.withColumnRenamed('venue.venue_id','venue_id').distinct()

# COMMAND ----------

write_delta_table(df_fact_venue_unique, 'fact_venue', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('fact_venue', zone = 'sanctioned')

# COMMAND ----------

