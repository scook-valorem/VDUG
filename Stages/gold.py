# Databricks notebook source
md
# TODO 

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Badge Views

# COMMAND ----------

df_badges = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'badges')
df_badges_unique = df_badges.withColumnRenamed('id','badge_id').withColumnRenamed('name', 'badge_name').drop('time').distinct()

# COMMAND ----------

write_delta_table(df_badges_unique, 'badges', zone = 'sanctioned')

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
df_brewery_unique = df_beer.withColumnRenamed('id','brewery_id').distinct()

# COMMAND ----------

write_delta_table(df_brewery_unique, 'brewery', zone = 'sanctioned')

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

write_delta_table(df_media_unique, 'media', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('media', zone = 'sanctioned')

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

write_delta_table(df_brewery_unique, 'source', zone = 'sanctioned')

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
df_facts_unique = df_facts.withColumnRenamed('brewery.brewery_id','brewery_id').distinct()

# COMMAND ----------

write_delta_table(df_facts_unique, 'facts', zone = 'sanctioned')

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
df_facts_beer_unique = df_facts_beer.distinct()

# COMMAND ----------

write_delta_table(df_facts_beer_unique, 'fact_beer', zone = 'sanctioned')

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
df_facts_brewery_unique = df_facts_brewery.distinct()

# COMMAND ----------

write_delta_table(df_facts_brewery_unique, 'fact_brewery', zone = 'sanctioned')

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
df_facts_badges_unique = df_facts_badges.distinct('checkin')

# COMMAND ----------

write_delta_table(df_facts_badges_unique, 'fact_badges', zone = 'sanctioned')

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
df_fact_comments_unique = df_fact_comments.distinct()

# COMMAND ----------

write_delta_table(df_fact_comments_unique, 'fact_comments', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('fact_comments', zone = 'sanctioned')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Media Fact

# COMMAND ----------

df_fact_media = spark.read\
          .format('delta')\
          .option('ignoreChanges', True)\
          .load(untappd_base_query_path+'fact_badges')
df_fact_media_unique = df_fact_media.distinct()

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
df_facts_toasts_unique = df_facts_toasts.distinct()

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
df_fact_venue_unique = df_fact_venue.distinct()

# COMMAND ----------

write_delta_table(df_fact_venue_unique, 'fact_venue', zone = 'sanctioned')

# COMMAND ----------

register_delta_table('fact_venue', zone = 'sanctioned')

# COMMAND ----------

