# Databricks notebook source
# MAGIC %md
# MAGIC # TODO
# MAGIC #### clean up flattening logic
# MAGIC #### Remove Venue from fact table

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

df_raw = spark.readStream.format('delta').option('ignoreChanges', True).load(untappd_raw_delta_path)
df_date_fix = df_raw.withColumn('created_at', split(df_raw.created_at,',')[1])
df_date_fix = df_date_fix.withColumn('created_at', split(df_date_fix.created_at,' \\+')[0])
df_date_fix = df_date_fix.withColumn('created_at', to_timestamp(df_date_fix.created_at, 'dd MMM yyyy HH:mm:ss'))
df = df_date_fix.dropDuplicates(['checkin_id','created_at']).withWatermark('created_at', '24 hours')

# COMMAND ----------

from pyspark.sql.functions import col, json_tuple, from_json, schema_of_json
schema = schema_of_json('''{"venue_id":9917985,"venue_name":"Untappd at Home","venue_slug":"untappd-at-home","primary_category_key":"Residence","primary_category":"Residence","parent_category_id":"4e67e38e036454776db1fb3a","categories":{"count":1,"items":[{"category_key":"home_private","category_name":"Home (private)","category_id":"4bf58dd8d48988d103941735","is_primary":true}]},"location":{"venue_address":"","venue_city":"","venue_state":"Everywhere","venue_country":"United States","lat":34.2347,"lng":-77.9482},"contact":{"twitter":"","venue_url":""},"foursquare":{"foursquare_id":"5e7b4d99c91df60008e8b168","foursquare_url":"https://4sq.com/3bDWYuq"},"venue_icon":{"sm":"https://untappd.akamaized.net/venuelogos/venue_9917985_b3a5d245_bg_64.png","md":"https://untappd.akamaized.net/venuelogos/venue_9917985_b3a5d245_bg_88.png","lg":"https://untappd.akamaized.net/venuelogos/venue_9917985_b3a5d245_bg_176.png?v=1"},"is_verified":true}''')
df = df.withColumn("venue", from_json(df.venue, schema))

# COMMAND ----------

write_delta_table(df = df, name = 'untappd')

# COMMAND ----------

register_delta_table(name = 'untappd')