# Databricks notebook source
# MAGIC %md
# MAGIC # Perform a manual ingest of the Untappd CSV Data

# COMMAND ----------

# MAGIC %run Utilities/functions

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

untappd_raw_path = base_path+'raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
untappd_raw_delta_path = base_path+'raw/untappd/delta'
untappd_query_path =base_path+'query/untappd'
untappd_raw_schema_path = base_path + 'raw/untappd/schema'

# COMMAND ----------

dbutils.fs.ls('mnt/default/untappd_exports/')

# COMMAND ----------

df = spark.read.option('header', True).csv('dbfs:/mnt/default/untappd_exports/untappd_export_21_01_20')

# COMMAND ----------

display(df)

# COMMAND ----------

print(base_path)

# COMMAND ----------

df.write.format('delta').mode("append").save(base_path+'raw/manual_import')

# COMMAND ----------

df.write.format('delta').option('checkpointLocation', untappd_base_query_path+'{}/checkpoints'.format('manual_import_fact')).option('mergeSchema', True).mode("overwrite").save('{}{}'.format(untappd_base_query_path,'manual_import_fact'))

# COMMAND ----------

spark.sql(
    '''
    CREATE TABLE IF NOT EXISTS {}
    USING DELTA
    LOCATION '{}'
    '''.format('manual_import_fact', '{}{}'.format(untappd_base_query_path,'manual_import_fact'))
    )

# COMMAND ----------

display(spark.table('facts'))

# COMMAND ----------

display(spark.table('manual_import'))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE manual_import

# COMMAND ----------

from pyspark.sql.functions import split, explode
df_flavor = df.select(col('checkin_id'), explode(split(col('flavor_profiles'),',')).alias('flavor'))

# COMMAND ----------

display(df_flavor)

# COMMAND ----------

df_flavor.write.format('delta').option('checkpointLocation', untappd_base_query_path+'{}/checkpoints'.format('manual_import_flavor')).option('mergeSchema', True).mode("overwrite").save('{}{}'.format(untappd_base_query_path,'manual_import_flavor'))

# COMMAND ----------

spark.sql(
    '''
    CREATE TABLE IF NOT EXISTS {}
    USING DELTA
    LOCATION '{}'
    '''.format('manual_import_flavor', '{}{}'.format(untappd_base_query_path,'manual_import_flavor'))
    )

# COMMAND ----------

display(df)

# COMMAND ----------

display(spark.table('facts'))

# COMMAND ----------

from pyspark.sql.functions import lit, concat
df_upsert = df.select(col('comment').alias('checkin_comment'), col('checkin_id'), col('created_at'), col('rating_score'),col('bid').alias('beer_id'), col('brewery_id'), lit('2746346').alias('uid') ,lit('').alias('venue_id')).withColumn('sk_beers', concat(col('checkin_id'), col('beer_id'))).withColumn('sk_brewery', concat(col('checkin_id'), col('brewery_id')))

# COMMAND ----------

df_facts = spark.table('facts')

# COMMAND ----------

display(spark.table('facts').unionAll(df_upsert))

# COMMAND ----------

dbutils.fs.rm(untappd_base_sanctioned_path+'facts', True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE facts

# COMMAND ----------

.join(customers, orders("customers_id") === customers("id"), "rightouter")

# COMMAND ----------

df_facts_joined = df_upsert.join(df_facts, df_upsert.checkin_id == df_facts.checkin_id, 'left_outer').select(df_upsert.checkin_comment, df_upsert.checkin_id, df_upsert.created_at, df_upsert.rating_score,df_upsert.beer_id, df_upsert.brewery_id, df_facts.venue_id, df_upsert.uid)

# COMMAND ----------

display(df_facts_joined)

# COMMAND ----------

display(df)

# COMMAND ----------

df_extras = df.select(col('checkin_id'), col('beer_ibu'), col('venue_name').alias('venue_name_manual'), col('venue_city').alias('venue_city_manual'), col('venue_state').alias('venue_state_manual'), col('venue_country').alias('venue_country_manual'), col('venue_lat').alias('venue_lat_manual'), col('venue_lng').alias('venue_lng_manual'), col('checkin_url'), col('beer_url'), col('brewery_url'), col('purchase_venue'), col('serving_type'), col('global_rating_score'), col('global_weighted_rating_score'), col('tagged_friends'), col('total_toasts'), col('total_comments'))

# COMMAND ----------

df_extras.write.format('delta').option('checkpointLocation', untappd_base_query_path+'{}/checkpoints'.format('manual_import_extras')).option('mergeSchema', True).mode("overwrite").save('{}{}'.format(untappd_base_query_path,'manual_import_extras'))

# COMMAND ----------

spark.sql(
    '''
    CREATE TABLE IF NOT EXISTS {}
    USING DELTA
    LOCATION '{}'
    '''.format('manual_import_extras', '{}{}'.format(untappd_base_query_path,'manual_import_extras'))
    )

# COMMAND ----------

