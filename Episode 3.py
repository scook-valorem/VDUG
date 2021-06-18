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

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC from untappd

# COMMAND ----------

df = spark.table('untappd')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Beer

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_beer_flattened = flatten_df(df.select(df.beer)).withColumnRenamed('beer_beer_abv','abv').withColumnRenamed('beer_beer_active','active').withColumnRenamed('beer_beer_name','name').withColumnRenamed('beer_beer_slug','slug').withColumnRenamed('beer_beer_style','style').withColumnRenamed('beer_beer_bid','id').withColumnRenamed('beer_has_had','has_had').withColumnRenamed('beer_beer_label','label')
df_beer_flattened_cleaned = df_beer_flattened.withColumn('active_bool',when(df_beer_flattened.active > 0 , True).otherwise(False)).drop('active').withColumnRenamed('active_bool','active')

# COMMAND ----------

create_register_delta_table(df = df_beer_flattened_cleaned, name = 'beer', path = untappd_base_query_path+'beer', overwrite = True )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brewery

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_brewery_flattened = flatten_df(df.select(df.brewery))

# COMMAND ----------

names = [(x, x.split('_brewery')[len(x.split('_brewery'))-1]) for x in df_brewery_flattened.columns]
for old, new in names:
  print(old, new)

# COMMAND ----------

for col in df_brewery_flattened.columns:
  splits = col.split('brewery_')
  name = splits[len(splits) - 1]
  df_brewery_flattened = df_brewery_flattened.withColumnRenamed(col,name)
display(df_brewery_flattened)

# COMMAND ----------

create_register_delta_table(df_brewery_flattened, 'brewery', untappd_base_query_path+'brewery', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comments

# COMMAND ----------

# MAGIC %md
# MAGIC count:long
# MAGIC items:array
# MAGIC total_count:long

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_comments = df.select(df.comments.count.alias('comments_count'), explode(df.comments.items), df.comments.total_count.alias('total_count'), df.checkin_id)
print(df_comments.columns)

# COMMAND ----------

for col in df_comments_flattened.columns:
  splits = col.split('col_')
  name = splits[len(splits) - 1]
  df_comments_flattened = df_comments_flattened.withColumnRenamed(col,name)
display(df_comments_flattened)

# COMMAND ----------

create_register_delta_table(df_comments_flattened, 'comments', untappd_base_query_path+'comments', True)

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
# MAGIC ### User (Fixed in [Episode 4](https://adb-2840934607355364.4.azuredatabricks.net/?o=2840934607355364#notebook/350310534036549/command/350310534036556))

# COMMAND ----------

df_brewery = df.select(df.brewery)

# COMMAND ----------

from pyspark.sql.functions import explode, when
df_user = df.select(df.user)


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

