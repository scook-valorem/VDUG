# Databricks notebook source
# MAGIC %md
# MAGIC ### TODO
# MAGIC invesigate ability to only analyze sentiment for new records. 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### imports

# COMMAND ----------

import requests
import json
import datetime
from pyspark.sql.types import StructType

# COMMAND ----------

# MAGIC %md
# MAGIC ### import configuration

# COMMAND ----------

# MAGIC %run Utilities/parameters

# COMMAND ----------

dbutils.widgets.text("username","", label="username for untappd API")
username = dbutils.widgets.get("username")
untappd_raw_path = base_path+'raw/untappd/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
untappd_raw_delta_path = base_path+'raw/untappd/delta'
untappd_query_path =base_path+'query/untappd'
untappd_raw_schema_path = base_path + 'raw/untappd/schema'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest our schema file from the Data Lake

# COMMAND ----------

head = dbutils.fs.head(untappd_raw_schema_path, 10000)
schema = StructType.fromJson(json.loads(head))

# COMMAND ----------

df = spark.read.schema(schema).json(untappd_raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cognitive Services Ingestion

# COMMAND ----------

from mmlspark.cognitive import *
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, udf
from pyspark.ml.feature import SQLTransformer
import os

#put your service keys here
TEXT_API_KEY          = cognitive_key
# VISION_API_KEY        = os.environ["VISION_API_KEY"]
# BING_IMAGE_SEARCH_KEY = os.environ["BING_IMAGE_SEARCH_KEY"]

# COMMAND ----------

sentimentTransformer = TextSentiment()\
    .setTextCol("checkin_comment")\
    .setUrl("https://{}.api.cognitive.microsoft.com/text/analytics/v3.0/sentiment".format(cognitive_location))\
    .setSubscriptionKey(TEXT_API_KEY)\
    .setOutputCol("sentiment")

#Extract the sentiment score from the API response body
# unneeded when doing raw capture
# getSentiment = SQLTransformer(statement="SELECT *, sentiment[0].sentiment as sentimentLabel FROM __THIS__")

# COMMAND ----------

commentSentimentAnalysis = PipelineModel(stages=[
  sentimentTransformer])
df_checkin_comments = df.select(df.checkin_comment, df.checkin_id)
df_sentiment = commentSentimentAnalysis.transform(df_checkin_comments).drop(col('checkin_comment'))
error_column_name = df_sentiment.columns[1]
df_sentiment_renamed = df_sentiment.withColumnRenamed(error_column_name, 'TextSentiment_error')

# COMMAND ----------

sentiment_raw_path = base_path+'raw/sentiment/{}/{}/{}/untappd.json'.format(date.year,date.month,date.day)
sentiment_raw_delta_path = base_path+'raw/sentiment/delta'
sentiment_query_path =base_path+'query/sentiment'

# COMMAND ----------

df_sentiment_renamed.write.format('delta').mode("append").save(sentiment_raw_delta_path)

# COMMAND ----------

dbutils.notebook.exit("Success")