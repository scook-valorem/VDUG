# Databricks notebook source
# MAGIC %run Utilities/parameters

# COMMAND ----------

# MAGIC %run Utilities/functions

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

# MAGIC %sql
# MAGIC SELECT * from facts

# COMMAND ----------

sentimentTransformer = TextSentiment()\
    .setTextCol("checkin_comment")\
    .setUrl("https://{}.api.cognitive.microsoft.com/text/analytics/v3.0/sentiment".format(cognitive_location))\
    .setSubscriptionKey(TEXT_API_KEY)\
    .setOutputCol("sentiment")

#Extract the sentiment score from the API response body
getSentiment = SQLTransformer(statement="SELECT *, sentiment[0].sentiment as sentimentLabel FROM __THIS__")

# COMMAND ----------

celebrityQuoteAnalysis = PipelineModel(stages=[
  sentimentTransformer,getSentiment])

display(celebrityQuoteAnalysis.transform(spark.table('facts')))

# COMMAND ----------

