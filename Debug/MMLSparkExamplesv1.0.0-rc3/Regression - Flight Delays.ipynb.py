# Databricks notebook source
# MAGIC %md ## 102 - Training Regression Algorithms with the L-BFGS Solver
# MAGIC 
# MAGIC In this example, we run a linear regression on the *Flight Delay* dataset to predict the delay times.
# MAGIC 
# MAGIC We demonstrate how to use the `TrainRegressor` and the `ComputePerInstanceStatistics` APIs.
# MAGIC 
# MAGIC First, import the packages.

# COMMAND ----------

import numpy as np
import pandas as pd
import mmlspark

# COMMAND ----------

# MAGIC %md Next, import the CSV dataset.

# COMMAND ----------

flightDelay = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/On_Time_Performance_2012_9.parquet")
# print some basic info
print("records read: " + str(flightDelay.count()))
print("Schema: ")
flightDelay.printSchema()
flightDelay.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md Split the dataset into train and test sets.

# COMMAND ----------

train,test = flightDelay.randomSplit([0.75, 0.25])

# COMMAND ----------

# MAGIC %md Train a regressor on dataset with `l-bfgs`.

# COMMAND ----------

from mmlspark.train import TrainRegressor, TrainedRegressorModel
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import StringIndexer
# Convert columns to categorical
catCols = ["Carrier", "DepTimeBlk", "ArrTimeBlk"]
trainCat = train
testCat = test
for catCol in catCols:
    simodel = StringIndexer(inputCol=catCol, outputCol=catCol + "Tmp").fit(train)
    trainCat = simodel.transform(trainCat).drop(catCol).withColumnRenamed(catCol + "Tmp", catCol)
    testCat = simodel.transform(testCat).drop(catCol).withColumnRenamed(catCol + "Tmp", catCol)
lr = LinearRegression().setRegParam(0.1).setElasticNetParam(0.3)
model = TrainRegressor(model=lr, labelCol="ArrDelay").fit(trainCat)

# COMMAND ----------

# MAGIC %md Save, load, or Score the regressor on the test data.

# COMMAND ----------

import random
model_name = "flightDelayModel_{}.mml".format(random.randint(1, 25))
model.write().overwrite().save(model_name)
flightDelayModel = TrainedRegressorModel.load(model_name)

scoredData = flightDelayModel.transform(testCat)
scoredData.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md Compute model metrics against the entire scored dataset

# COMMAND ----------

from mmlspark.train import ComputeModelStatistics
metrics = ComputeModelStatistics().transform(scoredData)
metrics.toPandas()

# COMMAND ----------

# MAGIC %md Finally, compute and show per-instance statistics, demonstrating the usage
# MAGIC of `ComputePerInstanceStatistics`.

# COMMAND ----------

from mmlspark.train import ComputePerInstanceStatistics
evalPerInstance = ComputePerInstanceStatistics().transform(scoredData)
evalPerInstance.select("ArrDelay", "Scores", "L1_loss", "L2_loss").limit(10).toPandas()