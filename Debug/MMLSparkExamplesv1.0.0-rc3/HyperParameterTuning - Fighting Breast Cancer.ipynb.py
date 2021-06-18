# Databricks notebook source
# MAGIC %md ## 203 - Hyperparameter Tuning with MMLSpark
# MAGIC 
# MAGIC We can do distributed randomized grid search hyperparameter tuning with MMLSpark.
# MAGIC 
# MAGIC First, we import the packages

# COMMAND ----------

import pandas as pd


# COMMAND ----------

# MAGIC %md Now let's read the data and split it to tuning and test sets:

# COMMAND ----------

data = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/BreastCancer.parquet")
tune, test = data.randomSplit([0.80, 0.20])
tune.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md Next, define the models that wil be tuned:

# COMMAND ----------

from mmlspark.automl import TuneHyperparameters
from mmlspark.train import TrainClassifier
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
logReg = LogisticRegression()
randForest = RandomForestClassifier()
gbt = GBTClassifier()
smlmodels = [logReg, randForest, gbt]
mmlmodels = [TrainClassifier(model=model, labelCol="Label") for model in smlmodels]

# COMMAND ----------

# MAGIC %md We can specify the hyperparameters using the HyperparamBuilder.
# MAGIC We can add either DiscreteHyperParam or RangeHyperParam hyperparameters.
# MAGIC TuneHyperparameters will randomly choose values from a uniform distribution.

# COMMAND ----------

from mmlspark.automl import *

paramBuilder = \
  HyperparamBuilder() \
    .addHyperparam(logReg, logReg.regParam, RangeHyperParam(0.1, 0.3)) \
    .addHyperparam(randForest, randForest.numTrees, DiscreteHyperParam([5,10])) \
    .addHyperparam(randForest, randForest.maxDepth, DiscreteHyperParam([3,5])) \
    .addHyperparam(gbt, gbt.maxBins, RangeHyperParam(8,16)) \
    .addHyperparam(gbt, gbt.maxDepth, DiscreteHyperParam([3,5]))
searchSpace = paramBuilder.build()
# The search space is a list of params to tuples of estimator and hyperparam
print(searchSpace)
randomSpace = RandomSpace(searchSpace)

# COMMAND ----------

# MAGIC %md Next, run TuneHyperparameters to get the best model.

# COMMAND ----------

bestModel = TuneHyperparameters(
              evaluationMetric="accuracy", models=mmlmodels, numFolds=2,
              numRuns=len(mmlmodels) * 2, parallelism=1,
              paramSpace=randomSpace.space(), seed=0).fit(tune)

# COMMAND ----------

# MAGIC %md We can view the best model's parameters and retrieve the underlying best model pipeline

# COMMAND ----------

print(bestModel.getBestModelInfo())
print(bestModel.getBestModel())

# COMMAND ----------

# MAGIC %md We can score against the test set and view metrics.

# COMMAND ----------

from mmlspark.train import ComputeModelStatistics
prediction = bestModel.transform(test)
metrics = ComputeModelStatistics().transform(prediction)
metrics.limit(10).toPandas()