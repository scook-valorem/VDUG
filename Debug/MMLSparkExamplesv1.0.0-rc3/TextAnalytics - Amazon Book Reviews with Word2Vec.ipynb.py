# Databricks notebook source
# MAGIC %md ## 202 - Training and Evaluating CNTK Models in Spark ML Pipelines
# MAGIC 
# MAGIC Yet again, now using the `Word2Vec` Estimator from Spark.  We can use the tree-based
# MAGIC learners from spark in this scenario due to the lower dimensionality representation of
# MAGIC features.

# COMMAND ----------

import pandas as pd


# COMMAND ----------

data = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/BookReviewsFromAmazon10K.parquet")
data.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md Modify the label column to predict a rating greater than 3.

# COMMAND ----------

processedData = data.withColumn("label", data["rating"] > 3) \
                    .select(["text", "label"])
processedData.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md Split the dataset into train, test and validation sets.

# COMMAND ----------

train, test, validation = processedData.randomSplit([0.60, 0.20, 0.20])

# COMMAND ----------

# MAGIC %md Use `Tokenizer` and `Word2Vec` to generate the features.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, Word2Vec
tokenizer = Tokenizer(inputCol="text", outputCol="words")
partitions = train.rdd.getNumPartitions()
word2vec = Word2Vec(maxIter=4, seed=42, inputCol="words", outputCol="features",
                    numPartitions=partitions)
textFeaturizer = Pipeline(stages = [tokenizer, word2vec]).fit(train)

# COMMAND ----------

# MAGIC %md Transform each of the train, test and validation datasets.

# COMMAND ----------

ptrain = textFeaturizer.transform(train).select(["label", "features"])
ptest = textFeaturizer.transform(test).select(["label", "features"])
pvalidation = textFeaturizer.transform(validation).select(["label", "features"])
ptrain.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md Generate several models with different parameters from the training data.

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from mmlspark.train import TrainClassifier
import itertools

lrHyperParams       = [0.05, 0.2]
logisticRegressions = [LogisticRegression(regParam = hyperParam)
                       for hyperParam in lrHyperParams]
lrmodels            = [TrainClassifier(model=lrm, labelCol="label").fit(ptrain)
                       for lrm in logisticRegressions]

rfHyperParams       = itertools.product([5, 10], [2, 3])
randomForests       = [RandomForestClassifier(numTrees=hyperParam[0], maxDepth=hyperParam[1])
                       for hyperParam in rfHyperParams]
rfmodels            = [TrainClassifier(model=rfm, labelCol="label").fit(ptrain)
                       for rfm in randomForests]

gbtHyperParams      = itertools.product([8, 16], [2, 3])
gbtclassifiers      = [GBTClassifier(maxBins=hyperParam[0], maxDepth=hyperParam[1])
                       for hyperParam in gbtHyperParams]
gbtmodels           = [TrainClassifier(model=gbt, labelCol="label").fit(ptrain)
                       for gbt in gbtclassifiers]

trainedModels       = lrmodels + rfmodels + gbtmodels

# COMMAND ----------

# MAGIC %md Find the best model for the given test dataset.

# COMMAND ----------

from mmlspark.automl import FindBestModel
bestModel = FindBestModel(evaluationMetric="AUC", models=trainedModels).fit(ptest)
bestModel.getEvaluationResults().show()
bestModel.getBestModelMetrics().show()
bestModel.getAllModelMetrics().show()

# COMMAND ----------

# MAGIC %md Get the accuracy from the validation dataset.

# COMMAND ----------

from mmlspark.train import ComputeModelStatistics
predictions = bestModel.transform(pvalidation)
metrics = ComputeModelStatistics().transform(predictions)
print("Best model's accuracy on validation set = "
      + "{0:.2f}%".format(metrics.first()["accuracy"] * 100))
print("Best model's AUC on validation set = "
      + "{0:.2f}%".format(metrics.first()["AUC"] * 100))