# Databricks notebook source
# MAGIC %md ## 106 - Quantile Regression with VowpalWabbit
# MAGIC 
# MAGIC We will demonstrate how to use the VowpalWabbit quantile regressor with
# MAGIC TrainRegressor and ComputeModelStatistics on the Triazines dataset.
# MAGIC 
# MAGIC 
# MAGIC This sample demonstrates how to use the following APIs:
# MAGIC - [`TrainRegressor`
# MAGIC   ](http://mmlspark.azureedge.net/docs/pyspark/TrainRegressor.html)
# MAGIC - [`VowpalWabbitRegressor`
# MAGIC   ](http://mmlspark.azureedge.net/docs/pyspark/VowpalWabbitRegressor.html)
# MAGIC - [`ComputeModelStatistics`
# MAGIC   ](http://mmlspark.azureedge.net/docs/pyspark/ComputeModelStatistics.html)

# COMMAND ----------

triazines = spark.read.format("libsvm")\
    .load("wasbs://publicwasb@mmlspark.blob.core.windows.net/triazines.scale.svmlight")

# COMMAND ----------

# print some basic info
print("records read: " + str(triazines.count()))
print("Schema: ")
triazines.printSchema()
triazines.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md Split the dataset into train and test

# COMMAND ----------

train, test = triazines.randomSplit([0.85, 0.15], seed=1)

# COMMAND ----------

# MAGIC %md Train the quantile regressor on the training data.
# MAGIC 
# MAGIC Note: have a look at stderr for the task to see VW's output
# MAGIC 
# MAGIC Full command line argument docs can be found [here](https://github.com/VowpalWabbit/vowpal_wabbit/wiki/Command-Line-Arguments).
# MAGIC 
# MAGIC Learning rate, numPasses and power_t are exposed to support grid search.

# COMMAND ----------

from mmlspark.vw import VowpalWabbitRegressor
model = (VowpalWabbitRegressor(numPasses=20, args="--holdout_off --loss_function quantile -q :: -l 0.1")
            .fit(train))

# COMMAND ----------

# MAGIC %md Score the regressor on the test data.

# COMMAND ----------

scoredData = model.transform(test)
scoredData.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md Compute metrics using ComputeModelStatistics

# COMMAND ----------

from mmlspark.train import ComputeModelStatistics
metrics = ComputeModelStatistics(evaluationMetric='regression',
                                 labelCol='label',
                                 scoresCol='prediction') \
            .transform(scoredData)
metrics.toPandas()