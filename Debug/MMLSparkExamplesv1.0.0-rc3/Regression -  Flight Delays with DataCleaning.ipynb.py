# Databricks notebook source
# MAGIC %md ## 105 - Training Regressions
# MAGIC 
# MAGIC This example notebook is similar to
# MAGIC [Notebook 102](102 - Regression Example with Flight Delay Dataset.ipynb).
# MAGIC In this example, we will demonstrate the use of `DataConversion()` in two
# MAGIC ways.  First, to convert the data type of several columns after the dataset
# MAGIC has been read in to the Spark DataFrame instead of specifying the data types
# MAGIC as the file is read in.  Second, to convert columns to categorical columns
# MAGIC instead of iterating over the columns and applying the `StringIndexer`.
# MAGIC 
# MAGIC This sample demonstrates how to use the following APIs:
# MAGIC - [`TrainRegressor`
# MAGIC   ](http://mmlspark.azureedge.net/docs/pyspark/TrainRegressor.html)
# MAGIC - [`ComputePerInstanceStatistics`
# MAGIC   ](http://mmlspark.azureedge.net/docs/pyspark/ComputePerInstanceStatistics.html)
# MAGIC - [`DataConversion`
# MAGIC   ](http://mmlspark.azureedge.net/docs/pyspark/DataConversion.html)
# MAGIC 
# MAGIC First, import the pandas package

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md Next, import the CSV dataset: retrieve the file if needed, save it locally,
# MAGIC read the data into a pandas dataframe via `read_csv()`, then convert it to
# MAGIC a Spark dataframe.
# MAGIC 
# MAGIC Print the schema of the dataframe, and note the columns that are `long`.

# COMMAND ----------

flightDelay = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/On_Time_Performance_2012_9.parquet")
# print some basic info
print("records read: " + str(flightDelay.count()))
print("Schema: ")
flightDelay.printSchema()
flightDelay.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md Use the `DataConversion` transform API to convert the columns listed to
# MAGIC double.
# MAGIC 
# MAGIC The `DataConversion` API accepts the following types for the `convertTo`
# MAGIC parameter:
# MAGIC * `boolean`
# MAGIC * `byte`
# MAGIC * `short`
# MAGIC * `integer`
# MAGIC * `long`
# MAGIC * `float`
# MAGIC * `double`
# MAGIC * `string`
# MAGIC * `toCategorical`
# MAGIC * `clearCategorical`
# MAGIC * `date` -- converts a string or long to a date of the format
# MAGIC   "yyyy-MM-dd HH:mm:ss" unless another format is specified by
# MAGIC the `dateTimeFormat` parameter.
# MAGIC 
# MAGIC Again, print the schema and note that the columns are now `double`
# MAGIC instead of long.

# COMMAND ----------

from mmlspark.featurize import DataConversion
flightDelay = DataConversion(cols=["Quarter","Month","DayofMonth","DayOfWeek",
                                   "OriginAirportID","DestAirportID",
                                   "CRSDepTime","CRSArrTime"],
                             convertTo="double") \
                  .transform(flightDelay)
flightDelay.printSchema()
flightDelay.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md Split the datasest into train and test sets.

# COMMAND ----------

train, test = flightDelay.randomSplit([0.75, 0.25])

# COMMAND ----------

# MAGIC %md Create a regressor model and train it on the dataset.
# MAGIC 
# MAGIC First, use `DataConversion` to convert the columns `Carrier`, `DepTimeBlk`,
# MAGIC and `ArrTimeBlk` to categorical data.  Recall that in Notebook 102, this
# MAGIC was accomplished by iterating over the columns and converting the strings
# MAGIC to index values using the `StringIndexer` API.  The `DataConversion` API
# MAGIC simplifies the task by allowing you to specify all columns that will have
# MAGIC the same end type in a single command.
# MAGIC 
# MAGIC Create a LinearRegression model using the Limited-memory BFGS solver
# MAGIC (`l-bfgs`), an `ElasticNet` mixing parameter of `0.3`, and a `Regularization`
# MAGIC of `0.1`.
# MAGIC 
# MAGIC Train the model with the `TrainRegressor` API fit on the training dataset.

# COMMAND ----------

from mmlspark.train import TrainRegressor, TrainedRegressorModel
from pyspark.ml.regression import LinearRegression

trainCat = DataConversion(cols=["Carrier","DepTimeBlk","ArrTimeBlk"],
                          convertTo="toCategorical") \
               .transform(train)
testCat  = DataConversion(cols=["Carrier","DepTimeBlk","ArrTimeBlk"],
                          convertTo="toCategorical") \
               .transform(test)
lr = LinearRegression().setSolver("l-bfgs").setRegParam(0.1) \
                       .setElasticNetParam(0.3)
model = TrainRegressor(model=lr, labelCol="ArrDelay").fit(trainCat)

# COMMAND ----------

# MAGIC %md Score the regressor on the test data.

# COMMAND ----------

scoredData = model.transform(testCat)
scoredData.limit(10).toPandas()

# COMMAND ----------

# MAGIC %md Compute model metrics against the entire scored dataset

# COMMAND ----------

from mmlspark.train import ComputeModelStatistics
metrics = ComputeModelStatistics().transform(scoredData)
metrics.toPandas()

# COMMAND ----------

# MAGIC %md Finally, compute and show statistics on individual predictions in the test
# MAGIC dataset, demonstrating the usage of `ComputePerInstanceStatistics`

# COMMAND ----------

from mmlspark.train import ComputePerInstanceStatistics
evalPerInstance = ComputePerInstanceStatistics().transform(scoredData)
evalPerInstance.select("ArrDelay", "Scores", "L1_loss", "L2_loss") \
               .limit(10).toPandas()