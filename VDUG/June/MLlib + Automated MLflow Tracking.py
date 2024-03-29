# Databricks notebook source
# MAGIC %md
# MAGIC [Databricks blog post](https://docs.microsoft.com/en-us/azure/databricks/applications/machine-learning/automl/mllib-mlflow-integration)

# COMMAND ----------

# MAGIC %md
# MAGIC # MLlib + Automated MLflow Tracking
# MAGIC 
# MAGIC This notebook demonstrates how to use automated MLflow tracking to track MLlib model tuning. 
# MAGIC 
# MAGIC It demonstrates learning a [decision tree](https://en.wikipedia.org/wiki/Decision_tree_learning) using the Apache Spark distributed implementation.  Tracking the learning process in MLflow gives a better understanding of some critical [hyperparameters](https://en.wikipedia.org/wiki/Hyperparameter_optimization) for the tree learning algorithm, using examples to demonstrate how tuning the hyperparameters can improve accuracy.
# MAGIC 
# MAGIC **Data**: The classic MNIST handwritten digit recognition dataset.
# MAGIC 
# MAGIC **Goal**: Learn how to recognize digits (0 - 9) from images of handwriting.
# MAGIC 
# MAGIC **Takeaways**: Decision trees take several hyperparameters that can affect the accuracy of the learned model.  There is no one "best" setting for these for all datasets.  To get the optimal accuracy, you need to tune these hyperparameters based on your data.

# COMMAND ----------

# MAGIC %md ## Load MNIST training and test datasets
# MAGIC 
# MAGIC The datasets are vectors of pixels representing images of handwritten digits.
# MAGIC 
# MAGIC These datasets are stored in the popular LibSVM dataset format.  Load them using MLlib's LibSVM dataset reader utility.

# COMMAND ----------

training = spark.read.format("libsvm").option("numFeatures", "784").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-train.txt")
test = spark.read.format("libsvm").option("numFeatures", "784").load("/databricks-datasets/mnist-digits/data-001/mnist-digits-test.txt")

training.cache()
test.cache()

print("There are {} training images and {} test images.".format(training.count(), test.count()))

# COMMAND ----------

# MAGIC %md Display the data.  Each image has the true label (the `label` column) and a vector of `features` that represent pixel intensities.

# COMMAND ----------

display(training)

# COMMAND ----------

# MAGIC %md ## Define an ML Pipeline with a Decision Tree Estimator
# MAGIC 
# MAGIC Before training, Use the `StringIndexer` class to convert the labels to the categories 0-9, rather than continuous values. Tie this feature preprocessing together with the tree algorithm using a `Pipeline`.  Pipelines are objects Apache Spark provides for piecing together machine learning algorithms into workflows.  To learn more about Pipelines, check out other ML example notebooks in Databricks and the [ML Pipelines user guide](http://spark.apache.org/docs/latest/ml-guide.html).

# COMMAND ----------

# Import the ML classification, indexer, and pipeline classes 
from pyspark.ml.classification import DecisionTreeClassifier, DecisionTreeClassificationModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline

# COMMAND ----------

# StringIndexer: Read input column "label" (digits) and annotate them as categorical values.
indexer = StringIndexer(inputCol="label", outputCol="indexedLabel")
# DecisionTreeClassifier: Learn to predict column "indexedLabel" using the "features" column.
dtc = DecisionTreeClassifier(labelCol="indexedLabel")
# Chain indexer + dtc together into a single ML Pipeline.
pipeline = Pipeline(stages=[indexer, dtc])

# COMMAND ----------

# MAGIC %md ## Automated MLflow Tracking for CrossValidator model tuning
# MAGIC 
# MAGIC This section tunes some of the Pipeline's hyperparameters.  While tuning, MLflow automatically tracks the models produced by `CrossValidator`, along with their evaluation metrics.  This allows you to examine the behavior of the following tuning hyperparameters using MLflow:
# MAGIC 
# MAGIC * `maxDepth`, which determines how deep (and large) the tree can be.  Train trees at varying depths and see how it affects the accuracy on your held-out test set.
# MAGIC * `maxBins`, which controls how to discretize (bin) continuous features.  This case bins pixel values; e.g., choosing `maxBins=2` effectively turns your images into black-and-white images.

# COMMAND ----------

# Define an evaluation metric.  In this case, use "weightedPrecision", which is equivalent to 0-1 accuracy.
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", metricName="weightedPrecision")

# COMMAND ----------

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# COMMAND ----------

grid = ParamGridBuilder() \
  .addGrid(dtc.maxDepth, [2, 3, 4, 5, 6, 7, 8]) \
  .addGrid(dtc.maxBins, [2, 4, 8]) \
  .build()

# COMMAND ----------

cv = CrossValidator(estimator=pipeline, evaluator=evaluator, estimatorParamMaps=grid, numFolds=3)

# COMMAND ----------

# MAGIC %md Run `CrossValidator`.  `CrossValidator` checks to see if an MLflow tracking server is available.  If so, it log runs within MLflow:
# MAGIC 
# MAGIC * Under the current active run, log info for `CrossValidator`.  (Create a new run if none are active.)
# MAGIC * For each submodel (number of folds of cross-validation x number of ParamMaps tested)
# MAGIC   * Log a run for this submodel, along with the evaluation metric on the held-out data.

# COMMAND ----------

# Explicitly create a new run.
# This allows this cell to be run multiple times.
# If you omit mlflow.start_run(), then this cell could run once,
# but a second run would hit conflicts when attempting to overwrite the first run.
import mlflow
with mlflow.start_run():
  cvModel = cv.fit(training)
  test_metric = evaluator.evaluate(cvModel.transform(test))
  mlflow.log_metric('test_' + evaluator.getMetricName(), test_metric) # Logs additional metrics
  run_id = mlflow.active_run().info.run_id

# COMMAND ----------

# MAGIC %md To view the MLflow experiment associated with the notebook, click the **Runs** icon in the notebook context bar on the upper right.  There, you can view all runs. To more easily compare their results, click the button on the upper right that reads "View Experiment UI" when you hover over it.
# MAGIC 
# MAGIC To understand the effect of tuning `maxDepth`:
# MAGIC 
# MAGIC 1. Filter by `params.maxBins = "8"`.
# MAGIC 1. Select the resulting runs and click **Compare**.
# MAGIC 1. In the Scatter Plot, select X-axis **maxDepth** and Y-axis **avg_weightedPrecision**.

# COMMAND ----------

# MAGIC %md # Register the model with the MLflow Model Registry API
# MAGIC 
# MAGIC Now that a forecasting model has been trained and tracked with MLflow, the next step is to register it with the MLflow Model Registry. You can register and manage models using the MLflow UI or the MLflow API .
# MAGIC 
# MAGIC The following cells use the API to register your forecasting model, add rich model descriptions, and perform stage transitions. See the documentation for the UI workflow.

# COMMAND ----------

model_name = "mnist-model" # Replace this with the name of your registered model, if necessary.

# COMMAND ----------

# MAGIC %md ### Create a new registered model using the API
# MAGIC 
# MAGIC The following cells use the `mlflow.register_model()` function to create a new registered model whose name begins with the string `power-forecasting-model`. This also creates a new model version (e.g., `Version 1` of `power-forecasting-model`).

# COMMAND ----------

import mlflow

# The default path where the MLflow autologging function stores the Keras model
artifact_path = "model"
model_uri = "runs:/{run_id}/{artifact_path}".format(run_id=run_id, artifact_path=artifact_path)

model_details = mlflow.register_model(model_uri=model_uri, name=model_name)

# COMMAND ----------

# MAGIC %md After creating a model version, it may take a short period of time to become ready. Certain operations, such as model stage transitions, require the model to be in the `READY` state. Other operations, such as adding a description or fetching model details, can be performed before the model version is ready (e.g., while it is in the `PENDING_REGISTRATION` state).
# MAGIC 
# MAGIC The following cell uses the `MlflowClient.get_model_version()` function to wait until the model is ready.

# COMMAND ----------

import time
from mlflow.tracking.client import MlflowClient
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

def wait_until_ready(model_name, model_version):
  client = MlflowClient()
  for _ in range(10):
    model_version_details = client.get_model_version(
      name=model_name,
      version=model_version,
    )
    status = ModelVersionStatus.from_string(model_version_details.status)
    print("Model status: %s" % ModelVersionStatus.to_string(status))
    if status == ModelVersionStatus.READY:
      break
    time.sleep(1)
  
wait_until_ready(model_details.name, model_details.version)

# COMMAND ----------

# MAGIC %md ### Add model descriptions
# MAGIC 
# MAGIC You can add descriptions to registered models as well as model versions: 
# MAGIC * Model version descriptions are useful for detailing the unique attributes of a particular model version (e.g., the methodology and algorithm used to develop the model). 
# MAGIC * Registered model descriptions are useful for recording information that applies to multiple model versions (e.g., a general overview of the modeling problem and dataset).

# COMMAND ----------

# MAGIC %md Add a high-level description to the registered model, including the machine learning problem and dataset.

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()
client.update_registered_model(
  name=model_details.name,
  description="This model recognizes text."
)

# COMMAND ----------

