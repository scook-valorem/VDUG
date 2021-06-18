# Databricks notebook source
# MAGIC %md # Exploring Art across Culture and Medium with Fast, Conditional, k-Nearest Neighbors
# MAGIC 
# MAGIC <img src="https://mmlspark.blob.core.windows.net/graphics/art/cross_cultural_matches.jpg"  width="600"/>
# MAGIC 
# MAGIC This notebook serves as a guideline for match-finding via k-nearest-neighbors. In the code below, we will set up code that allows queries involving cultures and mediums of art amassed from the Metropolitan Museum of Art in NYC and the Rijksmuseum in Amsterdam.

# COMMAND ----------

# MAGIC %md ### Overview of the BallTree
# MAGIC The structure functioning behind the kNN model is a BallTree, which is a recursive binary tree where each node (or "ball") contains a partition of the points of data to be queried. Building a BallTree involves assigning data points to the "ball" whose center they are closest to (with respect to a certain specified feature), resulting in a structure that allows binary-tree-like traversal and lends itself to finding k-nearest neighbors at a BallTree leaf.

# COMMAND ----------

# MAGIC %md #### Setup
# MAGIC Import necessary Python libraries and prepare dataset.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.ml.feature import Normalizer
from pyspark.sql.functions import lit, array, array_contains, udf, col, struct
from mmlspark.nn import ConditionalKNN, ConditionalKNNModel
from PIL import Image
from io import BytesIO

import requests
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md Our dataset comes from a table containing artwork information from both the Met and Rijks museums. The schema is as follows:
# MAGIC 
# MAGIC - **id**: A unique identifier for a piece of art
# MAGIC   - Sample Met id: *388395* 
# MAGIC   - Sample Rijks id: *SK-A-2344* 
# MAGIC - **Title**: Art piece title, as written in the museum's database
# MAGIC - **Artist**: Art piece artist, as written in the museum's database
# MAGIC - **Thumbnail_Url**: Location of a JPEG thumbnail of the art piece
# MAGIC - **Image_Url** Location of an image of the art piece hosted on the Met/Rijks website
# MAGIC - **Culture**: Category of culture that the art piece falls under
# MAGIC   - Sample culture categories: *latin american*, *egyptian*, etc
# MAGIC - **Classification**: Category of medium that the art piece falls under
# MAGIC   - Sample medium categories: *woodwork*, *paintings*, etc
# MAGIC - **Museum_Page**: Link to the work of art on the Met/Rijks website
# MAGIC - **Norm_Features**: Embedding of the art piece image
# MAGIC - **Museum**: Specifies which museum the piece originated from

# COMMAND ----------

# loads the dataset and the two trained CKNN models for querying by medium and culture
df = spark.read.parquet("wasbs://publicwasb@mmlspark.blob.core.windows.net/met_and_rijks.parquet")
display(df.drop("Norm_Features"))

# COMMAND ----------

# MAGIC %md #### Define categories to be queried on
# MAGIC We will be using two kNN models: one for culture, and one for medium. The categories for each grouping are defined below.

# COMMAND ----------

from pyspark.sql.types import BooleanType

#mediums = ['prints', 'drawings', 'ceramics', 'textiles', 'paintings', "musical instruments","glass", 'accessories', 'photographs',  "metalwork", 
#           "sculptures", "weapons", "stone", "precious", "paper", "woodwork", "leatherwork", "uncategorized"]

mediums = ['paintings', 'glass', 'ceramics']

#cultures = ['african (general)', 'american', 'ancient american', 'ancient asian', 'ancient european', 'ancient middle-eastern', 'asian (general)', 
#            'austrian', 'belgian', 'british', 'chinese', 'czech', 'dutch', 'egyptian']#, 'european (general)', 'french', 'german', 'greek', 
#            'iranian', 'italian', 'japanese', 'latin american', 'middle eastern', 'roman', 'russian', 'south asian', 'southeast asian', 
#            'spanish', 'swiss', 'various']

cultures = ['japanese', 'american', 'african (general)']

# Uncomment the above for more robust and large scale searches!

classes = cultures + mediums

medium_set = set(mediums)
culture_set = set(cultures)
selected_ids = {"AK-RBK-17525-2", "AK-MAK-1204", "AK-RAK-2015-2-9"}

small_df = df.where(udf(lambda medium, culture, id_val: (medium in medium_set) or (culture in culture_set) or (id_val in selected_ids), BooleanType())("Classification", "Culture", "id"))

small_df.count()

# COMMAND ----------

# MAGIC %md ### Define and fit ConditionalKNN models
# MAGIC Below, we create ConditionalKNN models for both the medium and culture columns; each model takes in an output column, features column (feature vector), values column (cell values under the output column), and label column (the quality that the respective KNN is conditioned on).

# COMMAND ----------

medium_cknn = (ConditionalKNN()
  .setOutputCol("Matches")
  .setFeaturesCol("Norm_Features")
  .setValuesCol("Thumbnail_Url")
  .setLabelCol("Classification")
  .fit(small_df))

# COMMAND ----------

culture_cknn = (ConditionalKNN()
  .setOutputCol("Matches")
  .setFeaturesCol("Norm_Features")
  .setValuesCol("Thumbnail_Url")
  .setLabelCol("Culture")
  .fit(small_df))

# COMMAND ----------

# MAGIC %md #### Define matching and visualizing methods
# MAGIC 
# MAGIC After the intial dataset and category setup, we prepare methods that will query and visualize the conditional kNN's results. 
# MAGIC 
# MAGIC `addMatches()` will create a Dataframe with a handful of matches per category.

# COMMAND ----------

def add_matches(classes, cknn, df):
  results = df
  for label in classes:
    results = (cknn.transform(results.withColumn("conditioner", array(lit(label))))
                 .withColumnRenamed("Matches", "Matches_{}".format(label)))
  return results

# COMMAND ----------

# MAGIC %md `plot_urls()` calls `plot_img` to visualize top matches for each category into a grid.

# COMMAND ----------

def plot_img(axis, url, title):
  response = requests.get(url)
  img = Image.open(BytesIO(response.content)).convert('RGB')
  axis.imshow(img, aspect="equal")
  if title is not None: axis.set_title(title,  fontsize=4)
  axis.axis("off")

def plot_urls(url_arr, titles, filename):
  nx, ny = url_arr.shape
  
  plt.figure(figsize=(nx*5, ny*5), dpi=1600)
  fig, axes = plt.subplots(ny,nx)
  
  # reshape required in the case of 1 image query
  if len(axes.shape) == 1:
    axes = axes.reshape(1, -1)
    
  for i in range(nx):
     for j in range(ny):
          if j == 0:
            plot_img(axes[j, i], url_arr[i,j], titles[i])
          else:
            plot_img(axes[j, i],  url_arr[i,j], None)
            
  plt.savefig(filename, dpi=1600) # saves the results as a PNG

  display(plt.show())

# COMMAND ----------

# MAGIC %md ### Putting it all together
# MAGIC Below, we define `test_all()` to take in the data, CKNN models, the art id values to query on, and the file path to save the output visualization to. The medium and culture models were previously trained and loaded.

# COMMAND ----------

# main method to test a particular dataset with two CKNN models and a set of art IDs, saving the result to filename.png

def test_all(data, cknn_medium, cknn_culture, test_ids, root):
  is_nice_obj = udf(lambda obj: obj in test_ids, BooleanType())
  test_df = data.where(is_nice_obj("id"))
  
  results_df_medium = add_matches(mediums, cknn_medium, test_df)
  results_df_culture = add_matches(cultures, cknn_culture, results_df_medium)
  
  results = results_df_culture.collect()
  
  original_urls = [row["Thumbnail_Url"] for row in results]
  
  culture_urls = [ [row["Matches_{}".format(label)][0]["value"] for row in results] for label in cultures]
  culture_url_arr = np.array([original_urls] + culture_urls)[:, :]
  plot_urls(culture_url_arr, ["Original"] + cultures, root + "matches_by_culture.png")
  
  medium_urls = [ [row["Matches_{}".format(label)][0]["value"] for row in results] for label in mediums]
  medium_url_arr = np.array([original_urls] + medium_urls)[:, :]
  plot_urls(medium_url_arr, ["Original"] + mediums, root + "matches_by_medium.png")
  
  return results_df_culture

# COMMAND ----------

# MAGIC %md ### Demo
# MAGIC The following cell performs batched queries given desired image IDs and a filename to save the visualization.
# MAGIC 
# MAGIC 
# MAGIC <img src="https://mmlspark.blob.core.windows.net/graphics/art/cross_cultural_matches.jpg"  width="600"/>

# COMMAND ----------

# sample query
result_df = test_all(small_df, medium_cknn, culture_cknn, selected_ids, root=".")