from pyspark.sql import SparkSession
spark = SparkSession\
.builder\
.getOrCreate()

print("Testing simple count")
# The Spark code will execute on the Databricks cluster.
print(spark.range(100).count())