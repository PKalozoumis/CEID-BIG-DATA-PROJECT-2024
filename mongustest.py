from pyspark.sql import DataFrame, SparkSession
import os

#Packages are specified in $SPARK_HOME/conf/spark-defaults.conf

spark = SparkSession.builder.appName("Mongus")\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:10.3.0') \
    .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/newDatabase.collectionName')\
    .config('spark.mongodb.database', 'newDatabase')\
    .config('spark.mongodb.collection', 'collectionName')\
    .getOrCreate()

df = spark.read.format("mongodb") \
    .load()

print("======================================================================================")
print(df)
print("======================================================================================")