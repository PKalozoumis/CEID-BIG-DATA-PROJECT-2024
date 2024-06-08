from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, count, avg, to_json, struct, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import window

import sys


spark = SparkSession.builder.appName("Traffic Data").getOrCreate()

df = spark.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers", "localhost:9092")\
.option("subscribe", "vehicle_positions")\
.load()

assert isinstance(df, DataFrame)

#Define schema for the json column
schema = StructType([
    StructField("name", StringType()),
    StructField("origin", StringType()),
    StructField("destination", StringType()),
    StructField("time", TimestampType()),
    StructField("link", StringType()),
    StructField("position", FloatType()),
    StructField("spacing", FloatType()),
    StructField("speed", FloatType())
])

#df is a kafka message, with a binary "value" field
#We need to cast it into json string
df = df.select(from_json(col("value").cast("string"), schema).alias("data"))
df = df.select("data.time", "data.link", "data.name", "data.speed")

#Perform all the required calculations
df = df.groupby("time", "link").agg(
    count("name").alias("vcount"),
    avg("speed").alias("vspeed"),
)

#Turn back into a json string
json_df = df.select(to_json(struct("*")).alias("value")).selectExpr("CAST(value AS STRING)")

#Sink dataframe into another kafka topic 
query = json_df.writeStream\
.outputMode("update")\
.trigger(processingTime="100 milliseconds")\
.format("kafka")\
.option("kafka.bootstrap.servers", "localhost:9092")\
.option("checkpointLocation", "../../spark_checkpoints")\
.option("topic", "spark_output")\
.start()

'''
query = df.writeStream\
.format("json")\
.option("path", "./spark.json")\
.option("checkpointLocation", "../../kafka_checkpoints")\
.start()

query.awaitTermination()
'''

query.awaitTermination()
