from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, count, avg, to_json, struct, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import window

import sys

#==============================================================================================

def mdb_write(batch, batch_id):
    batch.write\
        .format("mongodb")\
        .mode("append").option("database", "big_data_project_2024")\
        .option("collection", "raw")\
        .save()
    
def mdb_processed_write(batch, batch_id):
    batch.write\
        .format("mongodb")\
        .mode("append").option("database", "big_data_project_2024")\
        .option("collection", "processed")\
        .save()
    
#==============================================================================================

spark = SparkSession.builder.appName("Traffic Data").getOrCreate()

#Read froom kafka stream
#==============================================================================================
df = spark.readStream\
.format("kafka")\
.option("kafka.bootstrap.servers", "localhost:9092")\
.option("subscribe", "vehicle_positions")\
.load()

assert isinstance(df, DataFrame)

#Define schema for the json column
#==============================================================================================
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
#==============================================================================================
df = df.select(from_json(col("value").cast("string"), schema).alias("data"))
df = df.select("data.time", "data.link", "data.name", "data.speed")

#Write raw data to mongo
#==============================================================================================
mdb_raw_query = df.writeStream.outputMode("append").foreachBatch(mdb_write).start()

#Perform all the required calculations
#==============================================================================================
df = df.withWatermark("time", "1 milliseconds").groupby(window("time", "1 seconds"), "link").agg(
    count("name").alias("vcount"),
    avg("speed").alias("vspeed"),
)

df = df.select(col("window.start").alias("time"), col("link"), col("vcount"), col("vspeed"))

#Write processed data to mongodb
#==============================================================================================
mdb_processed_query = df.writeStream.outputMode("append").foreachBatch(mdb_processed_write).start()

mdb_raw_query.awaitTermination()
mdb_processed_query.awaitTermination()
