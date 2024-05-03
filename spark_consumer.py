#$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_consumer.py

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, count, avg, to_json, struct, expr
from pyspark.sql.types import StructType, StructField, StringType, FloatType


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
    StructField("time", StringType()),
    StructField("link", StringType()),
    StructField("position", FloatType()),
    StructField("spacing", FloatType()),
    StructField("speed", FloatType())
])

#df is a kafka message, with a binary "value" field
#We need to cast it into json string
df = df.select(from_json(col("value").cast("string"), schema).alias("data"))
df = df.select("data.time", "data.link", "data.name", "data.speed")

#Perform all the requires calculations

df = df.groupby("time", "link").agg(
    count("name").alias("vcount"),
    avg("speed").alias("vspeed"),
)

#Turn back into a json string

#HOW THE FUCK DO I DO THIS????????????????

#Sink dataframe into another kafka topic 
query = df.writeStream\
.outputMode("update")\
.format("kafka")\
.option("kafka.bootstrap.servers", "localhost:9092")\
.option("checkpointLocation", "../../kafka_checkpoints")\
.option("topic", "spark_output")\
.start()

query.awaitTermination()