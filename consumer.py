from kafka import KafkaConsumer
from pyspark.sql import DataFrame, SparkSession

'''
consumer = KafkaConsumer("vehicle_positions", bootstrap_servers="localhost:9092")

for msg in consumer:
    print(msg.value)

consumer.close()
'''
spark = SparkSession.builder.appName("Read CSV into DataFrame").getOrCreate()

df = spark.read.csv("proj/results/vehicles.csv", header=True, sep="\t")

df.show()

df.printSchema()

df.select("link", "x").show()

df.createOrReplaceTempView("sus")

sqlDf = spark.sql("SELECT * FROM sus WHERE name=0")

sqlDf.show()