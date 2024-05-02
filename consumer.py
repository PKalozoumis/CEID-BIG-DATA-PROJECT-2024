from kafka import KafkaConsumer

consumer = KafkaConsumer("irememberyoureTOPICS", bootstrap_servers="localhost:9092")

for msg in consumer:
    print(msg)

consumer.close()