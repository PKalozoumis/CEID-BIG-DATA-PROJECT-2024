from kafka import KafkaConsumer
import sys

consumer = KafkaConsumer("vehicle_positions", bootstrap_servers="localhost:9092", enable_auto_commit=False)

try:
    for msg in consumer:
        print(msg.value.decode("utf-8"))
        
except KeyboardInterrupt:
    print(" Exiting...")

finally:
    consumer.close()