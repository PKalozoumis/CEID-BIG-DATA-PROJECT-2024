from simulator import *
import pandas as pd
from datetime import datetime, timedelta
import json
from kafka import KafkaProducer

if __name__ == "__main__":

    df = run_simulation()

    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    start = datetime.now()

    for t in range(5, 3605, 5):
        #Exclude waiting_at_origin_node because nothing is happening those time moments
        #Exclude trip_end because we already have link data for the same car and timestamp,
        #we already know that link was its destination
        data = df.loc[(df["t"] == t) & (df["link"] != "waiting_at_origin_node") & (df["link"] != "trip_end")]

        for _, value in data.iterrows():
            json_data = {
                "name": value["name"],
                "origin": value["orig"],
                "destination": value["dest"],
                "time": (start + timedelta(seconds=t)).strftime("%Y-%m-%d %H:%M:%S"),
                "link": value["link"],
                "position": value["x"],
                "spacing": value["s"],
                "speed": value["v"]
            }

            producer.send("vehicle_positions", json.dumps(json_data).encode("utf-8"))

    producer.flush()
    producer.close()