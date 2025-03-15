import json
import time

import pandas as pd
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


server = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=[server], value_serializer=json_serializer)
t0 = time.time()

topic_name = "test-topic"

# transform to dict
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount']
data_path = "B:/Belajar/data engineering/06-streaming/pyflink/green_tripdata_2019-10.csv.gz"

df = pd.read_csv(data_path, compression='gzip', usecols=columns)

df_dict = df.head().to_dict(orient='records')

for i, record in enumerate(df_dict):
    producer.send(topic_name, value=record)
    print(f"Sent {i}: {record}")
    time.sleep(0.05)

producer.flush()

t1 = time.time()
print(f"took {(t1 - t0):.2f} seconds")
