# Week 5 Streaming

## no 1
``` bash
docker exec -it redpanda-1 bash
```
it will open redpanda shell  
to show documentation
``` bash
rpk help
```

from documentation to show redpanda version use version command or --version flags  
``` bash
rpk version
```
the output  
``` bash
rpk version v24.2.18 (rev f9a22d4430)
redpanda@4e9d3f1d5ab8:/$ rpk version
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9
```


## no 2
to make a topic in redpanda step-by-step:  
1. execute the redpanda container with this following command
``` bash
docker exec -it redpanda-1 bash
```
2. execute rpk help to show the documentation
3. to create topic use this following command based on documentation
``` bash
rpk topic create green-trips
```
4. the output is
``` bash
TOPIC        STATUS
green-trips  OK
```


## no 3
the output is True


## no 4
``` python
import csv
import json
import time

from kafka import KafkaProducer


def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    columns_to_keep = {
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
        "tip_amount",
    }

    csv_file = "B:/Belajar/data engineering/06-streaming/pyflink/src/producers/data/green_tripdata_2019-10.csv"  # change to your CSV file path if needed

    with open(csv_file, "r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green-data"
            filtered_row = {key: row[key] for key in columns_to_keep if key in row}

            producer.send("green-trips", value=filtered_row)

    # Make sure any remaining messages are delivered
    producer.flush()
    producer.close()


if __name__ == "__main__":
    t0 = time.time()
    main()
    t1 = time.time()
    print(f"took {(t1 - t0):.2f} seconds")

```

took 70.47 seconds


## no 5




