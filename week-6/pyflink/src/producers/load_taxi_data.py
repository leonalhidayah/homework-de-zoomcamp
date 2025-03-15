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
