-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `kestra-project-449609.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://kestra-project-449609-bucket/yellow_tripdata_2024-*.parquet']
);


-- Check yello trip data
SELECT * FROM kestra-project-449609.nytaxi.external_yellow_tripdata limit 10;

-- no 1
SELECT COUNT(*) FROM kestra-project-449609.nytaxi.external_yellow_tripdata;


-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE kestra-project-449609.nytaxi.yellow_tripdata_non_partitioned AS
SELECT * FROM kestra-project-449609.nytaxi.external_yellow_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE kestra-project-449609.nytaxi.yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM kestra-project-449609.nytaxi.external_yellow_tripdata;


-- Impact of partition
-- Scanning ~310.24 MB of data
SELECT DISTINCT(VendorID)
FROM kestra-project-449609.nytaxi.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning ~0 B of DATA
SELECT DISTINCT(VendorID)
FROM kestra-project-449609.nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';


-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE kestra-project-449609.nytaxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM kestra-project-449609.nytaxi.external_yellow_tripdata;

-- Query Elapse time  Execution ~259 ms
SELECT count(*) as trips
FROM kestra-project-449609.nytaxi.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query Elapse time  Execution ~173 ms
SELECT count(*) as trips
FROM kestra-project-449609.nytaxi.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;


-- no 3
-- Scanning 155.12 MB of data
SELECT PULocationID from kestra-project-449609.nytaxi.yellow_tripdata_2024;

-- Scanning 310.24 MB of data
SELECT PULocationID, DOLocationID from kestra-project-449609.nytaxi.yellow_tripdata_2024;

-- no 4
SELECT COUNT(*) FROM kestra-project-449609.nytaxi.yellow_tripdata_2024
WHERE fare_amount = 0;

-- no 5
CREATE OR REPLACE TABLE kestra-project-449609.nytaxi.yellow_tripdata_partitioned_do_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM kestra-project-449609.nytaxi.external_yellow_tripdata;

-- no 6
-- Scanning ~310.24 MB of data
SELECT DISTINCT(VendorID)
FROM kestra-project-449609.nytaxi.yellow_tripdata_2024
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Scanning ~26.84 MB of data
SELECT DISTINCT(VendorID)
FROM kestra-project-449609.nytaxi.yellow_tripdata_partitioned_do_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
