# Week 3 - Data Warehouse

## no 1
c. 20,332,093

first way:
- click 3 dot at the right siide of dataset name
- then create table
- create table from Google Cloud Storage
- we can use url pattern to uploud all csv file in our bucket. gs://kestra-project-449609-bucket/yellow_tripdata_2024-*.parquet 
- choose Parquet as a format
- fill the table name with your preference
- after finish click to table name that already created
- then go to details tab
- scroll down to storage info
- we can see Number of Rows there

second way:
- create external table using query that refereing to gcs
- here are the query
``` sql
CREATE OR REPLACE EXTERNAL TABLE `kestra-project-449609.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://kestra-project-449609-bucket/yellow_tripdata_2024-*.parquet']
);
```
- we can't check number of row in external table because of the source is not in big query but the source from bucket
- use this sql query to show the number of rows of the data
``` sql
SELECT COUNT(*) FROM kestra-project-449609.nytaxi.external_yellow_tripdata;
```


## no 2
b. 0 MB for the External Table and 155.12 MB for the Materialized Table

- above, it is not the same size of materialize table. materialize table has a size 2.72GB and external table has size 0 MB. it's 0 because external table refereing into gcs that the actual data store,
- go to the detail information of each table
- we can see in Total logical bytes


## no 3
a. BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


## no 4
d. 8333

query
``` sql
SELECT COUNT(*) FROM kestra-project-449609.nytaxi.yellow_tripdata_2024
WHERE fare_amount = 0;
```

## no 5
a. Partition by tpep_dropoff_datetime and Cluster on VendorID

to reduce the bytes processed and to reduce execution time

query
``` sql
CREATE OR REPLACE TABLE kestra-project-449609.nytaxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM kestra-project-449609.nytaxi.external_yellow_tripdata;
```

## no 6
b. 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

query
``` sql
-- Scanning ~310.24 MB of data
SELECT DISTINCT(VendorID)
FROM kestra-project-449609.nytaxi.yellow_tripdata_2024
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Scanning ~26.84 MB of data
SELECT DISTINCT(VendorID)
FROM kestra-project-449609.nytaxi.yellow_tripdata_partitioned_do_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

## no 7
c. GCP Bucket

## no 8
a. True


