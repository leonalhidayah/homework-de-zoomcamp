# week 5 batch

## no 1
``` python
import pyspark

pyspark.__version__
```

3.3.2


## no 2
```python
df = df.repartition(4)

df.write.parquet("yellow_tripdata/2024/10/")
```

the average size of .parquet file is about 24.8 mb,  
the closest answer is  
b. 25 MB


## no 3
``` python
# create temp view
df.createOrReplaceTempView("yellow_2024_10")

# sql query
spark.sql("""
SELECT 
    COUNT(*) AS trip_count
FROM
    yellow_2024_10
WHERE
    DATE(tpep_pickup_datetime) = '2024-10-15'

""").show()
```

c. 125,567

## no 4
``` python
# create new column trip duration
df = df.withColumn(
    "trip_duration_hours",
    (
        F.unix_timestamp(df["tpep_dropoff_datetime"])
        - F.unix_timestamp(df["tpep_pickup_datetime"])
    )
    / 3600,
)

# sql query
spark.sql("""
SELECT
    MAX(trip_duration_hours)
FROM
    yellow_2024_10
""").show()
```

the longest trip is  
c. 162


## no 5
c. 4040


## no 6
``` python
# join df and zones
df_join = df.join(zone_lookup, df["PULocationID"] == zone_lookup["LocationID"], "inner")

# create temp view
df_join.createOrReplaceTempView("yellow_2024_10_with_zones")

# group by using sql query
spark.sql("""
SELECT 
    Zone,
    COUNT(*) AS trip_count
FROM 
    yellow_2024_10_with_zones
GROUP BY Zone
ORDER BY trip_count

""").show()
```

or we can use join in sql like below:

``` sql
SELECT 
    zones.Zone,
    COUNT(*) AS trip_count
FROM 
    zones
INNER JOIN
    yellow_2024_10
    ON 
        yellow_2024_10.PULocationID = zones.LocationID 
GROUP BY Zone
ORDER BY trip_count
```

a. governor's island