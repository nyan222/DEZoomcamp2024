
-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;

/*CREATE OR REPLACE EXTERNAL TABLE  `nytaxi.external_green_taxi_2022` (

                    VendorID INT64,
                    lpep_pickup_datetime TIMESTAMP, 
                    lpep_dropoff_datetime TIMESTAMP,
                    store_and_fwd_flag STRING,
                    RatecodeID INT64,
                    PULocationID INT64,
                    DOLocationID INT64, 
                    passenger_count INT64,
                    trip_distance DOUBLE,
                    fare_amount DOUBLE,
                    extra DOUBLE,
                    mta_tax DOUBLE,
                    tip_amount DOUBLE,
                    tolls_amount DOUBLE,
                    ehail_fee DOUBLE,
                    improvement_surcharge DOUBLE,
                    total_amount DOUBLE,
                    payment_type INT64,
                    trip_type INT64,
                    congestion_surcharge DOUBLE

) OPTIONS (
    format = CSV,
    uris = [gs://ny_taxi_222/green/green_tripdata_2022-*.parquet],
    skip_leading_rows = 1); 
*/


-- Creating external table referring to gcs path
-- CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_yellow_tripdata`
-- OPTIONS (
--   format = CSV,
--   uris = [gs://ny_taxi_222/yellow/yellow_tripdata_2019-*.csv, gs://ny_taxi_222/yellow/yellow_tripdata_2020-*.csv]
-- );

CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_green_taxi_2022`
OPTIONS (
  format = PARQUET,
  uris = [gs://ny_taxi_222/green/green_tripdata_2022-*.parquet]
);

-- Check yello trip data
SELECT * FROM nytaxi.external_yellow_tripdata limit 10;

SELECT count(*) cnt FROM nytaxi.external_green_taxi_2022; 

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE nytaxi.yellow_tripdata_non_partitoned AS
SELECT * FROM nytaxi.external_yellow_tripdata;

CREATE OR REPLACE TABLE nytaxi.green_taxi_222_non_partitoned AS
SELECT * FROM nytaxi.external_green_taxi_2022;

-- hw3 q1
SELECT count(*) cnt FROM nytaxi.external_green_taxi_2022; 
SELECT count(*) cnt FROM nytaxi.green_taxi_222_non_partitoned; 
-- ++ see Details of green_taxi_222_non_partitoned table 

-- hw3 q2
-- Query scans 0 B
SELECT count(distinct PULocationID) cnt FROM nytaxi.external_green_taxi_2022; 
--Query scans 6.41 MB
SELECT count(distinct PULocationID) cnt FROM nytaxi.green_taxi_222_non_partitoned;

-- hw3 q3
SELECT count(*) cnt FROM nytaxi.external_green_taxi_2022 where fare_amount = 0;
SELECT count(*) cnt FROM nytaxi.green_taxi_222_non_partitoned where fare_amount = 0;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE nytaxi.yellow_tripdata_partitoned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM nytaxi.external_yellow_tripdata;

-- Impact of partition
-- Scanning 1.6GB of data
SELECT DISTINCT(VendorID)
FROM nytaxi.yellow_tripdata_non_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN 2019-06-01 AND 2019-06-30;

-- Scanning ~106 MB of DATA
SELECT DISTINCT(VendorID)
FROM nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN 2019-06-01 AND 2019-06-30;

-- Lets look into the partitons
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = yellow_tripdata_partitoned
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE nytaxi.yellow_tripdata_partitoned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM nytaxi.external_yellow_tripdata;

-- hw3 q4
CREATE OR REPLACE TABLE nytaxi.green_taxi_222_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM nytaxi.external_green_taxi_2022;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM nytaxi.yellow_tripdata_partitoned
WHERE DATE(tpep_pickup_datetime) BETWEEN 2019-06-01 AND 2020-12-31
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM nytaxi.yellow_tripdata_partitoned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN 2019-06-01 AND 2020-12-31
  AND VendorID=1;

-- hw3 q5
--Query scans 12.82 MB
SELECT count(distinct PULocationID) cnt 
FROM nytaxi.green_taxi_222_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN DATE('2022-06-01') AND DATE('2022-06-30');
--Query scans 1.12 MB
SELECT count(distinct PULocationID) cnt 
FROM nytaxi.nytaxi.green_taxi_222_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN DATE('2022-06-01') AND DATE('2022-06-30');
