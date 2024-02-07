CREATE OR REPLACE EXTERNAL TABLE `nytaxi.ext_yellow_tripdata`
OPTIONS (
  format = PARQUET,
  uris = ['gs://ny_taxi_222/yellow/yellow_tripdata_2019-*.parquet', 'gs://ny_taxi_222/yellow/yellow_tripdata_2020-*.parquet']
);
CREATE OR REPLACE TABLE trips_data_all.yellow_tripdata AS
SELECT * FROM nytaxi.ext_yellow_tripdata;

CREATE OR REPLACE EXTERNAL TABLE `nytaxi.ext_green_tripdata`
OPTIONS (
  format = PARQUET,
  uris = ['gs://ny_taxi_222/green/green_tripdata_2019-*.parquet', 'gs://ny_taxi_222/green/green_tripdata_2020-*.parquet']
);
CREATE OR REPLACE TABLE trips_data_all.green_tripdata AS
SELECT * FROM nytaxi.ext_green_tripdata;

CREATE OR REPLACE EXTERNAL TABLE `nytaxi.ext_fhv_tripdata`
OPTIONS (
  format = PARQUET,
  uris = ['gs://ny_taxi_222/fhv/fhv_tripdata_2019-*.parquet']
);
CREATE OR REPLACE TABLE trips_data_all.fhv_tripdata AS
SELECT * FROM nytaxi.ext_fhv_tripdata;