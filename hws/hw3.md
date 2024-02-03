## LOAD DATA
```python
import io
import os
import requests
import pandas as pd
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
##init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "ny_taxi_222")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        ##file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # download it using requests via a pandas df
        ##request_url = f"{init_url}{service}/{file_name}"
        request_url = f"{init_url}{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        ##df = pd.read_csv(file_name, compression='gzip')
        ##df = pd.read_parquet(file_name)
        ##file_name = file_name.replace('.csv.gz', '.parquet')######
        ##df.to_parquet(file_name, engine='pyarrow')###
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


#web_to_gcs('2019', 'green')
web_to_gcs('2022', 'green')
#web_to_gcs('2019', 'yellow')
#web_to_gcs('2020', 'yellow')

##https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
##https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-02.csv.gz
```

```sql
CREATE OR REPLACE EXTERNAL TABLE `nytaxi.external_green_taxi_2022`
OPTIONS (
  format = PARQUET,
  uris = [gs://ny_taxi_222/green/green_tripdata_2022-*.parquet]
);
```

##1.
```sql
-- hw3 q1
SELECT count(*) cnt FROM nytaxi.external_green_taxi_2022; 
SELECT count(*) cnt FROM nytaxi.green_taxi_222_non_partitoned; 
-- ++ see Details of green_taxi_222_non_partitoned table 840 402
```

##2.
```sql
-- hw3 q2
-- Query scans 0 B
SELECT count(distinct PULocationID) cnt FROM nytaxi.external_green_taxi_2022; 
--Query scans 6.41 MB
SELECT count(distinct PULocationID) cnt FROM nytaxi.green_taxi_222_non_partitoned;
```


##3.
```sql
-- hw3 q3
SELECT count(*) cnt FROM nytaxi.external_green_taxi_2022 where fare_amount = 0;
SELECT count(*) cnt FROM nytaxi.green_taxi_222_non_partitoned where fare_amount = 0;
--1622
```

##4.
```sql
-- hw3 q4
CREATE OR REPLACE TABLE nytaxi.green_taxi_222_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM nytaxi.external_green_taxi_2022;
```

##5.
```sql
-- hw3 q5
--Query scans 12.82 MB
SELECT count(distinct PULocationID) cnt 
FROM nytaxi.green_taxi_222_non_partitoned
WHERE DATE(lpep_pickup_datetime) BETWEEN DATE('2022-06-01') AND DATE('2022-06-30');
--Query scans 1.12 MB
SELECT count(distinct PULocationID) cnt 
FROM nytaxi.nytaxi.green_taxi_222_partitoned_clustered
WHERE DATE(lpep_pickup_datetime) BETWEEN DATE('2022-06-01') AND DATE('2022-06-30');
```

##6.
```sql
uris = [gs://ny_taxi_222/green/green_tripdata_2022-*.parquet]
```

##7.
We never know profit of clustering before execution
There is no profit for tables less then 1 GB

##8.
```sql
--Query scans 0 B
SELECT count(*) cnt FROM nytaxi.green_taxi_222_non_partitoned; 
-- ++ see Details of green_taxi_222_non_partitoned table 840 402
```