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
#init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
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
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        ##file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        ##request_url = f"{init_url}{file_name}"
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression='gzip')
        if service == "yellow":
            """Fix dtype issues"""
            df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
            df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

        if service == "green":
            """Fix dtype issues"""
            df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
            df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
            df["trip_type"] = df["trip_type"].astype('Int64')

        if service == "yellow" or service == "green":
            df["VendorID"] = df["VendorID"].astype('Int64')
            df["RatecodeID"] = df["RatecodeID"].astype('Int64')
            df["PULocationID"] = df["PULocationID"].astype('Int64')
            df["DOLocationID"] = df["DOLocationID"].astype('Int64')
            df["passenger_count"] = df["passenger_count"].astype('Int64')
            df["payment_type"] = df["payment_type"].astype('Int64')

        if service == "fhv":
            """Rename columns"""
            df.rename({'dropoff_datetime':'dropOff_datetime'}, axis='columns', inplace=True)
            df.rename({'PULocationID':'PUlocationID'}, axis='columns', inplace=True)
            df.rename({'DOLocationID':'DOlocationID'}, axis='columns', inplace=True)

            """Fix dtype issues"""
            df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
            df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

            # See https://pandas.pydata.org/docs/user_guide/integer_na.html
            df["PUlocationID"] = df["PUlocationID"].astype('Int64')
            df["DOlocationID"] = df["DOlocationID"].astype('Int64')

        print(df.head(2))
        print(f"columns: {df.dtypes}")
        print(f"rows: {len(df)}")
        
        ##df = pd.read_parquet(file_name)
        file_name = file_name.replace('.csv.gz', '.parquet')######
        df.to_parquet(file_name, engine='pyarrow')###
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


web_to_gcs('2019', 'green')
web_to_gcs('2020', 'green')
web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')

##https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet
##https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-02.csv.gz